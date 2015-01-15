/**
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.online.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Scan table while checking violations.
 * 
 */
public class TableReader {

    private static Options options;
    private static TableHelper tableHelper;
    private static AmazonDynamoDBClient dynamoDBClient;
    private static List<String> attributesToGet;
    private static long itemsScanLimit;
    private static AtomicLong itemsScanned;
    private static long violationsFindLimit;
    private static AtomicLong violationsFound;
    private static AtomicLong violationsDeleted;
    
    // Used for running tests on DDB Local. (Rate Limiter cannot be used with DDB Local.)
    private static boolean isRunningOnDDBLocal = false;

    /**
     * Constructor for unit test purpose only.
     */
    protected TableReader(Options options, AmazonDynamoDBClient dynamoDBClient, TableHelper tableHelper, ViolationChecker violationChecker,
            List<String> attributesToGet, double taskRateLimit) {
        TableReader.options = options;
        TableReader.dynamoDBClient = dynamoDBClient;
        TableReader.tableHelper = tableHelper;
        TableReader.attributesToGet = attributesToGet;
    }

    public TableReader(Options options, AmazonDynamoDBClient dynamoDBClient, TableHelper tableHelper, boolean isRunningOnDDBLocal)
            throws IOException, IllegalArgumentException {
        TableReader.options = options;
        TableReader.dynamoDBClient = dynamoDBClient;
        TableReader.tableHelper = tableHelper;
        attributesToGet = tableHelper.getListOfAttributesToFetch(options.getGsiHashKeyName(), options.getGsiRangeKeyName());
        itemsScanned = new AtomicLong(0);
        itemsScanLimit = options.getNumOfRecords();
        violationsFound = new AtomicLong(0);
        violationsFindLimit = options.getNumOfViolations();
        violationsDeleted = new AtomicLong(0);
        if (options.recordDetails()) {
            createViolationWriter();
        }
        TableReader.isRunningOnDDBLocal = isRunningOnDDBLocal;
    }

    protected void createViolationWriter() throws IOException {
        String outputFilePath;
        if (options.isDetectionOutputS3Path()) {
            outputFilePath = options.getTmpDetectionOutputPath();
        } else {
            outputFilePath = options.getDetectionOutputPath();
        }
        ViolationWriter.getInstance().createOutputFile(outputFilePath);
    }

    public void scanTable(boolean deleteViolationsAfterFound) throws IOException {
        int numOfSegments = options.getNumOfSegments();
        boolean parallelScan = Integer.parseInt(Options.NUM_OF_SEGMENTS_DEFAULT) != numOfSegments;
        PrintHelper.printScanStartInfo(parallelScan, options.getTableName(), options.getGsiHashKeyName(), options.getGsiRangeKeyName());
        if (deleteViolationsAfterFound) {
            PrintHelper.printDeleteWarning();
        }
        createSegmentScanThreads(numOfSegments, deleteViolationsAfterFound);
        if (options.recordDetails()) {
            ViolationWriter.getInstance().flushAndCloseWriter();
        }
        PrintHelper.printScanSummary(itemsScanned.get(), violationsFound.get(), violationsDeleted.get(), options.getDetectionOutputPath(), options.recordDetails());
        return;
    }

    protected void createSegmentScanThreads(int numOfThreadsToCreate, boolean deleteViolationsAfterFound) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreadsToCreate);
        for (int segment = 0; segment < numOfThreadsToCreate; segment++) {
            ScanSegment scanSegmenTask = new ScanSegment(options, tableHelper, dynamoDBClient, deleteViolationsAfterFound, segment);
            executor.execute(scanSegmenTask);
        }

        /** Wait until all threads end */
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // INGORE InterruptedException
        }
    }
    
    /**
     * For testing
     */
    public long getViolationsFound() {
        return violationsFound.get();
    }
    
    /**
     * For testing
     */
    public long getItemsScanned() {
        return itemsScanned.get();
    }
    
    /**
     * For testing
     */
    public long getViolationsDeleted() {
        return violationsDeleted.get();
    }

    private static class ScanSegment implements Runnable {
        private String tableName;
        private int numOfSegments;
        private int segmentNum;
        private long itemScannedByThread = 0;
        private long violationFoundByThread = 0;
        private long violationDeleteByThread = 0;
        private boolean deleteViolationAfterFound = false;
        private ViolationChecker violationChecker;

        private TableWriter tableWriter;
        private TableRWRateLimiter tableReadRateLimiter;

        public ScanSegment(Options options, TableHelper tableHelper, AmazonDynamoDBClient dynamoDBClient, boolean deleteViolationAfterFound, int segmentNum)
                throws IOException {
            this.tableName = options.getTableName();
            this.numOfSegments = options.getNumOfSegments();
            this.segmentNum = segmentNum;
            this.deleteViolationAfterFound = deleteViolationAfterFound;
            this.violationChecker = new ViolationChecker(options, tableHelper);
            this.tableWriter = new TableWriter(options, tableHelper, dynamoDBClient, numOfSegments, isRunningOnDDBLocal);
            this.tableReadRateLimiter = new TableRWRateLimiter(tableHelper.getReadCapacityUnits(), options.getReadWriteIOPSPercent(),
                    options.getNumOfSegments());
            /**Write header to the output file, this is not a good idea to test the first*/
            if (segmentNum == 0 && options.recordDetails()) {
                ViolationWriter.getInstance().addViolationRecord(violationChecker.getViolationRecordHead());
            }
        }

        @Override
        public void run() {
            Map<String, AttributeValue> exclusiveStartKey = null;
            ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withAttributesToGet(attributesToGet)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).withTotalSegments(numOfSegments).withSegment(segmentNum);
            boolean scanNumLimitReached = false;
            while (!scanNumLimitReached) {
                scanRequest.withExclusiveStartKey(exclusiveStartKey);
                ScanResult scanResult = dynamoDBClient.scan(scanRequest);
                if(!isRunningOnDDBLocal) {
                    // DDB Local does not support rate limiting
                    tableReadRateLimiter.adjustRateWithConsumedCapacity(scanResult.getConsumedCapacity());
                }

                for (Map<String, AttributeValue> item : scanResult.getItems()) {
                    checkItemViolationAndAddDeleteRequest(item);
                    itemsScanned.addAndGet(1);
                    itemScannedByThread += 1;
                    scanNumLimitReached = isScanNumberLimitReached();
                    if(scanNumLimitReached) {
                        break;
                    }
                }

                if (deleteViolationAfterFound) {
                    sendDeleteViolations();
                }
                PrintHelper.printScanProgress(itemsScanned.get(), itemScannedByThread, violationFoundByThread, violationDeleteByThread);

                if (null == (exclusiveStartKey = scanResult.getLastEvaluatedKey())) {
                    break;
                }
            }
            return;
        }

        protected void checkItemViolationAndAddDeleteRequest(Map<String, AttributeValue> item) {
            try {
                ViolationRecord violationRecord = violationChecker.checkItemViolationAndGetRecord(item);
                if (violationRecord != null) {
                    if (options.recordDetails()) {
                        ViolationWriter.getInstance().addViolationRecord(violationRecord);
                    }
                    violationsFound.addAndGet(1);
                    violationFoundByThread += 1;
                    if (deleteViolationAfterFound) {
                        addDeleteViolationRequests(item);
                    }
                }
            } catch (IOException ioe) {
                throw new IllegalArgumentException("Error: Failed to write violation records to file.");
            }
        }

        protected boolean isScanNumberLimitReached() {
            if (itemsScanLimit > 0 && itemsScanLimit <= itemsScanned.get()) {
                PrintHelper.printNumOfItemReachedExitInfo();
                return true;
            }
            if (violationsFindLimit > 0 && violationsFindLimit <= violationsFound.get()) {
                PrintHelper.printNumOfViolationReachedExitInfo();
                return true;
            }
            return false;
        }

        protected void addDeleteViolationRequests(Map<String, AttributeValue> item) {
            int deletedItem = tableWriter.addDeleteRequest(item);
            violationsDeleted.addAndGet(deletedItem);
            violationDeleteByThread += deletedItem;
        }

        protected void sendDeleteViolations() {
            int numOfDeletedItem = tableWriter.sendDeleteRequests();
            violationsDeleted.addAndGet(numOfDeletedItem);
            violationDeleteByThread += numOfDeletedItem;
        }
    }
}
