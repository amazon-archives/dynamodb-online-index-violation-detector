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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Print progress information.
 * 
 */
public class PrintHelper {
    
    private static final Logger logger = Logger.getLogger(PrintHelper.class);

    public static void printItem(int segment, long itemCount, Map<String, AttributeValue> attributeList) {
        logger.info("Segment " + segment + ", itemCount " + itemCount + ", ");
        for (Entry<String, AttributeValue> item : attributeList.entrySet()) {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            logger.info(attributeName + " " + (value.getS() == null ? "" : "S = [" + value.getS() + "]")
                    + (value.getN() == null ? "" : "N = [" + value.getN() + "]")
                    + (value.getB() == null ? "" : "B = [" + new String(value.getB().array()) + "]"));

            /** String Set */
            if (value.getSS() != null) {
                logger.info("SS = [");
                Iterator<String> iterator = value.getSS().iterator();
                while (iterator.hasNext()) {
                    logger.info(iterator.next() + (iterator.hasNext() ? ", " : ""));
                }
                logger.info("]");
            }

            /** Number Set */
            if (value.getNS() != null) {
                logger.info("NS = [");
                Iterator<String> iterator = value.getNS().iterator();
                while (iterator.hasNext()) {
                    logger.info(iterator.next() + (iterator.hasNext() ? ", " : ""));
                }
                logger.info("]");
            }

            /** Binary Set */
            if (value.getBS() != null) {
                logger.info("SS = [");
                Iterator<ByteBuffer> iterator = value.getBS().iterator();
                while (iterator.hasNext()) {
                    logger.info(new String(iterator.next().array()) + (iterator.hasNext() ? ", " : ""));
                }
                logger.info("]");
            }
            logger.info(", ");

        }
    }

    public static void printScanStartInfo(boolean parallelScan, String tableName, String gsiHashKeyName, String gsiRangeKeyName) {
        String message = "Violation detection started: " + (parallelScan == true ? "paralallel scan" : "sequential scan") + ", Table name: " + tableName
                + (gsiHashKeyName != null ? ", GSI hash Key: " + gsiHashKeyName : "")
                + (gsiRangeKeyName != null ? ", GSI range key: " + gsiRangeKeyName : "");
        logger.info(message);
    }

    public static void printDeleteWarning() {
        String message = "WARNING: delete has been chosen, violation will be deleted from table!!";
        logger.info(message);
    }

    public static void printScanProgress(long recordScanned, long recordScannedbyThread, long violationFound, long violationDelete) {
        String message = "Progress: " + "Items scanned in total: " + recordScanned + "," + "\tItems scanned by this thread: " + recordScannedbyThread
                + "," + "\tViolations found by this thread: " + violationFound + "," + "\tViolations deleted by this thread: " + violationDelete + "\t...";
        logger.info(message);
    }

    public static void printNumOfViolationReachedExitInfo() {
        String message = "Given number of violations has been found, will stop scanning now.";
        logger.info(message);
    }

    public static void printNumOfItemReachedExitInfo() {
        String message = "Given number of items has been scanned, will stop scanning now";
        logger.info(message);
    }

    public static void printScanSummary(long recordsScanned, long violationsFound, long violationDelete, String outputPath, boolean recordViolations) {
        String message = "Violation detection finished: " + "Records scanned: " + recordsScanned + ", Violations found: " + violationsFound
                + ", Violations deleted: " + violationDelete;
        if(recordViolations) {
            message += ", see results at: " + outputPath;
        }
        logger.info(message);
    }

    public static void printDeleteStartInfo(String inputFilePath) {
        String message = "Violation correction from file started: " + "Reading records from file: " + inputFilePath + ", will delete these records from table.";
        logger.info(message);
    }

    public static void printDeleteProgressInfo(int deleteItems, long totalNumOfItemsDeleted) {
        String message = "Violation delete progress: " + deleteItems + " items deleted, \t" + totalNumOfItemsDeleted + " total number of items delted...";
        logger.info(message);
    }

    public static void printUpdateStartInfo(String inputFilePath) {
        String message = "Violation correction from file started: " + "Reading records from file: " + inputFilePath + ", will update these records from table.";
        logger.info(message);
    }

    public static void printCorrectionSummary(long violationUpdateRequests, long success, 
            long failedConditionalUpdates, long unexpectedErrors, String outputFile) {
        String message = "Violation correction from file finished: " + "Total Requests With Updates: " + violationUpdateRequests 
                + ", Successful Requests: " + success + ", Conditional Update Failures: " 
                + failedConditionalUpdates + ", Errors: " + unexpectedErrors;
        
        if(failedConditionalUpdates > 0 || unexpectedErrors > 0) {
            message += "\nSee violation update error details at: " + outputFile;
        }
        logger.info(message);
    }
    
    public static void printCorrectionSummary(long violationUpdateRequests, long success, 
            long unexpectedErrors, String outputFile) {
        String message = "Violation correction from file finished: " + "Total Requests With Updates: " + violationUpdateRequests 
                + ", Successful Requests: " + success + ", Errors: " + unexpectedErrors;
        
        if(unexpectedErrors > 0) {
            message += "\nSee violation update error details at: " + outputFile;
        }
        logger.info(message);
    }
    
    public static void printCorrectionDeleteSummary(long violationDelete) {
        String message = "Violation correction from file finished: " + "Total Violations deleted: " + violationDelete;
        logger.info(message);
    }

}
