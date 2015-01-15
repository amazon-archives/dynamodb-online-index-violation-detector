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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Write delete or update to the table.
 * 
 */
public class TableWriter {
    private String tableName;
    private String tableHashKeyName;
    private String tableRangeKeyName;
    private long totalNumOfItemsDeleted;
    private AmazonDynamoDBClient dynamoDBClient;
    private List<WriteRequest> batchDeleteRequests;
    public final static int MAX_BATCH_WRITE_REQUEST_NUM = 25;
    private TableRWRateLimiter tableWriteRateLimiter;
    
    private static final Logger logger = Logger.getLogger(TableWriter.class);
    
    // Used for running tests on DDB Local. Rate Limiter cannot be used with DDB Local.
    private static boolean isRunningOnDDBLocal = false;

    /**
     * Constructor for unit test.
     */
    public TableWriter(String tableName, String tableHashKeyName, String tableRangeKeyName, AmazonDynamoDBClient dynamoDBClient, RateLimiter rateLimiter,
            ArrayList<WriteRequest> batchDeleteRequests) {
        this.tableName = tableName;
        this.tableHashKeyName = tableHashKeyName;
        this.tableRangeKeyName = tableRangeKeyName;
        this.dynamoDBClient = dynamoDBClient;
        this.batchDeleteRequests = batchDeleteRequests;
    }

    public TableWriter(Options options, TableHelper tableHelper, AmazonDynamoDBClient dynamoDBClient, int numOfTasks, boolean isRunningOnDDBLocal) {
        this.tableName = options.getTableName();
        this.tableHashKeyName = tableHelper.getTableHashKeyName();
        this.tableRangeKeyName = tableHelper.getTableRangeKeyName();
        this.dynamoDBClient = dynamoDBClient;
        this.totalNumOfItemsDeleted = 0;
        batchDeleteRequests = new ArrayList<WriteRequest>();
        tableWriteRateLimiter = new TableRWRateLimiter(tableHelper.getWriteCapacityUnits(), options.getReadWriteIOPSPercent(), numOfTasks);
        TableWriter.isRunningOnDDBLocal = isRunningOnDDBLocal;
    }

    /**
     * Add delete request to the bath write requests, since batch write has a
     * limit on number of requests, will send the request automatically if the
     * requests number limit reached.
     */
    public int addDeleteRequest(Map<String, AttributeValue> item) {
        AttributeValue tableHashKey = item.get(tableHashKeyName);
        AttributeValue tableRangeKey = item.get(tableRangeKeyName);
        return addDeleteRequest(tableHashKey, tableRangeKey);
    }

    protected int addDeleteRequest(AttributeValue tableHashKey, AttributeValue tableRangeKey) {
        Map<String, AttributeValue> primaryKey = genTablePrimaryKey(tableHashKey, tableRangeKey);
        batchDeleteRequests.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(primaryKey)));
        int deletedItems = 0;
        if (batchDeleteRequests.size() == MAX_BATCH_WRITE_REQUEST_NUM) {
            deletedItems = sendDeleteRequests();
        }
        return deletedItems;
    }

    public Map<String, AttributeValue> genTablePrimaryKey(AttributeValue tableHashKeyValue, AttributeValue tableRangeKeyValue) {
        Map<String, AttributeValue> primaryKey = new HashMap<String, AttributeValue>();
        primaryKey.put(tableHashKeyName, tableHashKeyValue);
        if (tableRangeKeyValue != null) {
            primaryKey.put(tableRangeKeyName, tableRangeKeyValue);
        }
        return primaryKey;
    }

    public int sendDeleteRequests() throws IllegalArgumentException {
        if (batchDeleteRequests.isEmpty()) {
            return 0;
        }
        BatchWriteItemRequest batchWriteItemRequest = genBatchWriteItemRequest();
        BatchWriteItemResult bathWriteResult = sendBatchWriteRequest(batchWriteItemRequest);
        int undeletedItemNum = countAndPrintUndeletedItems(bathWriteResult);
        if(!isRunningOnDDBLocal) {
            // DDB Local does not support rate limiting
            tableWriteRateLimiter.adjustRateWithConsumedCapacity(bathWriteResult.getConsumedCapacity());
        }
        int deletedRequest = batchDeleteRequests.size() - undeletedItemNum;
        totalNumOfItemsDeleted += deletedRequest;
        PrintHelper.printDeleteProgressInfo(deletedRequest, totalNumOfItemsDeleted);
        batchDeleteRequests = new ArrayList<WriteRequest>();
        return deletedRequest;
    }

    protected BatchWriteItemRequest genBatchWriteItemRequest() {
        Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
        requestItems.put(tableName, batchDeleteRequests);
        return new BatchWriteItemRequest().withRequestItems(requestItems).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
    }

    protected BatchWriteItemResult sendBatchWriteRequest(BatchWriteItemRequest batchWriteItemRequest) {
        try {
            return dynamoDBClient.batchWriteItem(batchWriteItemRequest);
        } catch (AmazonServiceException ase) {
            throw new IllegalArgumentException("Error: Failed to delete " + ase.getMessage());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Error: Invalid argument: " + iae.getMessage());
        }
    }

    protected int countAndPrintUndeletedItems(BatchWriteItemResult batchWriteResult) {
        if (!batchWriteResult.getUnprocessedItems().isEmpty()) {
            logger.warn("WARNING: UNPROCESSED ITEMS:");
            List<WriteRequest> unprocessedRequests = (batchWriteResult.getUnprocessedItems()).get(tableName);
            int count = 0;
            for (WriteRequest w : unprocessedRequests) {
                PrintHelper.printItem(0, count++, w.getDeleteRequest().getKey());
            }
            return unprocessedRequests.size();
        }
        return 0;
    }

    /**
     * Null or empty key value for table key is not allowed.
     */
    public AttributeValue genAttributeValueForTableKey(String attributeType, String keyValue) throws IllegalArgumentException {
        if (keyValue == null || keyValue.isEmpty()) {
            throw new IllegalArgumentException("Error: Key value must not be empty.");
        }
        return genAttributeValueForKey(attributeType, keyValue);
    }

    /**
     * Empty GSI key value for GSI is allowed, since it will be interpreted as
     * delete operation.
     */
    public AttributeValue genAttributeValueForGSIKey(String attributeType, String keyValue) throws IllegalArgumentException {
        if (keyValue == null || keyValue.isEmpty()) {
            return null;
        }
        return genAttributeValueForKey(attributeType, keyValue);
    }

    /**
     * For key, they are stored as blank value, so parse them as blank value
     */
    protected AttributeValue genAttributeValueForKey(String attributeType, String keyValue) throws IllegalArgumentException {
        if (!attributeType.equals("S") && !attributeType.equals("N") && !attributeType.equals("B")) {
            throw new IllegalArgumentException("Error: Key attribute value must be S, B or N");
        }
        return AttributeValueConverter.parseFromBlankString(attributeType, keyValue);
    }

    /**
     * For expected value, they are stored with their attribute type, so parse
     * them with their value.
     */
    protected ExpectedAttributeValue genExpectedAttributeValue(String value) throws IllegalArgumentException {
        AttributeValue attributeValue = AttributeValueConverter.parseFromWithAttributeTypeString(value);
        return new ExpectedAttributeValue().withExists(true).withValue(attributeValue);
    }

    /**
     * Sends an update request to the service and returns true if the request is successful.
     */
    public boolean sendUpdateRequest(Map<String, AttributeValue> primaryKey, Map<String, AttributeValueUpdate> updateItems,
            Map<String, ExpectedAttributeValue> expectedItems) throws Exception {
        if (updateItems.isEmpty()) {
            return false; // No update, return false
        }
        UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(primaryKey).withReturnValues(ReturnValue.UPDATED_NEW)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).withAttributeUpdates(updateItems);
        if (expectedItems != null) {
            updateItemRequest.withExpected(expectedItems);
        }

        UpdateItemResult result = dynamoDBClient.updateItem(updateItemRequest);
        if(!isRunningOnDDBLocal) {
            // DDB Local does not support rate limiting
            tableWriteRateLimiter.adjustRateWithConsumedCapacity(result.getConsumedCapacity());
        }
        return true;
    }
}
