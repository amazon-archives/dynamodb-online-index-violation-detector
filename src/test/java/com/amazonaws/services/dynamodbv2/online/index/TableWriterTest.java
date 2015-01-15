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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.online.index.TableWriter;
import com.google.common.util.concurrent.RateLimiter;

/**
 * 
 * Unit test TableWriter.
 * 
 */
@SuppressWarnings("unchecked")
public class TableWriterTest {
    /** Mock objects */
    private AmazonDynamoDBClient mockDynamoDBClient = Mockito.mock(AmazonDynamoDBClient.class);
    private RateLimiter mockRateLimiter = Mockito.mock(RateLimiter.class);
    private ArrayList<WriteRequest> mockBatchDeleteRequests = Mockito.mock(ArrayList.class);

    private final String tableName = "TestTableForDB";
    private final String hashKeyName = "hashKey";
    private final String rangeKeyName = "rangeKey";

    private RandomDataGenerator randDataGenerator = new RandomDataGenerator();
    private TableWriter tableWriter;

    @Before
    public void setup() {
        tableWriter = new TableWriter(tableName, hashKeyName, rangeKeyName, mockDynamoDBClient, mockRateLimiter, mockBatchDeleteRequests);
    }

    @Test
    public void testGenTablePrimaryKeyWithBothHashKeyAndRangeKey() {
        String hashKeyValue = randDataGenerator.nextRadomString(10);
        String rangekeyValue = String.valueOf(randDataGenerator.nextRandomInt());
        AttributeValue tableHashkey = new AttributeValue().withS(hashKeyValue);
        AttributeValue tableRangeKey = new AttributeValue().withN(rangekeyValue);
        Map<String, AttributeValue> primaryKey = tableWriter.genTablePrimaryKey(tableHashkey, tableRangeKey);
        assertEquals("Should return the given table hash key", tableHashkey, primaryKey.get(hashKeyName));
        assertEquals("Should return the given table range key", tableRangeKey, primaryKey.get(rangeKeyName));
    }

    @Test
    public void testGenTablePrimaryKeyWithOnlyHashKey() {
        String hashKeyValue = randDataGenerator.nextRadomString(10);
        AttributeValue tableHashkey = new AttributeValue().withS(hashKeyValue);
        Map<String, AttributeValue> primaryKey = tableWriter.genTablePrimaryKey(tableHashkey, null);
        assertEquals("Should return the given table hash key", tableHashkey, primaryKey.get(hashKeyName));
        assertNull(primaryKey.get(rangeKeyName));
    }

    @Test
    public void testAddDeleteRequest() {
        String hashKeyValue = randDataGenerator.nextRadomString(10);
        String rangekeyValue = String.valueOf(randDataGenerator.nextRandomInt());
        AttributeValue tableHashkey = new AttributeValue().withS(hashKeyValue);
        AttributeValue tableRangeKey = new AttributeValue().withN(rangekeyValue);
        Mockito.when(mockBatchDeleteRequests.size()).thenReturn(0);
        assertEquals("Should be no delete now ", 0, tableWriter.addDeleteRequest(tableHashkey, tableRangeKey));
    }

    @Test
    public void testAddDeleteRequestWithItem() {
        String hashKeyValue = randDataGenerator.nextRadomString(10);
        String rangekeyValue = String.valueOf(randDataGenerator.nextRandomInt());
        AttributeValue tableHashkey = new AttributeValue().withS(hashKeyValue);
        AttributeValue tableRangeKey = new AttributeValue().withN(rangekeyValue);
        Map<String, AttributeValue> mockItem = Mockito.mock(HashMap.class);
        Mockito.when(mockItem.get(hashKeyName)).thenReturn(tableHashkey);
        Mockito.when(mockItem.get(rangeKeyName)).thenReturn(tableRangeKey);
        Mockito.when(mockBatchDeleteRequests.size()).thenReturn(0);
        assertEquals("Should be no delete now", 0, tableWriter.addDeleteRequest(mockItem));
    }

    @Test
    public void testSendDeleteRequestsWithListEmpty() {
        Mockito.when(mockBatchDeleteRequests.isEmpty()).thenReturn(true);
        assertEquals("Should not be any delete if empty", 0, tableWriter.sendDeleteRequests());
    }

    @Test
    public void testCountAndPrintUndeletedItems() {
        BatchWriteItemResult mockBatchWriteResult = Mockito.mock(BatchWriteItemResult.class);
        HashMap<String, List<WriteRequest>> mockUnProcessedItems = Mockito.mock(HashMap.class);
        Mockito.when(mockBatchWriteResult.getUnprocessedItems()).thenReturn(mockUnProcessedItems);
        Mockito.when(mockUnProcessedItems.isEmpty()).thenReturn(true);
        tableWriter.countAndPrintUndeletedItems(mockBatchWriteResult);
        Mockito.verify(mockUnProcessedItems, Mockito.times(1)).isEmpty();
        Mockito.verify(mockUnProcessedItems, Mockito.times(0)).get(Mockito.anyString());
    }

    @Test
    public void testGenAttributeValueForKeyWithS() {
        String value = randDataGenerator.nextRadomString(10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        assertEquals("Should return the right attribute value", keyValue, tableWriter.genAttributeValueForKey("S", value));
    }

    @Test
    public void testGenAttributeValueForKeyWithN() {
        String value = String.valueOf(randDataGenerator.nextRandomInt());
        AttributeValue keyValue = new AttributeValue().withN(value);
        assertEquals("Should return the right attribute value", keyValue, tableWriter.genAttributeValueForKey("N", value));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenAttribtueValueForKeyWithSS() {
        String value = String.valueOf(randDataGenerator.nextRandomInt());
        tableWriter.genAttributeValueForKey("SS", value);
    }
    
}
