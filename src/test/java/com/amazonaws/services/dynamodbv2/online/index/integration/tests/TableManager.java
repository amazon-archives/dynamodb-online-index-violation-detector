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
package com.amazonaws.services.dynamodbv2.online.index.integration.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.online.index.PrintHelper;
import com.amazonaws.services.dynamodbv2.online.index.RandomDataGenerator;

/**
 * 
 * Manage DynamoDB tables for functional tests.
 * 
 */
public class TableManager {
    private AmazonDynamoDBClient client;
    RandomDataGenerator randDataGenerator;
    PrintHelper printHelper = new PrintHelper();

    public TableManager(AmazonDynamoDBClient client) {
        this.client = client;
        this.randDataGenerator = new RandomDataGenerator();
    }

    /**
     * Check if table has already existed Using describe table API. If table has
     * not been created, exception will be thrown.
     */
    public String isTableExist(String tableName) {
        if (tableName == null)
            throw new IllegalArgumentException("tableName should not be null.");
        DescribeTableResult table = new DescribeTableResult();

        try {
            table = client.describeTable(tableName);
            return table.getTable().getTableName();
        } catch (ResourceNotFoundException rnfe) {
            return null;
        }
    }

    /**
     * 
     * Create table with given name and schema
     * 
     */
    public boolean createNewTable(String tableName, String hashKeyName, String hashKeyType, String rangeKeyName, String rangeKeyType, long readCapacity,
            long writeCapacity) {
        /** Attribute definition */
        ArrayList<AttributeDefinition> attributeDefinition = new ArrayList<AttributeDefinition>();
        attributeDefinition.add(new AttributeDefinition().withAttributeName(hashKeyName).withAttributeType(hashKeyType));
        if (rangeKeyName != null)
            attributeDefinition.add(new AttributeDefinition().withAttributeName(rangeKeyName).withAttributeType(rangeKeyType));

        /** KeySchema */
        ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName(hashKeyName).withKeyType(KeyType.HASH));
        if (rangeKeyName != null)
            keySchema.add(new KeySchemaElement().withAttributeName(rangeKeyName).withKeyType(KeyType.RANGE));

        /** Provisioned throughput */
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity);

        /** Create table request */
        CreateTableRequest request = new CreateTableRequest().withTableName(tableName).withAttributeDefinitions(attributeDefinition).withKeySchema(keySchema)
                .withProvisionedThroughput(provisionedThroughput);

        /** Try to create table */
        try {
            client.createTable(request);
            waitForTableToBecomeAvailable(tableName);
            System.out.println("Table " + tableName + " created.");
        } catch (ResourceInUseException riue) {
            System.out.println("Table " + tableName + " already existed.");
            return false;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }
    
    private void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            DescribeTableRequest request = new DescribeTableRequest()
                    .withTableName(tableName);
            TableDescription tableDescription = client.describeTable(
                    request).getTable();
            String tableStatus = tableDescription.getTableStatus();
            System.out.println("  - current state: " + tableStatus);
            if (tableStatus.equals(TableStatus.ACTIVE.toString()))
                return;
            try { Thread.sleep(1000 * 20); } catch (Exception e) { }
        }
        throw new RuntimeException("Table " + tableName + " never went active");
    }

    /**
     * 
     * Load the specific kind of data into table.
     * 
     */
    public List<Map<String, AttributeValue>> loadRandomData(String tableName, String hashKeyName, String hashKeyType, String rangeKeyName, String rangeKeyType, 
        Map<String, String> attributePairs, int randomStringLength, int loadItemCount) {
        List<Map<String, AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();
        for (int i = 0; i < loadItemCount; i++) {
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            // hash-key
            item.put(hashKeyName, generateItem(hashKeyName, hashKeyType, 5));
            // range-key
            item.put(rangeKeyName, generateItem(rangeKeyName, rangeKeyType, 5));
            /** Adding random values for specific attribute type */
            for (Entry<String, String> attribute : attributePairs.entrySet()) {
                item.put(attribute.getKey(), generateItem(attribute.getKey(), attribute.getValue(), randomStringLength));
            }
            /** Put data into table */
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
//          printHelper.printItem(1, i + 1, item);
            client.putItem(putItemRequest);
            list.add(item);
        }
        return list;
    }

    protected AttributeValue generateItem(String attributeName, String attributeType, int randomStringLength) {
        /** String */
        if ("S".equals(attributeType)) {
            return new AttributeValue().withS(randDataGenerator.nextRadomString(randomStringLength));
        }
        /** Number */
        if ("N".equals(attributeType)) {
            return new AttributeValue().withN(Integer.toString(randDataGenerator.nextRandomInt()));
        }
        /** Binary */
        if ("B".equals(attributeType)) {
            return new AttributeValue().withB(randDataGenerator.nextRandomBinary(randomStringLength));
        }
        /** String Set */
        if ("SS".equals(attributeType)) {
            return new AttributeValue().withSS(randDataGenerator.nextRandomStringArray(randomStringLength));
        }
        /** Integer set */
        if ("NS".equals(attributeType)) {
            return new AttributeValue().withNS(randDataGenerator.nextRandomIntArray());
        }
        /** Binary Set */
        if ("BS".equals(attributeType)) {
            return new AttributeValue().withBS(randDataGenerator.nextRandomBinaryArray(randomStringLength));
        }
        return null;
    }

	/**
     * 
     * Get the item from table by their hash key and range key
     * 
     */
    public Map<String, AttributeValue> getItem(String tableName, String hashKeyName, AttributeValue hashkeyValue, String rangeKeyName,
            AttributeValue rangeKeyValue) {
        HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put(hashKeyName, hashkeyValue);
        if (rangeKeyValue != null)
            key.put(rangeKeyName, rangeKeyValue);

        GetItemRequest getItemRequest = new GetItemRequest().withTableName(tableName).withKey(key);

        GetItemResult result = client.getItem(getItemRequest);
        return result.getItem();
    }
    
    /**
     * 
     * Put the item in table
     * 
     */
    public void putItem(String tableName, Map<String, AttributeValue> item) {
    	
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
        client.putItem(putItemRequest);
    }

    public void deleteTable(String tableName) {
        client.deleteTable(tableName);
    }

    public List<Map<String, AttributeValue>> getItems(String tableName) {
        List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
        ScanResult scanResult = client.scan(scanRequest);
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            scanRequest = new ScanRequest().withTableName(tableName).withExclusiveStartKey(lastEvaluatedKey);
            scanResult = client.scan(scanRequest);
            items.addAll(scanResult.getItems());
            lastEvaluatedKey = scanResult.getLastEvaluatedKey();
        } while(lastEvaluatedKey != null);
        return items;
    }
}
