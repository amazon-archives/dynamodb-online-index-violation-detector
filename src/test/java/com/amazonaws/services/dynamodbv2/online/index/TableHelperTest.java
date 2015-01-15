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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.online.index.TableHelper;

/**
 * 
 * Unit tests for table helper.
 * 
 */
public class TableHelperTest {
    private static String hashKeyName = "hashKey";
    private static String hashKeyType = "S";
    private static String rangeKeyName = "rangeKey";
    private static String rangeKeyType = "N";
    private String gsiName = "gsiName";
    private static long readCapacity = 30L;
    private static long writeCapacity = 100L;

    /** Mock Objects */
    AmazonDynamoDBClient mockDynamoDBClient = Mockito.mock(AmazonDynamoDBClient.class);
    TableDescription mockTableDescription = Mockito.mock(TableDescription.class);

    TableHelper tableHelper = new TableHelper(mockDynamoDBClient, mockTableDescription);

    @Before
    public void setupBeforeTest() {
        /** Setup key schema */
        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        KeySchemaElement hashKey = new KeySchemaElement().withAttributeName(hashKeyName).withKeyType(KeyType.HASH);
        KeySchemaElement rangeKey = new KeySchemaElement().withAttributeName(rangeKeyName).withKeyType(KeyType.RANGE);
        keySchema.add(hashKey);
        keySchema.add(rangeKey);
        Mockito.when(mockTableDescription.getKeySchema()).thenReturn(keySchema);

        /** Setup attribute definition */
        List<AttributeDefinition> attribtueDefinitions = new ArrayList<AttributeDefinition>();
        AttributeDefinition hashKeyDefinition = new AttributeDefinition().withAttributeName(hashKeyName).withAttributeType(hashKeyType);
        AttributeDefinition rangeKeyDefinition = new AttributeDefinition().withAttributeName(rangeKeyName).withAttributeType(rangeKeyType);
        attribtueDefinitions.add(hashKeyDefinition);
        attribtueDefinitions.add(rangeKeyDefinition);
        Mockito.when(mockTableDescription.getAttributeDefinitions()).thenReturn(attribtueDefinitions);

        /** Setup GSI */
        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = new ArrayList<GlobalSecondaryIndexDescription>();
        GlobalSecondaryIndexDescription gsiTest = new GlobalSecondaryIndexDescription().withIndexName(gsiName);
        globalSecondaryIndexes.add(gsiTest);
        Mockito.when(mockTableDescription.getGlobalSecondaryIndexes()).thenReturn(globalSecondaryIndexes);

        /** Setup provisioned throughput */
        ProvisionedThroughputDescription provisionedThroughPut = new ProvisionedThroughputDescription().withReadCapacityUnits(readCapacity)
                .withWriteCapacityUnits(writeCapacity);
        Mockito.when(mockTableDescription.getProvisionedThroughput()).thenReturn(provisionedThroughPut);
    }

    @Test
    public void testGetTableHashKeyName() {
        assertEquals("Should return the hash key name", tableHelper.getTableHashKeyName(), hashKeyName);
    }

    @Test
    public void testGetTableHashKeyType() {
        assertEquals("Should return the hash key type", tableHelper.getTableHashKeyType(), hashKeyType);
    }

    @Test
    public void testGetRangeKeyName() {
        assertEquals("Should return the range key name", tableHelper.getTableRangeKeyName(), rangeKeyName);
    }

    @Test
    public void testGetRangeKeyType() {
        assertEquals("Should return the hash key type", tableHelper.getTableRangeKeyType(), rangeKeyType);
    }

    @Test
    public void testGsiExists() {
        assertTrue(tableHelper.isGsiExists(gsiName));
        assertFalse(tableHelper.isGsiExists("randomName"));
    }

    @Test
    public void testGetReadCapacityUnits() {
        assertEquals("Should return read capacity units", tableHelper.getReadCapacityUnits(), readCapacity);
    }

    @Test
    public void testGetWriteCapacityUnits() {
        assertEquals("Should return write capacity units", tableHelper.getWriteCapacityUnits(),  writeCapacity);
    }
}
