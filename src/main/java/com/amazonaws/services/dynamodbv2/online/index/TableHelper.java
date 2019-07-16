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
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * Get table information.
 *
 */
public class TableHelper {

    private AmazonDynamoDBClient dynamoDBClient;
    private TableDescription tableDescription;

    /**
     * Constructor for unit test.
     */
    protected TableHelper(AmazonDynamoDBClient dynamoDBClient, TableDescription tableDescription) {
        this.dynamoDBClient = dynamoDBClient;
        this.tableDescription = tableDescription;
    }

    public TableHelper(AmazonDynamoDBClient dynamoDBClient, String tableName)
            throws IllegalArgumentException {
        this.dynamoDBClient = dynamoDBClient;
        describeTable(tableName);
    }

    private void describeTable(String tableName) throws IllegalArgumentException {
        try {
            DescribeTableResult describeResult = dynamoDBClient.describeTable(tableName);
            tableDescription = describeResult.getTable();
        } catch (ResourceNotFoundException rnfe) {
            throw new IllegalArgumentException("Error: given table " + tableName + " does not exist in given region.");
        } catch (AmazonServiceException ase) {
            if (ase.getErrorCode().equals("UnrecognizedClientException"))
                throw new IllegalArgumentException("Error: Security token in credential file invalid.");
            else
                throw new IllegalArgumentException(ase.getMessage());
        }
    }

    private KeySchemaElement getTableHashKey() {
        List<KeySchemaElement> keySchemaList = tableDescription.getKeySchema();
        for (KeySchemaElement keyElement : keySchemaList) {
            if (keyElement.getKeyType().equals(KeyType.HASH.toString())) {
                return keyElement;
            }
        }
        return null;
    }

    public String getTableHashKeyName() {
        return getTableHashKey().getAttributeName();
    }

    public String getTableHashKeyType() {
        String hashKeyName = getTableHashKeyName();
        List<AttributeDefinition> definitions = tableDescription.getAttributeDefinitions();
        for (AttributeDefinition attributeDef : definitions) {
            if (attributeDef.getAttributeName().endsWith(hashKeyName)) {
                return attributeDef.getAttributeType();
            }
        }
        return null;
    }

    private KeySchemaElement getTableRangeKey() {
        List<KeySchemaElement> keySchemaList = tableDescription.getKeySchema();
        for (KeySchemaElement keyElement : keySchemaList) {
            if (keyElement.getKeyType().equals(KeyType.RANGE.toString())) {
                return keyElement;
            }
        }
        return null;
    }

    public String getTableRangeKeyName() {
        return getTableRangeKey() == null ? null : getTableRangeKey().getAttributeName();
    }

    public String getTableRangeKeyType() {
        if (null == getTableRangeKey())
            return null;
        String rangeKeyName = getTableRangeKeyName();
        List<AttributeDefinition> definitions = tableDescription.getAttributeDefinitions();
        for (AttributeDefinition attributeDef : definitions) {
            if (attributeDef.getAttributeName().equals(rangeKeyName)) {
                return attributeDef.getAttributeType();
            }
        }
        return null;
    }

    public List<String> getListOfAttributesToFetch(String gsiHashKeyName, String gsiRangeKeyName) {
        List<String> attributesToGet = new ArrayList<String>();
        attributesToGet.add(getTableHashKeyName());
        if (getTableRangeKey() != null) {
            attributesToGet.add(getTableRangeKeyName());
        }
        /** Both GSI hash key and range key are optional */
        if (gsiHashKeyName != null) {
            attributesToGet.add(gsiHashKeyName);
        }
        if (gsiRangeKeyName != null) {
            attributesToGet.add(gsiRangeKeyName);
        }
        return attributesToGet;
    }

    public boolean isGsiExists(String gsiName) {
        List<GlobalSecondaryIndexDescription> descriptionList = null;
        descriptionList = tableDescription.getGlobalSecondaryIndexes();
        if (null == descriptionList) {
            return false;
        }

        for (GlobalSecondaryIndexDescription desc : descriptionList) {
            if (desc.getIndexName().equals(gsiName)) {
                return true;
            }
        }
        return false;
    }

    public long getReadCapacityUnits() {
        Long readCapacityUnits = tableDescription.getProvisionedThroughput().getReadCapacityUnits();
        return readCapacityUnits != 0 ? readCapacityUnits : 100;
    }

    public long getWriteCapacityUnits() {
        Long writeCapacityUnits = tableDescription.getProvisionedThroughput().getWriteCapacityUnits();
        return writeCapacityUnits != 0 ? writeCapacityUnits : 100;
    }
}
