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

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.online.index.Options;
import com.amazonaws.services.dynamodbv2.online.index.TableHelper;
import com.amazonaws.services.dynamodbv2.online.index.TableReader;
import com.amazonaws.services.dynamodbv2.online.index.ViolationChecker;

/**
 * 
 * Unit tests for tableReader.
 * 
 */
public class TableReaderTest {
    /** Mock objects */
    Options mockOptions = Mockito.mock(Options.class);
    AmazonDynamoDBClient mockDynamoDBClient = Mockito.mock(AmazonDynamoDBClient.class);
    TableHelper mockTableHelper = Mockito.mock(TableHelper.class);
    ViolationChecker mockViolationChecker = Mockito.mock(ViolationChecker.class);
    List<String> attributesToGet = new ArrayList<String>();
    double taskRateLimit = 100.0;

    TableReader reader = new TableReader(mockOptions, mockDynamoDBClient, mockTableHelper, mockViolationChecker, 
                                         attributesToGet, taskRateLimit);

    @Test
    public void testScanEntireTable() {

    }

    @Test
    public void testScanGivenNumOfRecords() {

    }

    @Test
    public void testScanGivenNumOfViolations() {

    }
}
