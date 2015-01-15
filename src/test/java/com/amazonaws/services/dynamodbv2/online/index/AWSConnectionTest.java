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

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.online.index.AWSConnection;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Tests for AWSConnection.
 * 
 */
public class AWSConnectionTest {
    /** Mock objects */
    private AWSCredentials mockAWSCredentials = Mockito.mock(AWSCredentials.class);

    private AWSConnection awsConnection;

    @Before
    public void setupBeforeTest() {
        awsConnection = new AWSConnection(mockAWSCredentials);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructWithInvalidCredentialFilePath() throws FileNotFoundException, IOException {
        new AWSConnection("invalid file path");
    }

    @Test
    public void testGetDynamoDBClient() {
        assertTrue(awsConnection.getDynamoDBClient(Region.getRegion(Regions.EU_WEST_1), false /*runOnDDBLocal*/) instanceof AmazonDynamoDBClient);
    }

    @Test
    public void testGetS3Client() {
        assertTrue(awsConnection.getS3Client() instanceof AmazonS3Client);
    }
}
