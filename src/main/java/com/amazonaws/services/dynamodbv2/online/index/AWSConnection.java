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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Providing AWS Clients, including DynamoDB and S3.
 * 
 */
public class AWSConnection {

    private AWSCredentials awsCredentials;
    public static final String DDB_LOCAL_ENDPOINT = "http://localhost:8000";

    /**
     * Constructor for unit tests
     */
    protected AWSConnection(AWSCredentials awsCredentials) {
        this.awsCredentials = awsCredentials;
    }

    public AWSConnection(String credentialFilePath)
            throws FileNotFoundException, IOException {
        this.awsCredentials = loadCredentialFile(credentialFilePath);
    }

    private AWSCredentials loadCredentialFile(String credentialsFilePath) throws IllegalArgumentException {
        try {
            return new PropertiesCredentials(getInputStream(credentialsFilePath));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Error: Failed to read credential file " + credentialsFilePath);
        }
    }

    private FileInputStream getInputStream(String credentialsFilePath) throws FileNotFoundException {
        try {
            return new FileInputStream(credentialsFilePath);
        } catch (FileNotFoundException ffe) {
            throw new IllegalArgumentException("Error: Credential file " + credentialsFilePath + " not Found.");
        }
    }

    public AmazonDynamoDBClient getDynamoDBClient(Region dynamoDBTableRegion, boolean runOnDDBLocal) {
        AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(awsCredentials);
        dynamoDBClient.setRegion(dynamoDBTableRegion);
        if(runOnDDBLocal) {
            dynamoDBClient.setEndpoint(DDB_LOCAL_ENDPOINT);
        }
        return dynamoDBClient;
    }

    public AmazonS3Client getS3Client() {
        return new AmazonS3Client(awsCredentials);
    }
}
