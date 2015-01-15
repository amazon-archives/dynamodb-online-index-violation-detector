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

import com.amazonaws.regions.Region;

/**
 * A Singleton class for options loading.
 * 
 */
public class Options {
    /** Instance */
    private static Options instance = new Options();

    /** Option names on property file */
    public static final String AWS_CREDENTIAL_FILE = "awsCredentialsFile";
    public static final String DYNAMODB_REGION = "dynamoDBRegion";
    public static final String TABLE_NAME = "tableName";
    public static final String GSI_HASH_KEY_NAME = "gsiHashKeyName";
    public static final String GSI_HASH_KEY_TYPE = "gsiHashKeyType";
    public static final String GSI_RANGE_KEY_NAME = "gsiRangeKeyName";
    public static final String GSI_RANGE_KEY_TYPE = "gsiRangeKeyType";
    public static final String READ_WRITE_IOPS_PERCENT = "readWriteIOPSPercent";
    public static final String RECORD_DETAILS = "recordDetails";
    public static final String RECORD_GSI_VALUE_IN_VIOLATION_RECORD = "recordGsiValueInViolationRecord";
    public static final String EXISTING_GSI = "existingGSI";
    public static final String DETECTION_OUTPUT_PATH = "detectionOutputPath";
    public static final String NUM_OF_SEGMENTS = "numOfSegments";
    public static final String NUM_OF_VIOLATIONS = "numOfViolations";
    public static final String NUM_OF_RECORDS = "numOfRecords";
    public static final String CORRECTION_INPUT_PATH = "correctionInputPath";
    public static final String CORRECTION_OUTPUT_PATH = "correctionOutputPath";

    /** Default value and limits */
    public static final String READ_WRITE_IOPS_PERCENT_DEFAULT = "25";
    public static int MIN_READ_WRITE_IOPS_PERCENT = 1;
    public static int MAX_READ_WRITE_IOPS_PERCENT = 100;
    public static final String RECORD_DETAILS_DEFAULT = "true";
    public static final String RECORD_GSI_VALUE_IN_VIOLATION_RECORD_DEFAULT = "false";
    public static final String EXISTING_GSI_DEFAULT = "false";
    public static final String DETECTION_OUTPUT_PATH_DEFAULT = "./violation_detection.csv";
    public static final String CORRECTION_OUTPUT_PATH_DEFAULT = "./violation_update_errors.csv";
    public static final String TEMP_DETECTION_OUTPUT_PATH = "./detection.tmp";
    public static final String NUM_OF_SEGMENTS_DEFAULT = "1";
    public static int MIN_NUM_OF_SEGMENTS = 1;
    public static int MAX_NUM_OF_SEGMENTS = 4096;
    public static final int NUM_OF_VIOLATIONS_DEFAULT = -1;
    public static final int NUM_OF_RECORDS_DEFAULT = -1;
    public static final String TEMP_CORRECTION_INPUT_PATH = "./correction_input.tmp";
    public static final String TEMP_CORRECTION_OUTPUT_PATH = "./correction_output.tmp";

    /** Options provided by the users */
    private String credentialFilePath = null;
    private Region dynamoDBRegion = null;
    private String tableName = null;
    private String gsiHashKeyName = null;
    private String gsiHashKeyType = null;
    private String gsiRangeKeyName = null;
    private String gsiRangeKeyType = null;
    private boolean recordDetails = true;
    private boolean recordGsiValueInViolationRecord = false;
    private String detectionOutputPath = null;
    private String tmpDetectionOutputPath = null;
    private String correctionOutputPath = null;
    private String tmpCorrectionOutputPath = null;
    private int numOfSegments = 1;
    private long numOfViolations = -1;
    private long numOfRecords = -1;
    private int readWriteIOPSPercent = 25;
    private String correctionInputPath = null;
    private String tmpCorrectionInputPath = null;
    private boolean isDetectionOutputS3Path = false;
    private boolean isInputS3path = false;
    private boolean isCorrectionOutputS3Path = false;

    private Options() {
    };

    public static Options getInstance() {
        return instance;
    }

    public String getCredentialsFilePath() {
        return credentialFilePath;
    }

    public void setCredentialFilePath(String credentialFilePath) {
        this.credentialFilePath = credentialFilePath;
    }

    public Region getDynamoDBRegion() {
        return dynamoDBRegion;
    }

    public void setDynamoDBRegion(Region region) {
        this.dynamoDBRegion = region;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getGsiHashKeyName() {
        return gsiHashKeyName;
    }

    public void setGsiHashKeyName(String gsiHashKeyName) {
        this.gsiHashKeyName = gsiHashKeyName;
    }

    public String getGsiHashKeyType() {
        return gsiHashKeyType;
    }

    public void setGsiHashKeyType(String gsiHashKeyType) {
        this.gsiHashKeyType = gsiHashKeyType;
    }

    public String getGsiRangeKeyName() {
        return gsiRangeKeyName;
    }

    public void setGsiRangeKeyName(String gsiRangeKeyName) {
        this.gsiRangeKeyName = gsiRangeKeyName;
    }

    public String getGsiRangeKeyType() {
        return gsiRangeKeyType;
    }

    public void setGsiRangeKeyType(String gsiRangeKeyType) {
        this.gsiRangeKeyType = gsiRangeKeyType;
    }

    public boolean recordDetails() {
        return recordDetails;
    }

    public void setRecordDetails(boolean recordDetails) {
        this.recordDetails = recordDetails;
    }

    public boolean recordGsiValueInViolationRecord() {
        return recordGsiValueInViolationRecord;
    }

    public void setRecordGsiValueInViolationRecord(boolean recordGsiValueInViolationRecord) {
        this.recordGsiValueInViolationRecord = recordGsiValueInViolationRecord;
    }

    public String getDetectionOutputPath() {
        return detectionOutputPath;
    }

    public void setDetectionOutputPath(String detectionOutputPath) {
        this.detectionOutputPath = detectionOutputPath;
    }

    public String getTmpDetectionOutputPath() {
        return this.tmpDetectionOutputPath;
    }

    public void setTmpDetectionOutputPath(String tmpDetectionOutputPath) {
        this.tmpDetectionOutputPath = tmpDetectionOutputPath;
    }

    public boolean isDetectionOutputS3Path() {
        return this.isDetectionOutputS3Path;
    }

    public void setIsDetectionOutputS3Path(boolean isDetectionOutputS3Path) {
        this.isDetectionOutputS3Path = isDetectionOutputS3Path;
    }

    public String getS3PathBucketName(String path) {
        return path.split("/")[2];
    }

    public String getS3PathKey(String path) {
        int start = path.indexOf("/", 5);
        if (start <= 0)
            throw new IllegalArgumentException("Error: Invalid S3 path: " + path);
        return path.substring(start + 1);
    }

    public int getNumOfSegments() {
        return numOfSegments;
    }

    public void setNumOfSegments(int numOfSegments) {
        this.numOfSegments = numOfSegments;
    }

    public long getNumOfViolations() {
        return numOfViolations;
    }

    public void setNumOfViolations(long numOfViolations) {
        this.numOfViolations = numOfViolations;
    }

    public long getNumOfRecords() {
        return numOfRecords;
    }

    public void setNumOfRecords(long numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    public int getReadWriteIOPSPercent() {
        return readWriteIOPSPercent;
    }

    public void setReadWriteIOPSPercentage(int readWriteIOPSPercent) {
        this.readWriteIOPSPercent = readWriteIOPSPercent;
    }

    public String getCorrectionInputPath() {
        return this.correctionInputPath;
    }

    public void setCorrectionInputPath(String correctionInputPath) {
        this.correctionInputPath = correctionInputPath;
    }

    public boolean isCorrectionInputS3Path() {
        return this.isInputS3path;
    }

    public void setIsCorrectionInputS3Path(boolean isInputS3Path) {
        this.isInputS3path = isInputS3Path;
    }

    public String getTmpCorrectionInputPath() {
        return this.tmpCorrectionInputPath;
    }

    public void setTmpCorrectionInputPath(String tmpCorrectionInputPath) {
        this.tmpCorrectionInputPath = tmpCorrectionInputPath;
    }

    public String getCorrectionOutputPath() {
        return correctionOutputPath;
    }

    public void setCorrectionOutputPath(String correctionOutputPath) {
        this.correctionOutputPath = correctionOutputPath;
    }

    public String getTmpCorrectionOutputPath() {
        return tmpCorrectionOutputPath;
    }

    public void setTmpCorrectionOutputPath(String tmpCorrectionOutputPath) {
        this.tmpCorrectionOutputPath = tmpCorrectionOutputPath;
    }

    public boolean isCorrectionOutputS3Path() {
        return isCorrectionOutputS3Path;
    }

    public void setIsCorrectionOutputS3Path(boolean isCorrectionOutputS3Path) {
        this.isCorrectionOutputS3Path = isCorrectionOutputS3Path;
    }
}
