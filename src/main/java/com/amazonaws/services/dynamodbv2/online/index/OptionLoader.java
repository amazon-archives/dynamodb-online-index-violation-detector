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
import java.util.Properties;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Load options from property file and check options.
 * 
 */
public class OptionLoader {
    private Properties properties = new Properties();
    private OptionChecker optionChecker;
    private Options options = Options.getInstance();

    /**
     * Constructor for unit test purpose only.
     */
    protected OptionLoader(Properties properties, OptionChecker optionChecker, Options options) {
        this.properties = properties;
        this.optionChecker = optionChecker;
        this.options = options;
    }

    public OptionLoader(String propertyFilePath) {
        optionChecker = new OptionChecker();
        loadPropertyFile(propertyFilePath);
    }

    protected void loadPropertyFile(String propertyFilePath) {
        try {
            properties.load(new FileInputStream(propertyFilePath));
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException("Error: Property file" + propertyFilePath + " does not exist.");
        } catch (IOException e) {
            throw new IllegalArgumentException("Error: Failed to load properties from " + propertyFilePath + " .");
        }
    }

    public Options getOptions() {
        return this.options;
    }

    /**
     * Load common properties for both detection and correction.
     */
    private void loadCommonProperties() throws IllegalArgumentException {
        String credentialFilePath = loadCredentialFilePath();
        options.setCredentialFilePath(credentialFilePath);

        Region region = loadDynamoDBRegion();
        options.setDynamoDBRegion(region);

        String tableName = loadTableName();
        options.setTableName(tableName);

        String gsiHashKeyName = loadGsiHashKeyName();
        String gsiHashKeyType = loadGsiHashKeyType();
        checkGsiHashKey(gsiHashKeyName, gsiHashKeyType);
        options.setGsiHashKeyName(gsiHashKeyName);
        options.setGsiHashKeyType(gsiHashKeyType);

        String gsiRangeKeyName = loadGsiRangeKeyName();
        String gsiRangeKeyType = loadGsiRangeKeyType();
        checkGsiRangeKey(gsiRangeKeyName, gsiRangeKeyType);
        checkGsiHashKeyAndRangeKey(gsiHashKeyName, gsiRangeKeyName);
        options.setGsiRangeKeyName(gsiRangeKeyName);
        options.setGsiRangeKeyType(gsiRangeKeyType);

        int scanIOPSPercent = loadScanIOPSPercent();
        options.setReadWriteIOPSPercentage(scanIOPSPercent);
    }

    protected String loadCredentialFilePath() throws IllegalArgumentException {
        String credentialFilePath = properties.getProperty(Options.AWS_CREDENTIAL_FILE);
        if (null == credentialFilePath) {
            throw new IllegalArgumentException("Error: " + Options.AWS_CREDENTIAL_FILE + " missing.");
        }
        return credentialFilePath.trim();
    }

    protected Region loadDynamoDBRegion() throws IllegalArgumentException {
        String regionName = properties.getProperty(Options.DYNAMODB_REGION);
        if (null == regionName) {
            throw new IllegalArgumentException("Error: " + Options.DYNAMODB_REGION + " dynamoDBRegion missing.");
        }
        Region region = null;
        try {
            region = Region.getRegion(Regions.fromName(regionName.trim()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Error: Given " + Options.DYNAMODB_REGION + " " + regionName + " is invalid.");
        }
        return region;
    }

    protected String loadTableName() throws IllegalArgumentException {
        String tableName = properties.getProperty(Options.TABLE_NAME);
        if (null == tableName) {
            throw new IllegalArgumentException("Error: " + Options.TABLE_NAME + " missing.");
        }
        return tableName.trim();
    }

    protected String loadGsiHashKeyName() {
        return properties.getProperty(Options.GSI_HASH_KEY_NAME, null);
    }

    protected String loadGsiHashKeyType() throws IllegalArgumentException {
        String gsiHashKeyType = properties.getProperty(Options.GSI_HASH_KEY_TYPE, null);
        if (null != gsiHashKeyType) {
            gsiHashKeyType = gsiHashKeyType.trim();
            if(!optionChecker.isValidKeyType(gsiHashKeyType)) {
                throw new IllegalArgumentException("Error: Given " + Options.GSI_HASH_KEY_TYPE + " " + gsiHashKeyType + " not valid key type.");
            }
        }
        return gsiHashKeyType;
    }

    protected void checkGsiHashKey(String gsiHashKeyName, String gsiHashKeyType) throws IllegalArgumentException {
        if (null == gsiHashKeyName && null != gsiHashKeyType) {
            throw new IllegalArgumentException("Error: " + Options.GSI_HASH_KEY_TYPE + " set while " + Options.GSI_HASH_KEY_NAME + " missing.");
        }
        if (null != gsiHashKeyName && null == gsiHashKeyType) {
            throw new IllegalArgumentException("Error: " + Options.GSI_HASH_KEY_NAME + " set while " + Options.GSI_HASH_KEY_TYPE + " missing.");
        }
    }

    protected String loadGsiRangeKeyName() throws IllegalArgumentException {
        return properties.getProperty(Options.GSI_RANGE_KEY_NAME, null);
    }

    protected String loadGsiRangeKeyType() throws IllegalArgumentException {
        String gsiRangeKeyType = properties.getProperty(Options.GSI_RANGE_KEY_TYPE, null);
        if(null != gsiRangeKeyType) {
            gsiRangeKeyType = gsiRangeKeyType.trim();
            if(!optionChecker.isValidKeyType(gsiRangeKeyType)) {
                throw new IllegalArgumentException("Error: Given " + Options.GSI_RANGE_KEY_TYPE + " " + gsiRangeKeyType + " not a valid key type.");
            }
        }
        return gsiRangeKeyType;
    }

    protected void checkGsiRangeKey(String gsiRangeKeyName, String gsiRangeKeyType) throws IllegalArgumentException {
        if (null == gsiRangeKeyName && null != gsiRangeKeyType) {
            throw new IllegalArgumentException("Error: " + Options.GSI_RANGE_KEY_TYPE + "set while " + Options.GSI_RANGE_KEY_NAME + " missing.");
        }
        if (null != gsiRangeKeyName && null == gsiRangeKeyType) {
            throw new IllegalArgumentException("Error: " + Options.GSI_RANGE_KEY_TYPE + "set while " + Options.GSI_RANGE_KEY_NAME + " missing.");
        }
    }

    protected void checkGsiHashKeyAndRangeKey(String gsiHashKeyName, String gsiRangeKeyName) throws IllegalArgumentException {
        if (null != gsiRangeKeyName && null != gsiHashKeyName && gsiHashKeyName.equals(gsiRangeKeyName)) {
            throw new IllegalArgumentException("Error: " + Options.GSI_HASH_KEY_NAME + " and " + Options.GSI_RANGE_KEY_NAME + " should be different.");
        }
        if (null == gsiHashKeyName && null == gsiRangeKeyName) {
            throw new IllegalArgumentException("Error: '" + Options.GSI_HASH_KEY_NAME + "' and '" + Options.GSI_RANGE_KEY_NAME + "' can not be null together.");
        }
    }

    protected int loadScanIOPSPercent() throws IllegalArgumentException {
        String scanIOPSPercentStr = properties.getProperty(Options.READ_WRITE_IOPS_PERCENT, Options.READ_WRITE_IOPS_PERCENT_DEFAULT).trim();
        try {
            int scanIOPSPercent = Integer.parseInt(scanIOPSPercentStr);
            if (!optionChecker.isNumberInRange(scanIOPSPercent, Options.MIN_READ_WRITE_IOPS_PERCENT, Options.MAX_READ_WRITE_IOPS_PERCENT)) {
                throw new IllegalArgumentException("Error: Given " + Options.READ_WRITE_IOPS_PERCENT + " " + scanIOPSPercentStr + " exceeds range "
                        + Options.MIN_READ_WRITE_IOPS_PERCENT + " ~ " + Options.MAX_READ_WRITE_IOPS_PERCENT + " .");
            }
            return scanIOPSPercent;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Error: Given " + Options.READ_WRITE_IOPS_PERCENT + " " + scanIOPSPercentStr + " not valid integer format.");
        }
    }

    /**
     * Load options for violation detection
     */
    public void loadDetectionOptions() throws IllegalArgumentException {
        loadCommonProperties();

        boolean recordDetail = loadRecordDetails();
        options.setRecordDetails(recordDetail);

        boolean recordGsiValueInViolationRecord = loadRecordGsiValueInViolationRecord();
        options.setRecordGsiValueInViolationRecord(recordGsiValueInViolationRecord);
        checkRecordDetailsAndRecordGsiValueConflict(recordDetail, recordGsiValueInViolationRecord);

        String outputPath = loadDetectionOutputPath();
        options.setDetectionOutputPath(outputPath);
        boolean isOutputS3Path = optionChecker.isS3Path(outputPath);
        if (isOutputS3Path) {
            options.setTmpDetectionOutputPath(Options.TEMP_DETECTION_OUTPUT_PATH);
        }
        options.setIsDetectionOutputS3Path(isOutputS3Path);

        int numOfSegments = loadNumOfSegments();
        options.setNumOfSegments(numOfSegments);

        int numOfViolations = loadNumOfViolations();
        options.setNumOfViolations(numOfViolations);

        int numOfRecords = loadNumOfRecords();
        options.setNumOfRecords(numOfRecords);
    }

    protected boolean loadRecordDetails() throws IllegalArgumentException {
        String recordDetail = properties.getProperty(Options.RECORD_DETAILS, Options.RECORD_DETAILS_DEFAULT).trim();
        if (!recordDetail.equalsIgnoreCase("true") && !recordDetail.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("Error: Given " + Options.RECORD_DETAILS + " invalid,  should be 'true' or 'false' if set.");
        }
        return Boolean.parseBoolean(recordDetail);
    }

    protected boolean loadRecordGsiValueInViolationRecord() throws IllegalArgumentException {
        String recordGsiValueInViolationRecord = properties.getProperty(Options.RECORD_GSI_VALUE_IN_VIOLATION_RECORD, Options.READ_WRITE_IOPS_PERCENT_DEFAULT).trim();
        if (!recordGsiValueInViolationRecord.equalsIgnoreCase("true") && !recordGsiValueInViolationRecord.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("Error: Given " + Options.RECORD_GSI_VALUE_IN_VIOLATION_RECORD + " value '" + recordGsiValueInViolationRecord
                    + "' invalid,  should be 'true' or 'false' if set.");
        }
        return Boolean.parseBoolean(recordGsiValueInViolationRecord);
    }

    protected void checkRecordDetailsAndRecordGsiValueConflict(boolean recordDetail, boolean recordGsiValueInViolationRecord) {
        if (!recordDetail && recordGsiValueInViolationRecord) {
            throw new IllegalArgumentException("Error: Conflcct! " + Options.RECORD_GSI_VALUE_IN_VIOLATION_RECORD + " set true while " + Options.RECORD_DETAILS
                    + " set as false.");
        }
    }

    protected String loadDetectionOutputPath() {
        return properties.getProperty(Options.DETECTION_OUTPUT_PATH, Options.DETECTION_OUTPUT_PATH_DEFAULT).trim();
    }

    protected int loadNumOfSegments() throws IllegalArgumentException {
        String numOfSegmentsStr = properties.getProperty(Options.NUM_OF_SEGMENTS, Options.NUM_OF_SEGMENTS_DEFAULT).trim();
        try {
            int numOfSegments = Integer.parseInt(numOfSegmentsStr);
            if (!optionChecker.isNumberInRange(numOfSegments, Options.MIN_NUM_OF_SEGMENTS, Options.MAX_NUM_OF_SEGMENTS)) {
                throw new IllegalArgumentException("Error: Given " + Options.NUM_OF_SEGMENTS + " " + numOfSegmentsStr + " exceeds range "
                        + Options.MIN_NUM_OF_SEGMENTS + " ~ " + Options.MAX_NUM_OF_SEGMENTS + ".");
            }
            return numOfSegments;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Error: Given " + Options.NUM_OF_SEGMENTS + " " + numOfSegmentsStr + " is not valid integer format.");
        }
    }

    protected int loadNumOfViolations() throws IllegalArgumentException {
        String numOfViolationsStr = properties.getProperty(Options.NUM_OF_VIOLATIONS);
        try {
            /** If not set, use default. Else check if negative */
            int numOfViolations = Options.NUM_OF_VIOLATIONS_DEFAULT;
            if (null != numOfViolationsStr) {
                numOfViolations = Integer.parseInt(numOfViolationsStr.trim());
                if (numOfViolations != Options.NUM_OF_VIOLATIONS_DEFAULT && numOfViolations <= 0) {
                    throw new IllegalArgumentException("Error: Given " + Options.NUM_OF_VIOLATIONS + " " + numOfViolationsStr + " invalid, must be positive.");
                }
            }
            return numOfViolations;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Error: Given " + Options.NUM_OF_VIOLATIONS + numOfViolationsStr + " is not a valid integer format.");
        }
    }

    protected int loadNumOfRecords() throws IllegalArgumentException {
        String numOfRecordsStr = properties.getProperty(Options.NUM_OF_RECORDS);
        try {
            /** If not set, use default. Else check if negative */
            int numOfRecords = Options.NUM_OF_RECORDS_DEFAULT;
            if (null != numOfRecordsStr) {
                numOfRecords = Integer.parseInt(numOfRecordsStr.trim());
                if (numOfRecords != Options.NUM_OF_RECORDS_DEFAULT && numOfRecords <= 0) {
                    throw new IllegalArgumentException("Error: Given " + Options.NUM_OF_RECORDS + " " + numOfRecords + " invalid, must be positive.");
                }
            }
            return numOfRecords;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Error: Given " + Options.NUM_OF_RECORDS + " " + numOfRecordsStr + " is not a valid integer format.");
        }
    }

    /**
     * Load options for correction.
     */
    public void loadCorrectionOptions() throws IllegalArgumentException {
        loadCommonProperties();

        String correctionInputPath = loadCorrectionInputPath();
        options.setCorrectionInputPath(correctionInputPath);
        boolean isInputS3Path = optionChecker.isS3Path(correctionInputPath);
        if (isInputS3Path) {
            options.setTmpCorrectionInputPath(Options.TEMP_CORRECTION_INPUT_PATH);
        }
        options.setIsCorrectionInputS3Path(isInputS3Path);
        
        String correctionOutputPath = loadCorrectionOutputPath();
        options.setCorrectionOutputPath(correctionOutputPath);
        boolean isCorrectionOutputS3Path = optionChecker.isS3Path(correctionOutputPath);
        if (isCorrectionOutputS3Path) {
            options.setTmpCorrectionOutputPath(Options.TEMP_CORRECTION_OUTPUT_PATH);
        }
        options.setIsCorrectionOutputS3Path(isCorrectionOutputS3Path);
        
        // validate that input and output paths are not the same
        if(correctionInputPath.equals(correctionOutputPath)) {
            throw new IllegalArgumentException("Error: " + Options.CORRECTION_INPUT_PATH + " and " + 
                    Options.CORRECTION_OUTPUT_PATH + " cannot be the same.");
        }
    }

    protected String loadCorrectionInputPath() throws IllegalArgumentException {
        String correctionInputPath = properties.getProperty(Options.CORRECTION_INPUT_PATH);
        if (null == correctionInputPath) {
            throw new IllegalArgumentException("Error: " + Options.CORRECTION_INPUT_PATH + " missing.");
        }
        return correctionInputPath.trim();
    }
    
    protected String loadCorrectionOutputPath() throws IllegalArgumentException {
        return properties.getProperty(Options.CORRECTION_OUTPUT_PATH, Options.CORRECTION_OUTPUT_PATH_DEFAULT).trim();
    }
}
