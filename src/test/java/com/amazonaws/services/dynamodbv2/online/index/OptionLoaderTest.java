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

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.online.index.OptionChecker;
import com.amazonaws.services.dynamodbv2.online.index.OptionLoader;
import com.amazonaws.services.dynamodbv2.online.index.Options;

/**
 * Unit tests for OptionManager.
 * 
 * 
 */
public class OptionLoaderTest {
    private Properties mockProperties = Mockito.mock(Properties.class);
    private OptionChecker mockOptionChecker = Mockito.mock(OptionChecker.class);
    private Options mockOptions = Mockito.mock(Options.class);
    private OptionLoader optionLoader;
    
    @Before
    public void setupBeforeTest() {
        optionLoader = new OptionLoader(mockProperties, mockOptionChecker, mockOptions);
    }

    @Test
    public void testGetOptions() {
        assertEquals("Should return the options instance", optionLoader.getOptions(), mockOptions);
    }

    @Test
    public void testLoadCredentialFilePath() {
        String awsCredentialsFile = "/Documents/credentialfilePath";
        Mockito.when(mockProperties.getProperty(Options.AWS_CREDENTIAL_FILE)).thenReturn(awsCredentialsFile);
        assertEquals("Should return the credential fiel on property file", awsCredentialsFile, optionLoader.loadCredentialFilePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadCredentialFilePathWithValueMissing() {
        Mockito.when(mockProperties.getProperty(Options.AWS_CREDENTIAL_FILE)).thenReturn(null);
        optionLoader.loadCredentialFilePath();
    }

    @Test
    public void testLoadDynamoDBRegion() {
        String dynamoDBRegion = "us-west-2";
        Mockito.when(mockProperties.getProperty(Options.DYNAMODB_REGION)).thenReturn(dynamoDBRegion);
        assertEquals("Should return the valid region", Region.getRegion(Regions.fromName(dynamoDBRegion)), optionLoader.loadDynamoDBRegion());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadDynamoDBRegionWithInvalidRegionname() {
        Mockito.when(mockProperties.getProperty(Options.DYNAMODB_REGION)).thenReturn("invalidregion");
        optionLoader.loadDynamoDBRegion();
    }

    @Test
    public void testLoadTableName() {
        String tableName = "Table1";
        Mockito.when(mockProperties.getProperty(Options.TABLE_NAME)).thenReturn(tableName);
        assertEquals("Should return the right table name", tableName, optionLoader.loadTableName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadTableNameWithValueMissing() {
        Mockito.when(mockProperties.getProperty(Options.TABLE_NAME)).thenReturn(null);
        optionLoader.loadTableName();
    }

    @Test
    public void testLoadGsiHashKeyName() {
        String gsiHashKeyName = "gsiHashKeyName";
        Mockito.when(mockProperties.getProperty(Options.GSI_HASH_KEY_NAME, null)).thenReturn(gsiHashKeyName);
        assertEquals("Should return the right gsi hash key name", gsiHashKeyName, optionLoader.loadGsiHashKeyName());
    }

    @Test
    public void testLoadGsiHashKeyType() {
        String gsiHashKeyType = "N";
        Mockito.when(mockProperties.getProperty(Options.GSI_HASH_KEY_TYPE, null)).thenReturn(gsiHashKeyType);
        Mockito.when(mockOptionChecker.isValidKeyType(gsiHashKeyType)).thenReturn(true);
        assertEquals("Should return the right gsi hash key tyype", gsiHashKeyType, optionLoader.loadGsiHashKeyType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadGsiHashKeyTypeWithInvalidKeyType() {
        String invalidGsiRangeKeyType = "invalidKeyType";
        Mockito.when(mockProperties.getProperty(Options.GSI_HASH_KEY_TYPE, null)).thenReturn(invalidGsiRangeKeyType);
        Mockito.when(mockOptionChecker.isValidKeyType(invalidGsiRangeKeyType)).thenReturn(false);
        optionLoader.loadDetectionOptions();
    }

    @Test
    public void testCheckGsiHashKey() {
        optionLoader.checkGsiHashKey("gsiHashKeyName", "N");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckGsiHashKeyWithHashKeyNameMissing() {
        optionLoader.checkGsiHashKey(null, "N");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckGsiHashKeyWithHashKeyTypeMissing() {
        optionLoader.checkGsiHashKey("gsiHashKeyName", null);
    }

    @Test
    public void testLoadGsiRangeKeyName() {
        String gsiRangeKeyName = "gsiRangeKeyName";
        Mockito.when(mockProperties.getProperty(Options.GSI_RANGE_KEY_NAME, null)).thenReturn(gsiRangeKeyName);
        assertEquals("Should return the right gsi range key name", gsiRangeKeyName, optionLoader.loadGsiRangeKeyName());
    }
    
    @Test
    public void testLoadGsiRangeKeyType(){
        String gsiRangeKeyType = "S";
        Mockito.when(mockProperties.getProperty(Options.GSI_RANGE_KEY_TYPE, null)).thenReturn(gsiRangeKeyType);
        Mockito.when(mockOptionChecker.isValidKeyType(gsiRangeKeyType)).thenReturn(true);
        assertEquals("Should return right gsi range key type", gsiRangeKeyType, optionLoader.loadGsiRangeKeyType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadGsiRangeKeyTypeWithInvalidType() {
        Mockito.when(mockProperties.getProperty(Options.GSI_RANGE_KEY_TYPE, null)).thenReturn("invalidtype");
        optionLoader.loadDetectionOptions();
    }
    
    @Test
    public void testCheckGsiRangeKey(){
        optionLoader.checkGsiRangeKey("gsiRangeKeyName", "S");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCheckGsiRangeKeyWithRangeKeyNameMissing(){
        optionLoader.checkGsiRangeKey(null, "N");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCheckGsiRangeKeyWithRangeKeyTypeMissing(){
        optionLoader.checkGsiRangeKey("gsiRangeKeyName", null);
    }
    
    @Test
    public void testCheckGsiHashKeyAndRangeKey(){
        optionLoader.checkGsiHashKeyAndRangeKey("gsiHashKeyName", "gsiRangeKeyName");
    }
    
    @Test
    public void testCheckGsiHashKeyAndRangeKeyWithRangeKeyNameEmpty(){
        optionLoader.checkGsiHashKeyAndRangeKey("gsiHashKeyName", null);
    }
    
    @Test
    public void testCheckGsiHashKeyAndRangeKeyWithHashKeyNameEmpty(){
        optionLoader.checkGsiHashKeyAndRangeKey(null, "gsiRangeKeyName");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCheckGsiHashKeyAndRangeKeyWithSameName(){
        optionLoader.checkGsiHashKeyAndRangeKey("gsiHashKeyName", "gsiHashKeyName");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCheckGsiHashKeyAndRangeKeyWithBothNameEmpty(){
        optionLoader.checkGsiHashKeyAndRangeKey(null, null);
    }
    
    @Test
    public void testLoadScanIOPSPercent(){
        String ScanIOPSPercent = "23";
        Mockito.when(mockProperties.getProperty(Options.READ_WRITE_IOPS_PERCENT, Options.READ_WRITE_IOPS_PERCENT_DEFAULT)).thenReturn(ScanIOPSPercent);
        Mockito.when(mockOptionChecker.isNumberInRange(Integer.parseInt(ScanIOPSPercent), Options.MIN_READ_WRITE_IOPS_PERCENT, Options.MAX_READ_WRITE_IOPS_PERCENT)).thenReturn(true);
        assertEquals("Should return the scan IOPS set", Integer.parseInt(ScanIOPSPercent), optionLoader.loadScanIOPSPercent());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLoadScanIOPSPercentWithInvalidInteger() {
        Mockito.when(mockProperties.getProperty(Options.READ_WRITE_IOPS_PERCENT, Options.READ_WRITE_IOPS_PERCENT_DEFAULT)).thenReturn("notaninteger");
        optionLoader.loadScanIOPSPercent();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadScanIOPSPercentWithIntegerExceedsRange() {
        String invalidScanIOPSPercent = "-23";
        Mockito.when(mockProperties.getProperty(Options.READ_WRITE_IOPS_PERCENT, Options.READ_WRITE_IOPS_PERCENT_DEFAULT)).thenReturn(invalidScanIOPSPercent);
        Mockito.when(mockOptionChecker.isNumberInRange(Integer.parseInt(invalidScanIOPSPercent), Options.MIN_READ_WRITE_IOPS_PERCENT, Options.MAX_READ_WRITE_IOPS_PERCENT)).thenReturn(false);
        optionLoader.loadScanIOPSPercent();
    }

    @Test
    public void testLoadRecordDetails(){
        String recordDeteils = "true";
        Mockito.when(mockProperties.getProperty(Options.RECORD_DETAILS, Options.RECORD_DETAILS_DEFAULT)).thenReturn(recordDeteils);
        assertEquals("Should return the boolean set for record details", Boolean.parseBoolean(recordDeteils), optionLoader.loadRecordDetails());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLoadRecordDetailsWithInvalidBoolean() {
        Mockito.when(mockProperties.getProperty(Options.RECORD_DETAILS, Options.RECORD_DETAILS_DEFAULT)).thenReturn("invalidboolean");
        optionLoader.loadRecordDetails();
    }

    @Test
    public void testLoadOutputPath(){
        String outputPath = "/Document/outputPath";
        Mockito.when(mockProperties.getProperty(Options.DETECTION_OUTPUT_PATH, Options.DETECTION_OUTPUT_PATH_DEFAULT)).thenReturn(outputPath);
        assertEquals("Should return the the given output path", outputPath, optionLoader.loadDetectionOutputPath());
    }
    
    @Test
    public void testLoadNumOfSegments(){
        String numOfSegments = "12";
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_SEGMENTS, Options.NUM_OF_SEGMENTS_DEFAULT)).thenReturn(numOfSegments);
        Mockito.when(mockOptionChecker.isNumberInRange(Integer.parseInt(numOfSegments), Options.MIN_NUM_OF_SEGMENTS, Options.MAX_NUM_OF_SEGMENTS)).thenReturn(true);
        assertEquals("Should return the given num of segments", Integer.parseInt(numOfSegments), optionLoader.loadNumOfSegments());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLoadNumOfSegmentsWithInvalidInteger() {
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_SEGMENTS, Options.NUM_OF_SEGMENTS_DEFAULT)).thenReturn("invalidint");
        optionLoader.loadNumOfSegments();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadNumOfSegmentsWithNumberExceedsRange() {
        String numOfSegments = "-12";
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_SEGMENTS, Options.NUM_OF_SEGMENTS_DEFAULT)).thenReturn(numOfSegments);
        Mockito.when(mockOptionChecker.isNumberInRange(Integer.parseInt(numOfSegments), Options.MIN_NUM_OF_SEGMENTS, Options.MAX_NUM_OF_SEGMENTS)).thenReturn(false);
        optionLoader.loadNumOfSegments();
    }

    @Test
    public void testLoadNumOfViolations(){
        String numOfViolaitons = "1234";
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_VIOLATIONS)).thenReturn(numOfViolaitons);
        assertEquals("Should return the given num of violation", Integer.parseInt(numOfViolaitons), optionLoader.loadNumOfViolations());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLoadNumOfViolationsWithInvalidInteger() {
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_VIOLATIONS)).thenReturn("invalidInteger");
        optionLoader.loadNumOfViolations();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadNumOfViolationsWithValueExceedsRange() {
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_VIOLATIONS)).thenReturn("-20");
        optionLoader.loadNumOfViolations();
    }

    @Test
    public void testLoadNumOfRecords(){
        String numOfRecords = "1234";
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_RECORDS)).thenReturn(numOfRecords);
        assertEquals("Should return the number of records set", Integer.parseInt(numOfRecords), optionLoader.loadNumOfRecords());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLoadNumOfRecordsWithInvalidInteger() {
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_RECORDS)).thenReturn("invalidInt");
        optionLoader.loadNumOfRecords();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadNumOfRecordsWithValueExceedsRange() {
        Mockito.when(mockProperties.getProperty(Options.NUM_OF_RECORDS)).thenReturn("-20");
        optionLoader.loadNumOfRecords();
    }
    
    @Test
    public void testloadCorrectionInputPath(){
        String correctionInputPath = "/Documents/CorrectionInputPath";
        Mockito.when(mockProperties.getProperty(Options.CORRECTION_INPUT_PATH)).thenReturn(correctionInputPath);
        assertEquals("Should return the path set for correction input", correctionInputPath, optionLoader.loadCorrectionInputPath());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testloadCorrectionInputPathWithPathMissing() {
        Mockito.when(mockProperties.getProperty(Options.CORRECTION_INPUT_PATH)).thenReturn(null);
        optionLoader.loadCorrectionInputPath();
    }
}
