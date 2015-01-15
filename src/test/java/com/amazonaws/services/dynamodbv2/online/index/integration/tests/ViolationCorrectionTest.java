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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.online.index.AWSConnection;
import com.amazonaws.services.dynamodbv2.online.index.AttributeValueConverter;
import com.amazonaws.services.dynamodbv2.online.index.Correction;
import com.amazonaws.services.dynamodbv2.online.index.Options;
import com.amazonaws.services.dynamodbv2.online.index.TableHelper;
import com.amazonaws.services.dynamodbv2.online.index.TableReader;
import com.amazonaws.services.dynamodbv2.online.index.ViolationDetector;
import com.amazonaws.services.dynamodbv2.online.index.ViolationRecord;
import com.amazonaws.services.dynamodbv2.online.index.integration.tests.TestUtils.ExitException;
import com.amazonaws.services.dynamodbv2.online.index.integration.tests.TestUtils.NoExitSecurityManager;

/**
 * 
 * Functional tests for violation correction.
 *
 */
public class ViolationCorrectionTest {
    
    private static TableManager tableManager;
    private static AWSConnection awsConnection;
    private static AmazonDynamoDBClient dynamoDBClient;
    
    private static final Set<String> tablesToDelete = new HashSet<String>();
    
    private static final int READ_CAPACITY = 10;
    private static final int WRITE_CAPACITY = 10;
    
    // AWS credentials and region
    private static final String AWS_CREDENTIAL_PATH = "./config/credentials";
    private static final Region DYNAMODB_REGION = Region.getRegion(Regions.US_WEST_2);

    // Input and Output Paths
    private static final String DETECTION_OP_FILE = "./test-detection-1.csv";
    private static final String CORRECTION_INPUT_FILE = "./test-correction-1.csv";
    private static final String CORRECTION_OUTPUT_FILE = "./test-correction-output.csv";
    private static final String CORRECTION_TABLE = "correction-test-1";
    
    // Definition for table attributes
    private static final String CORRECTION_TABLE_HK = "HK";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION = "HSV";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION = "RSV";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2 = "HSV2";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2 = "RSV2";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3 = "HSV3";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3 = "RSV3";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4 = "HSV4";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_4 = "RSV4";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION = "HTV";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION = "RTV";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2 = "HTV2";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2 = "RTV2";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3 = "HTV3";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3 = "RTV3";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_4 = "HTV4";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_4 = "RTV4";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5 = "HTV5";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION = "HSTV";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION = "RSTV";
    
    // Below are used for conditional update tests
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER = "HVCN";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING = "HVCS";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING = "RVCS";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY = "HVCB";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY = "RVCB";
    
    // Types for all above table attribute names
    private static final String CORRECTION_TABLE_HK_TYPE = "N";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_TYPE = "B";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_TYPE = "S";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2_TYPE = "S";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2_TYPE = "B";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3_TYPE = "S";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3_TYPE = "S";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4_TYPE = "B";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_4_TYPE = "B";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE = "N";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE = "B";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE = "N";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE = "S";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE = "B";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE = "S";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_4_TYPE = "N";
    private static final String CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_4_TYPE = "B";
    private static final String CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE = "S";
    private static final String CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE = "B";
    private static final String CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE = "S";
    
    // Counts for violating and non-violating items for each attribute
    private static final int HSV_VIOLATION_COUNT = 4;
    private static final int HSV_NON_VIOLATION_COUNT = 2;
    private static final int RSV_VIOLATION_COUNT = 13;
    private static final int RSV_NON_VIOLATION_COUNT = 1;
    private static final int HSV_2_VIOLATION_COUNT = 3;
    private static final int HSV_2_NON_VIOLATION_COUNT = 8;
    private static final int RSV_2_VIOLATION_COUNT = 5;
    private static final int RSV_2_NON_VIOLATION_COUNT = 10;
    private static final int HSV_3_VIOLATION_COUNT = 6;
    private static final int HSV_3_NON_VIOLATION_COUNT = 4;
    private static final int RSV_3_VIOLATION_COUNT = 5;
    private static final int RSV_3_NON_VIOLATION_COUNT = 3;
    private static final int HSV_4_VIOLATION_COUNT = 6;
    private static final int HSV_4_NON_VIOLATION_COUNT = 8;
    private static final int RSV_4_VIOLATION_COUNT = 7;
    private static final int RSV_4_NON_VIOLATION_COUNT = 9;
    private static final int HTV_VIOLATION_COUNT = 7;
    private static final int HTV_NON_VIOLATION_COUNT = 5;
    private static final int RTV_VIOLATION_COUNT = 3;
    private static final int RTV_NON_VIOLATION_COUNT = 6;
    private static final int HTV_2_VIOLATION_COUNT = 6;
    private static final int HTV_2_NON_VIOLATION_COUNT = 3;
    private static final int RTV_2_VIOLATION_COUNT = 2;
    private static final int RTV_2_NON_VIOLATION_COUNT = 10;
    private static final int HTV_3_VIOLATION_COUNT = 9;
    private static final int HTV_3_NON_VIOLATION_COUNT = 4;
    private static final int RTV_3_VIOLATION_COUNT = 7;
    private static final int RTV_3_NON_VIOLATION_COUNT = 6;
    private static final int HTV_4_VIOLATION_COUNT = 5;
    private static final int HTV_4_NON_VIOLATION_COUNT = 3;
    private static final int HTV_5_VIOLATION_COUNT = 20;
    private static final int HTV_5_NON_VIOLATION_COUNT = 10;
    private static final int RTV_4_VIOLATION_COUNT = 8;
    private static final int RTV_4_NON_VIOLATION_COUNT = 3;
    private static final int HSTV_VIOLATION_COUNT_TYPE = 4;
    private static final int HSTV_VIOLATION_COUNT_SIZE = 2;
    private static final int HSTV_NON_VIOLATION_COUNT = 13;
    private static final int RSTV_VIOLATION_COUNT_TYPE = 1;
    private static final int RSTV_VIOLATION_COUNT_SIZE = 3;
    private static final int RSTV_NON_VIOLATION_COUNT = 8;
    private static final int HVCN_VIOLATION_COUNT = 10;
    private static final int HVCN_NON_VIOLATION_COUNT = 5;
    private static final int HVCB_VIOLATION_COUNT = 9;
    private static final int HVCB_NON_VIOLATION_COUNT = 4;
    private static final int RVCB_VIOLATION_COUNT = 7;
    private static final int RVCB_NON_VIOLATION_COUNT = 5;
    private static final int HVCS_VIOLATION_COUNT = 6;
    private static final int HVCS_NON_VIOLATION_COUNT = 5;
    private static final int RVCS_VIOLATION_COUNT = 4;
    private static final int RVCS_NON_VIOLATION_COUNT = 3;
    
    // List of violations for some attributes
    private static List<Map<String, AttributeValue>> hvcnViolations;
    private static List<Map<String, AttributeValue>> hvcsViolations;
    private static List<Map<String, AttributeValue>> rvcsViolations;
    private static List<Map<String, AttributeValue>> hvcbViolations;
    private static List<Map<String, AttributeValue>> rvcbViolations;
    
    // Saving state of security manager to revert after tests
    private static final SecurityManager securityManager = System.getSecurityManager();
    
    @BeforeClass
    public static void setup() throws FileNotFoundException, IOException {
        awsConnection = new AWSConnection(AWS_CREDENTIAL_PATH);
        dynamoDBClient = awsConnection.getDynamoDBClient(DYNAMODB_REGION, TestUtils.RUN_TESTS_ON_DYNAMODB_LOCAL);
        tableManager = new TableManager(dynamoDBClient);
        generateTableAndDataForCorrectorTable();
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @AfterClass
    public static void tearDown() {
        System.setSecurityManager(securityManager);
        
        if(tablesToDelete != null) {
            for(String tableName : tablesToDelete) {
                tableManager.deleteTable(tableName);
            }
        }
        
        // Delete the output files
        String[] fileNames = {DETECTION_OP_FILE, CORRECTION_INPUT_FILE, CORRECTION_OUTPUT_FILE};
        TestUtils.deleteFiles(fileNames);
    }
    
    /**
     * Tests non-conditional violation correction for both gsi hash and range 'size' violations specified in the input file.
     */
    @Test
    public void correctionWithUpdatesForHashAndRangeSizeViolationsSpecifiedTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_TYPE,
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_TYPE,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, (HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT) /*successfulRequests*/, 
                (HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT) /*violationUpdates*/,
                null /*conditionalFailures*/, 0 /*unexpectedErrors*/);

        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after correction has run", 0, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION, tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap);
    }
    
    /**
     * Tests non-conditional violation correction for only gsi hash 'size' violations specified in the input file.
     */
    @Test
    public void correctionWithUpdatesForHashSizeViolationsSpecifiedTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, HSV_2_VIOLATION_COUNT /*successfulRequests*/, 
                HSV_2_VIOLATION_COUNT /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after correction has run", 0, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2, 
                null /*gsiRangeKeyName*/, tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/);
    }
    
    /**
     * Tests non-conditional violation correction for only gsi range 'size' violations specified in the input file.
     */
    @Test
    public void correctionWithUpdatesForRangeSizeViolationsSpecifiedTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                null /*gsiHashKeyName*/, null /*gsiHashKeyType*/,
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2_TYPE,
                null /*tableHashToNewGsiHashValueMap*/, tableHashToNewGsiRangeValueMap, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/,
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, RSV_2_VIOLATION_COUNT /*successfulRequests*/, 
                RSV_2_VIOLATION_COUNT /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after correction has run", 0, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null /*gsiHashKeyName*/, 
                CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2, null /*tableHashToNewGsiHashValueMap*/, tableHashToNewGsiRangeValueMap);
    }
    
    /**
     * Tests non-conditional violation correction for gsi hash 'size' violations and 
     * gsi range 'type' violations specified in the input file.
     */
    @Test
    public void correctionWithUpdatesForHashSizeAndRangeTypeViolationsSpecifiedTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE,
                CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, 
                (HSTV_VIOLATION_COUNT_SIZE + RSTV_VIOLATION_COUNT_SIZE + HSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_TYPE) /*successfulRequests*/, 
                (HSTV_VIOLATION_COUNT_SIZE + RSTV_VIOLATION_COUNT_SIZE + HSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_TYPE) /*violationUpdates*/, 
                null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after correction has run", 0, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, 
                CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap);
    }
    
    /**
     * Tests non-conditional violation correction with some updates missing from the input file.
     * No update should happen on items with missing values.
     */
    @Test
    public void correctionWithSomeUpdatesMissingTest() throws IllegalArgumentException, IOException {
        
        // number of updates missing for hash/range key
        int missingUpdates = 2;
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, missingUpdates);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, 
                (HTV_VIOLATION_COUNT + RTV_VIOLATION_COUNT - (2 * missingUpdates)) /*successfulRequests*/, 
                (HTV_VIOLATION_COUNT + RTV_VIOLATION_COUNT - (2 * missingUpdates)) /*violationUpdates*/, 
                null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are only 'missing updates' number of violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after correction has run", (2 * missingUpdates), result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap);
    }
    
    /**
     * Tests non-conditional violation correction with no updates specified in the input file.
     * Nothing should be updated in this case.
     */
    @Test
    public void correctionWithNoUpdatesTest() throws IllegalArgumentException, IOException {
        
        // generate file containing violations
        int[] before = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // run violation correction without any updates
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, 
                0 /*successfulRequests*/, 0 /*violationUpdates*/,
                null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are only 'missing updates' number of violations
        int[] after = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found before running correction changed after running correction even when no updates were specified", 
                before[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX], after[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        Assert.assertEquals("Items scanned before running correction changed after running correction even when no updates were specified", 
                before[TestUtils.DETECTOR_ITEMS_SCANNED_INDEX], after[TestUtils.DETECTOR_ITEMS_SCANNED_INDEX]);
    }
    
    /**
     * Tests non-conditional violation correction with no updates specified but
     * 'Delete Blank Attributes When Updating' set as Y for all violations in the input file.
     * All the violations should be deleted in this case.
     */
    @Test
    public void correctionWithDeleteOnBlankAllAsYesTest() throws IllegalArgumentException, IOException {
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, false /*recordGsiValue*/);
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                null /*tableHashToNewGsiHashValueMap*/, null /*tableHashToNewGsiRangeValueMap*/, HTV_2_VIOLATION_COUNT /*missingUpdates*/, 
                0 /*missingGsiExpectedHashValues*/, 0 /*invalidValuesForDelete*/, HTV_2_VIOLATION_COUNT /*numOfYes*/, 0 /*numOfNo*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, HTV_2_VIOLATION_COUNT /*successfulRequests*/, 
                HTV_2_VIOLATION_COUNT /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after all violations were deleted", 0, 
                result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
    }
    
    /**
     * Tests non-conditional violation correction with no updates specified but
     * 'Delete Blank Attributes When Updating' set as N for all violations in the input file.
     * No updates/deletes should happen to violations in this case.
     */
    @Test
    public void correctionWithDeleteOnBlankAllAsNoTest() throws IllegalArgumentException, IOException {
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                null /*gsiHashKeyName*/, null /*gsiHashKeyType*/,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE,
                null /*tableHashToNewGsiHashValueMap*/, null /*tableHashToNewGsiRangeValueMap*/, RTV_2_VIOLATION_COUNT /*missingUpdates*/, 
                0 /*missingGsiExpectedHashValues*/, 0 /*invalidValuesForDelete*/, 0 /*numOfYes*/, RTV_2_VIOLATION_COUNT /*numOfNo*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, 0 /*successfulRequests*/, 
                0 /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are all violations still present
        int[] result = runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE, 
                DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found changed after running correction even when no updates/deletes were specified", 
                RTV_2_VIOLATION_COUNT, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
    }
    
    /**
     * Tests non-conditional violation correction with no updates specified but
     * 'Delete Blank Attributes When Updating' set as an invalid value for all violations in the input file.
     * No updates should happen in this case and a correction output file should be generated
     * which explains the reason for each update failure.
     */
    @Test
    public void correctionWithDeleteOnBlankHavingInvalidValueTest() throws IllegalArgumentException, IOException {
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // add corrections to the file
        List<List<String>> errorRecords = createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                null /*gsiHashKeyName*/, null /*gsiHashKeyType*/,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE,
                null /*tableHashToNewGsiHashValueMap*/, null /*tableHashToNewGsiRangeValueMap*/, RTV_2_VIOLATION_COUNT /*missingUpdates*/, 
                0 /*missingGsiExpectedHashValues*/, RTV_2_VIOLATION_COUNT /*invalidValuesForDelete*/, 0 /*numOfYes*/, 0 /*numOfNo*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, 0 /*successfulRequests*/, 
                0 /*violationUpdates*/, null /*conditionalFailures*/, RTV_2_VIOLATION_COUNT /*unexpectedErrors*/);
        
        // verify correction output file
        validateCorrectionOutput(CORRECTION_OUTPUT_FILE, errorRecords);
        
        // run detector again to ensure that there are all violations still present
        int[] result = runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE, 
                DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found changed after running correction even when no updates/deletes were specified", 
                RTV_2_VIOLATION_COUNT, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
    }
    
    /**
     * Tests non-conditional violation correction with no updates specified but
     * 'Delete Blank Attributes When Updating' set as Y for some violations, N for some other
     * violations and blank for the remaining violations.
     * No updates/deletes should happen to items with the violations set as N or blank.
     * Violations with value set as Y should get deleted.
     */
    @Test
    public void correctionWithDeleteOnBlankHavingMixOfYesNoAndBlankTest() throws IllegalArgumentException, IOException {
        
        int numOfYes = 3;
        int numOfNo = 2;
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                null /*gsiHashKeyName*/, null /*gsiHashKeyType*/,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE,
                null /*tableHashToNewGsiHashValueMap*/, null /*tableHashToNewGsiRangeValueMap*/, RTV_3_VIOLATION_COUNT /*missingUpdates*/, 
                0 /*missingGsiExpectedHashValues*/, 0 /*invalidValuesForDelete*/, numOfYes, numOfNo);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, numOfYes /*successfulRequests*/, 
                numOfYes /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that required violations are still present
        int[] result = runDetector(CORRECTION_TABLE, null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE, 
                DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found changed after running correction even when no updates/deletes were specified", 
                (RTV_3_VIOLATION_COUNT - numOfYes), result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
    }
    
    /**
     * Tests non-conditional violation correction with updates specified for some violations,
     * 'Delete Blank Attributes When Updating' set as Y for some other violations and N for the
     * remaining violations.
     * No updates/deletes should happen to items with the violations set as N.
     * Violations with value set as Y should get deleted.
     * Violations with updates specified should get updated.
     */
    @Test
    public void correctionWithDeleteOnBlankAsMixOfYesNoAndUpdatesTest() throws IllegalArgumentException, IOException {
        
        int numOfYes = 2;
        int numOfNo = 3;
        int numOfUpdates = (HTV_3_VIOLATION_COUNT - numOfYes - numOfNo);
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, (numOfYes + numOfNo) /*missingUpdates*/, 
                0 /*missingGsiExpectedHashValues*/, 0 /*invalidValuesForDelete*/, numOfYes /*numOfYes*/, numOfNo /*numOfNo*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, (numOfUpdates + numOfYes) /*successfulRequests*/, 
                (numOfUpdates + numOfYes) /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are only required violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found does not match expected number.", numOfNo, 
                result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, 
                null /*gsiRangeKey*/, tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/);
    }
    
    /**
     * Tests non-conditional violation correction with updates specified for all violations and
     * 'Delete Blank Attributes When Updating' set as either Y or N for all violations.
     * If both delete as 'Y' and updates are specified, update will be done and delete ignored.
     * Similarly, if delete as 'N' and updates are specified, update will be done (and delete
     * was anyways meant to be ignored
     */
    @Test
    public void correctionWithDeleteOnBlankAsAllYesOrNoAndWithUpdatesTest() throws IllegalArgumentException, IOException {
        
        int numOfYes = 3;
        int numOfNo = HSV_3_VIOLATION_COUNT - numOfYes;
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, 0 /*missingUpdates*/, 
                0 /*missingGsiExpectedHashValues*/, 0 /*invalidValuesForDelete*/, numOfYes /*numOfYes*/, numOfNo /*numOfNo*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, HSV_3_VIOLATION_COUNT /*successfulRequests*/, 
                HSV_3_VIOLATION_COUNT /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found does not match expected number.", 0, 
                result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3, 
                null /*gsiRangeKey*/, tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/);
    }
    
    /**
     * Tests for violation correction in delete mode.
     * This mode deletes all violations specified in the input file irrespective of the updates, if any.
     */
    @Test
    public void correctionInDeleteModeTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, false /*recordGsiValue*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE,
                false /*useConditionalUpdate*/, true /*deleteMode*/, 0 /*successfulRequests*/, 
                RSV_3_VIOLATION_COUNT /*violationUpdates*/, null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] detResult = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found does not match expected number.", 0, 
                detResult[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
    }
    
    /**
     * Tests violation correction with conditional updates specified for 
     * Gsi Hash Key having type violations with expected Type as Number.
     * For conditional failures, output file should be generated with a column 
     * explaining the reason for failures.
     */
    @Test
    public void correctionWithConditionalUpdatesForHashWithTypeNumberTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.NUMBER_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, 
                true /*recordDetails*/, true /*recordGsiValue*/);
        
        int numOfHashConditionalFailures = 2;
        // update some values so that conditional update fails
        Set<String> hashKeysUpdated = updateValues(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, 
                CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.NUMBER_TYPE, null /*rangeKey Name*/, null /*rangeKey type*/, 
                hvcnViolations, null /*range key violations*/, numOfHashConditionalFailures, 
                0 /*num of range conditional failures*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        List<List<String>> allRecords = createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.NUMBER_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.NUMBER_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE, true /*useConditionalUpdate*/, 
                false /*deleteMode*/, (HVCN_VIOLATION_COUNT - numOfHashConditionalFailures) /*successfulRequests*/, 
                HVCN_VIOLATION_COUNT /*violationUpdates*/, numOfHashConditionalFailures /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // get all records which failed because of conditional update failure
        List<List<String>> conditionalFailureRecords = filterRecords(allRecords, hashKeysUpdated);
        
        // verify correction output
        validateCorrectionOutput(CORRECTION_OUTPUT_FILE, conditionalFailureRecords);
        
        // run detector again to ensure that there are only expected number of violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.NUMBER_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations count does not match", numOfHashConditionalFailures, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, 
                null /*gsiRangeKeyName*/, tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, hashKeysUpdated);
    }
    
    /**
     * Tests violation correction with conditional updates specified for 
     * Gsi Hash Key having type violations with expected Type as String.
     * For conditional failures, output file should be generated with a column 
     * explaining the reason for failures.
     */
    @Test
    public void correctionWithConditionalUpdatesForHashWithTypeStringTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.STRING_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, true /*recordGsiValue*/);
        
        int numOfHashConditionalFailures = 2;
        int numOfRangeConditionalFailures = 1;
        // update some values so that conditional update fails
        Set<String> hashKeysUpdated = updateValues(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.STRING_TYPE, 
                hvcsViolations, rvcsViolations, numOfHashConditionalFailures, numOfRangeConditionalFailures);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        List<List<String>> allRecords = createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.STRING_TYPE,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.STRING_TYPE, 
                CORRECTION_INPUT_FILE, true /*useConditionalUpdate*/, false /*deleteMode*/, 
                (HVCS_VIOLATION_COUNT + RVCS_VIOLATION_COUNT - numOfHashConditionalFailures - numOfRangeConditionalFailures) /*successfulRequests*/, 
                (HVCS_VIOLATION_COUNT + RVCS_VIOLATION_COUNT) /*violationUpdates*/,
                numOfHashConditionalFailures + numOfRangeConditionalFailures /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // get all records which failed because of conditional update failure
        List<List<String>> conditionalFailureRecords = filterRecords(allRecords, hashKeysUpdated);
        
        // verify correction output
        validateCorrectionOutput(CORRECTION_OUTPUT_FILE, conditionalFailureRecords);
        
        // run detector again to ensure that there are only expected number of violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.STRING_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations count does not match", 
                (numOfHashConditionalFailures + numOfRangeConditionalFailures), result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, hashKeysUpdated);
    }
    
    /**
     * Tests violation correction with conditional updates specified for 
     * Gsi Hash Key having type violations with expected Type as Binary.
     * For conditional failures, output file should be generated with a column 
     * explaining the reason for failures.
     */
    @Test
    public void correctionWithConditionalUpdatesForHashWithTypeBinaryTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, true /*recordGsiValue*/);
        
        int numOfHashConditionalFailures = 3;
        int numOfRangeConditionalFailures = 2;
        // update some values so that conditional update fails
        Set<String> hashKeysUpdated = updateValues(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                hvcbViolations, rvcbViolations, numOfHashConditionalFailures, numOfRangeConditionalFailures);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        List<List<String>> allRecords = createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                CORRECTION_INPUT_FILE, true /*useConditionalUpdate*/, false /*deleteMode*/, 
                (HVCB_VIOLATION_COUNT + RVCB_VIOLATION_COUNT - numOfHashConditionalFailures - numOfRangeConditionalFailures) /*successfulRequests*/, 
                (HVCB_VIOLATION_COUNT + RVCB_VIOLATION_COUNT) /*violationUpdates*/, 
                numOfHashConditionalFailures + numOfRangeConditionalFailures /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // get all records which failed because of conditional update failure
        List<List<String>> conditionalFailureRecords = filterRecords(allRecords, hashKeysUpdated);
        
        // verify correction output
        validateCorrectionOutput(CORRECTION_OUTPUT_FILE, conditionalFailureRecords);
        
        // run detector again to ensure that there are only expected number of violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, DETECTION_OP_FILE, 
                false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations count does not match", 
                (numOfHashConditionalFailures + numOfRangeConditionalFailures), result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, hashKeysUpdated);
    }
    
    /**
     * Run violation correction with conditional updates but no column for 
     * GSI value present in the correction input file.
     * The tool should display an error and exit.
     */
    @Test
    public void correctionWithConditionalUpdatesButNoGSIValueInCorrectionInputTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, DETECTION_OP_FILE, 
                true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        Map<String, String> tableHashToNewGsiRangeValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE,
                CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, 0 /*missingUpdates*/);
        
        try {
            // run violation correction with conditional update as true and it should complain about missing GSI values
            runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                    CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE, 
                    CORRECTION_INPUT_FILE, true /*useConditionalUpdate*/, false /*deleteMode*/, 
                    null /*successfulRequests*/, null /*violationUpdates*/,
                    null /*conditionalFailures*/, null /*unexpectedErrors*/);
            Assert.fail("System.exit(1) should have been called");
        } catch(ExitException e) {
            Assert.assertEquals("Exit status", 1, e.status);
        }
    }
    
    /**
     * Test correction with conditional updates specified but with some violations missing the 
     * expected gsi value. 
     * Such updates should be reported as failures in the violation output file.
     */
    @Test
    public void correctionWithConditionalUpdateButMissingExpectedValuesTest() throws IllegalArgumentException, IOException {
        
        int numOfGsiHashValuesMissing = 3;
        
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, true /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        List<List<String>> allRecords = createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, 0 /*missingUpdates*/, 
                numOfGsiHashValuesMissing);
        // errors will be in top records, so fetch just those
        List<List<String>> errorRecords = allRecords.subList(0, numOfGsiHashValuesMissing);
        
        // Get hash keys which were not updated
        Set<String> hashKeysNotUpdated = new HashSet<String>();
        for(List<String> errRecord : errorRecords) {
            hashKeysNotUpdated.add(errRecord.get(0));
        }
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE,
                true /*useConditionalUpdate*/, false /*deleteMode*/, (HSV_4_VIOLATION_COUNT - numOfGsiHashValuesMissing) /*successfulRequests*/, 
                HSV_4_VIOLATION_COUNT /*violationUpdates*/, 
                null /*conditionalFailures*/, numOfGsiHashValuesMissing /*unexpectedErrors*/);
        
        // verify correction output
        validateCorrectionOutput(CORRECTION_OUTPUT_FILE, errorRecords);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found does not match expected number.", numOfGsiHashValuesMissing, 
                result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4, 
                null /*gsiRangeKey*/, tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, hashKeysNotUpdated);
    }
    
    /**
     * Run violation correction with rate limiting turned on.
     */
    @Test
    public void correctionWithRateLimiterTest() throws IllegalArgumentException, IOException {
        // generate file containing violations
        runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, true /*recordDetails*/, false /*recordGsiValue*/);
        
        // map of updated gsi values
        Map<String, String> tableHashToNewGsiHashValueMap = new HashMap<String, String>();
        
        // add corrections to the file
        createCorrectionFile(DETECTION_OP_FILE /*inputFile*/, CORRECTION_INPUT_FILE /*outputFile*/, 
                CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/,
                tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/, 0 /*missingUpdates*/);
        
        // run violation correction
        runCorrection(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, CORRECTION_INPUT_FILE,
                false /*useConditionalUpdate*/, false /*deleteMode*/, 10 /*readWriteIopsPercentage*/, HTV_5_VIOLATION_COUNT /*successfulRequests*/, 
                HTV_5_VIOLATION_COUNT /*violationUpdates*/,
                null /*conditionalFailures*/, 0 /*unexpectedErrors*/);
        
        // run detector again to ensure that there are no violations
        int[] result = runDetector(CORRECTION_TABLE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, DETECTION_OP_FILE, false /*recordDetails*/, false /*recordGsiValue*/);
        Assert.assertEquals("Violations found even after correction has run", 0, result[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        
        // ensure that the values in the table were updated correctly
        verifyUpdatedTableRecords(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, 
                null /*gsiRangeKeyName*/, tableHashToNewGsiHashValueMap, null /*tableHashToNewGsiRangeValueMap*/);
    }

    /**
     * Returns a list of records picked up from input list allRecords 
     * having keys specified in the input set of filterKeys.
     */
    private List<List<String>> filterRecords(List<List<String>> allRecords, Set<String> filterKeys) {
        List<List<String>> recordsToReturn = new ArrayList<List<String>>();
        for(List<String> record : allRecords) {
            // First column is always table hash-key
            String hashKey = record.get(0);
            if(filterKeys.contains(hashKey)) {
                recordsToReturn.add(record);
            }
        }
        return recordsToReturn;
    }

    /**
     * This method assumes that it is a hash-key only table.
     * It makes the specified number of updates to gsi hash key and range key from the 
     * given list of violations for each key.
     * It ensures that type violations still remain violations.
     * Not used for size violations.
     */
    private Set<String> updateValues(String tableName,
            String tableHashKeyName, String tableHashKeyType,
            String gsiHashKeyName, String gsiHashKeyType,
            String gsiRangeKeyName, String gsiRangeKeyType,
            List<Map<String, AttributeValue>> gsiHashKeyViolations,
            List<Map<String, AttributeValue>> gsiRangeKeyViolations,
            int numOfgsiHashUpdates, int numOfgsiRangeUpdates) {
        Set<String> updatedTableHashKeys = new HashSet<String>();
        
        if(numOfgsiHashUpdates > 0) {
            for(int i = 0 ; i < numOfgsiHashUpdates ; i++) {
                Map<String, AttributeValue> item = gsiHashKeyViolations.get(i);
                // change value of gsiHashKey and add to the table - make sure it is still a violation
                item.put(gsiHashKeyName, tableManager.generateItem(gsiHashKeyName, TestUtils.returnDifferentAttributeType(gsiHashKeyType), 7));
                tableManager.putItem(tableName, item);
                
                String hashKey = AttributeValueConverter.toBlankString(item.get(tableHashKeyName));
                updatedTableHashKeys.add(hashKey);
            }
        }
        
        if(numOfgsiRangeUpdates > 0) {
            for(int i = 0 ; i < numOfgsiRangeUpdates ; i++) {
                Map<String, AttributeValue> item = gsiRangeKeyViolations.get(i);
                // change value of gsiRangeKey and add to the table - make sure it is still a violation
                item.put(gsiRangeKeyName, tableManager.generateItem(gsiRangeKeyName, TestUtils.returnDifferentAttributeType(gsiRangeKeyType), 6));
                tableManager.putItem(tableName, item);
                
                String hashKey = AttributeValueConverter.toBlankString(item.get(tableHashKeyName));
                updatedTableHashKeys.add(hashKey);
            }
        }
        return updatedTableHashKeys;
    }
    
    /**
     * Validates the output of violation correction.
     */
    private void validateCorrectionOutput(String correctionOutputFile,
            List<List<String>> errorRecords) throws IOException {
        BufferedReader br = null;
        CSVParser parser = null;
        try {
            br = new BufferedReader(new FileReader(new File(correctionOutputFile)));
            parser = new CSVParser(br, TestUtils.csvFormat);
            List<CSVRecord> records = parser.getRecords();
            
            Assert.assertEquals("Error record count does not match", errorRecords.size(), records.size());
            for(CSVRecord record : records) {
                boolean foundError = false;
                List<String> readRecord = new ArrayList<String>();
                for(int i = 0 ; i < record.size() ; i++) {
                    if(record.get(i).equals(record.get(ViolationRecord.GSI_VALUE_UPDATE_ERROR))) {
                        foundError = true;
                        continue;
                    } else {
                        readRecord.add(record.get(i));
                    }
                }
                Assert.assertTrue("Error column not found", foundError);
                Assert.assertTrue("Unexpected record read from correction output", errorRecords.contains(readRecord));
                errorRecords.remove(readRecord);
            }
        } finally {
            br.close();
            parser.close();
        }
    }
    
    /**
     * verifies that the updated values through violations correction actually reflects in the table.
     */
    private void verifyUpdatedTableRecords(String tableName, 
            String tableHashKeyName, String tableHashKeyType,
            String gsiHashKeyName, String gsiRangeKeyName,
            Map<String, String> tableHashToNewGsiHashValueMap,
            Map<String, String> tableHashToNewGsiRangeValueMap) {
        verifyUpdatedTableRecords(tableName, tableHashKeyName, tableHashKeyType, gsiHashKeyName, gsiRangeKeyName,
                tableHashToNewGsiHashValueMap, tableHashToNewGsiRangeValueMap, null/*hashKeysUpdated*/);
    }

    /**
     * verifies that the updated values through violations correction actually reflects in the table.
     * If 'hashKeysUpdated' is not null, checks for only the specified hash keys in the set. Otherwise, checks for all
     */
    private void verifyUpdatedTableRecords(String tableName, 
            String tableHashKeyName, String tableHashKeyType,
            String gsiHashKeyName, String gsiRangeKeyName,
            Map<String, String> tableHashToNewGsiHashValueMap,
            Map<String, String> tableHashToNewGsiRangeValueMap,
            Set<String> hashKeysUpdated) {
        
        if(tableHashToNewGsiHashValueMap != null) {
            for(String hashVal : tableHashToNewGsiHashValueMap.keySet()) {
                if(hashKeysUpdated == null || (!hashKeysUpdated.contains(hashVal))) {
                    Map<String, AttributeValue> item = tableManager.getItem(tableName, tableHashKeyName, 
                            AttributeValueConverter.parseFromBlankString(tableHashKeyType, hashVal), 
                            null /*rangeKeyName*/, null /*rangeKeyValue*/);
                    
                    String updatedGsiHash = AttributeValueConverter.toBlankString(item.get(gsiHashKeyName));
                    Assert.assertEquals("Updated gsi hash value does not match", tableHashToNewGsiHashValueMap.get(hashVal), updatedGsiHash);
                }
            }
        }
        
        if(tableHashToNewGsiRangeValueMap != null) {
            for(String hashVal : tableHashToNewGsiRangeValueMap.keySet()) {
                if(hashKeysUpdated == null || (!hashKeysUpdated.contains(hashVal))) {
                    Map<String, AttributeValue> item = tableManager.getItem(tableName, tableHashKeyName, 
                            AttributeValueConverter.parseFromBlankString(tableHashKeyType, hashVal), 
                            null /*rangeKeyName*/, null /*rangeKeyValue*/);
                    
                    String updatedGsiRange = AttributeValueConverter.toBlankString(item.get(gsiRangeKeyName));
                    Assert.assertEquals("Updated gsi range value does not match", tableHashToNewGsiRangeValueMap.get(hashVal), updatedGsiRange);
                }
            }
        }
    }
    
    /**
     * Iterates through detection output file: first leave updates blank based on missing updates per key. 
     * Once blank number is reached, it adds updates for the remaining keys.
     */
    private static List<List<String>> createCorrectionFile(final String detectionFile, final String correctionFile,
            final String gsiHashKeyName, final String gsiHashKeyType, 
            final String gsiRangeKeyName, final String gsiRangeKeyType,
            final Map<String, String> tableHashToNewGsiHashValueMap,
            final Map<String, String> tableHashToNewGsiRangeValueMap,
            final int missingUpdatesPerKey) throws IOException {
        
        return createCorrectionFile(detectionFile, correctionFile, gsiHashKeyName, gsiHashKeyType, 
                gsiRangeKeyName, gsiRangeKeyType, tableHashToNewGsiHashValueMap,
                tableHashToNewGsiRangeValueMap, missingUpdatesPerKey, 0 /*missingGsiExpectedHashValues*/, 
                0 /*invalidValuesForDelete*/, 0 /*numOfYesForDelete*/, 0 /*numOfNoForDelete*/);
    }
    
    /**
     * Iterates through detection output file: first leave updates blank based on missing updates per key. 
     * Once it has reached the missing update number, it removes the expected gsi values as per the specified 'missingGsiExpectedHashValues'.
     * Note that once blank number is reached, it also starts adding updates. 
     */
    private static List<List<String>> createCorrectionFile(final String detectionFile, final String correctionFile,
            final String gsiHashKeyName, final String gsiHashKeyType, 
            final String gsiRangeKeyName, final String gsiRangeKeyType,
            final Map<String, String> tableHashToNewGsiHashValueMap,
            final Map<String, String> tableHashToNewGsiRangeValueMap,
            final int missingUpdatesPerKey, final int missingGsiExpectedHashValues) throws IOException {
        
        return createCorrectionFile(detectionFile, correctionFile, gsiHashKeyName, gsiHashKeyType, 
                gsiRangeKeyName, gsiRangeKeyType, tableHashToNewGsiHashValueMap,
                tableHashToNewGsiRangeValueMap, missingUpdatesPerKey, missingGsiExpectedHashValues, 
                0 /*invalidValuesForDelete*/, 0 /*numOfYesForDelete*/, 0 /*numOfNoForDelete*/);
    }

    /**
     * Iterates through detection output file: first leave updates blank based on missing updates per key. 
     * Once it has reached the missing update number, it removes the expected gsi values as per the specified 'missingGsiExpectedHashValues'.
     * Note that once blank number is reached, it also starts adding updates. 
     * It then iterates over the rows again and adds values for Yes/No/Invalid in the delete column.
     * It returns all error records, if present. If not, it returns all records.
     */
    private static List<List<String>> createCorrectionFile(final String detectionFile, final String correctionFile,
            final String gsiHashKeyName, final String gsiHashKeyType, 
            final String gsiRangeKeyName, final String gsiRangeKeyType,
            final Map<String, String> tableHashToNewGsiHashValueMap,
            final Map<String, String> tableHashToNewGsiRangeValueMap,
            final int missingUpdatesPerKey, final int missingGsiExpectedHashValues,
            final int invalidValuesForDelete, final int numOfYesForDelete, 
            final int numOfNoForDelete) throws IOException {
        
        List<List<String>> errorRecords = null;
        List<List<String>> allRecords = null;
        
        BufferedReader br = null;
        BufferedWriter bw = null;
        CSVParser parser = null;
        CSVPrinter csvPrinter = null;
        try {
            br = new BufferedReader(new FileReader(new File(detectionFile)));
            bw = new BufferedWriter(new FileWriter(new File(correctionFile)));
            parser = new CSVParser(br, TestUtils.csvFormat);
            csvPrinter = new CSVPrinter(bw, TestUtils.csvFormat);
            List<CSVRecord> detectorRecords = parser.getRecords();
            
            int hashMissingUpdates = 0;
            int rangeMissingUpdates = 0;
            int missingGsiExpectedHashValuesCurrent = 0;
            
            // Print Header
            Map<String, Integer> header = parser.getHeaderMap();
            csvPrinter.printRecord(header.keySet());
            
            allRecords = new ArrayList<List<String>>();
            for(CSVRecord csvRecord : detectorRecords) {
                List<String> newRecord = new ArrayList<String>();
                String tableHashKeyRecorded = csvRecord.get(ViolationRecord.TABLE_HASH_KEY);
                
                String hashKeyViolationType = null;
                if(gsiHashKeyName != null) {
                    hashKeyViolationType = csvRecord.get(ViolationRecord.GSI_HASH_KEY_VIOLATION_TYPE);
                }
                String rangeKeyViolationType = null;
                if(gsiRangeKeyName != null) {
                    rangeKeyViolationType = csvRecord.get(ViolationRecord.GSI_RANGE_KEY_VIOLATION_TYPE);
                }
                
                for(int i = 0 ; i < csvRecord.size() ; i++) {
                    newRecord.add(i, csvRecord.get(i));
                }
                
                String newGsiVal = null;
                if(hashKeyViolationType != null && (hashKeyViolationType.equals("Size Violation") || hashKeyViolationType.equals("Type Violation"))) {
                    if(hashMissingUpdates < missingUpdatesPerKey) {
                        allRecords.add(newRecord);
                        hashMissingUpdates++;
                        continue;
                    }
                    //Remove expected hash Values
                    if(missingGsiExpectedHashValuesCurrent < missingGsiExpectedHashValues) {
                        newRecord.remove((int)header.get(ViolationRecord.GSI_HASH_KEY));
                        newRecord.add(header.get(ViolationRecord.GSI_HASH_KEY), "");
                        missingGsiExpectedHashValuesCurrent++;
                    }
                    
                    newRecord.remove((int)header.get(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE));
                    newGsiVal = getNewValue(gsiHashKeyType, 4 /*length*/);
                    newRecord.add(header.get(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE), newGsiVal);
                    tableHashToNewGsiHashValueMap.put(tableHashKeyRecorded, newGsiVal);
                }
                
                if(rangeKeyViolationType != null && (rangeKeyViolationType.equals("Size Violation") || rangeKeyViolationType.equals("Type Violation"))) {
                    if(rangeMissingUpdates < missingUpdatesPerKey) {
                        allRecords.add(newRecord);
                        rangeMissingUpdates++;
                        continue;
                    }
                    
                    newRecord.remove(header.get(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE));
                    newGsiVal = getNewValue(gsiRangeKeyType, 4 /*length*/);
                    newRecord.add(header.get(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE), newGsiVal);
                    tableHashToNewGsiRangeValueMap.put(tableHashKeyRecorded, newGsiVal);
                }
                allRecords.add(newRecord);
            }
            
            // Add 'Y' or 'N' for delete column
            if(numOfNoForDelete > 0 || numOfYesForDelete > 0 || invalidValuesForDelete > 0) {
                errorRecords = new ArrayList<List<String>>();
                int numOfYesAdded = 0;
                int numOfNoAdded = 0;
                int numOfInvalids = 0;
                for(List<String> record : allRecords) {
                    if(numOfInvalids < invalidValuesForDelete) {
                        record.remove(header.get(ViolationRecord.GSI_CORRECTION_DELETE_BLANK));
                        record.add(header.get(ViolationRecord.GSI_CORRECTION_DELETE_BLANK), "xx");
                        numOfInvalids++;
                        errorRecords.add(record);
                        continue;
                    }
                    
                    if(numOfYesAdded < numOfYesForDelete) {
                        record.remove(header.get(ViolationRecord.GSI_CORRECTION_DELETE_BLANK));
                        record.add(header.get(ViolationRecord.GSI_CORRECTION_DELETE_BLANK), "Y");
                        numOfYesAdded++;
                        continue;
                    }
                    
                    if(numOfNoAdded < numOfNoForDelete) {
                        record.remove(header.get(ViolationRecord.GSI_CORRECTION_DELETE_BLANK));
                        record.add(header.get(ViolationRecord.GSI_CORRECTION_DELETE_BLANK), "N");
                        numOfNoAdded++;
                        continue;
                    }
                }
            }
            
            // Add all records to file
            csvPrinter.printRecords(allRecords);
        } finally {
            br.close();
            bw.close();
            parser.close();
            csvPrinter.close();
        }
        
        if(errorRecords != null)
            return errorRecords;
        else 
            return allRecords;
    }
    
    /**
     * Generates a random string for the specified length and type.
     * (Binary is also returned as String.)
     */
    private static String getNewValue(String type, int length) {
        if(type.equals(TestUtils.NUMBER_TYPE)) {
            return "" + tableManager.randDataGenerator.nextRandomInt();
        } else {
            return tableManager.randDataGenerator.nextRadomString(length);
        }
    }
    
    /**
     * Runs violation correction with the given inputs.
     * Also validates the results if the expected values are not null.
     * Returns the correction result numbers.
     */
    private int[] runCorrection(String tableName, String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, String gsiRangeKeyType, 
            String inputFile, boolean useConditionalUpdate, boolean deleteMode, 
            Integer successfulRequests, Integer violationUpdates,
            Integer conditionalFailures, Integer unexpectedErrors) throws IllegalArgumentException, IOException {
        return runCorrection(tableName, gsiHashKeyName, gsiHashKeyType, gsiRangeKeyName, 
                gsiRangeKeyType, inputFile, useConditionalUpdate, deleteMode, 100 /*readWriteIOPSPercent*/, 
                successfulRequests, violationUpdates, conditionalFailures, unexpectedErrors);
    }
    
    /**
     * Runs violation correction with the given inputs.
     * Also validates the results if the expected values are not null.
     * Returns the correction result numbers.
     */
    private int[] runCorrection(String tableName, String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, String gsiRangeKeyType, 
            String inputFile, boolean useConditionalUpdate, boolean deleteMode, int readWriteIOPSPercent, 
            Integer successfulRequests, Integer violationUpdates,
            Integer conditionalFailures, Integer unexpectedErrors) throws IllegalArgumentException, IOException {
        Options options = getOptions();
        options.setGsiHashKeyName(gsiHashKeyName);
        options.setGsiHashKeyType(gsiHashKeyType);
        options.setGsiRangeKeyName(gsiRangeKeyName);
        options.setGsiRangeKeyType(gsiRangeKeyType);
        options.setTableName(tableName);
        options.setCorrectionInputPath(inputFile);
        options.setReadWriteIOPSPercentage(readWriteIOPSPercent);
        ViolationDetector violationDetector = getViolationDetector(options, tableName);
        violationDetector.violationCorrection(deleteMode, useConditionalUpdate);
        int[] results = violationDetector.getViolationCorrectionOutput();
        validateResults(results, successfulRequests, violationUpdates, conditionalFailures, unexpectedErrors);
        return results;
    }

    private void validateResults(int[] results, Integer successfulRequests,
            Integer violationUpdates, Integer conditionalFailures, Integer errors) {
        
        if(successfulRequests != null) {
            Assert.assertEquals("Correction: successful requests do not match", 
                    (int)successfulRequests, results[TestUtils.CORRECTION_SUCCESSFUL_UPDATES_INDEX]);
        }
        
        if(violationUpdates != null) {
            Assert.assertEquals("Correction: update requests do not match", 
                    (int)violationUpdates, results[TestUtils.CORRECTION_UPDATE_REQUESTS_INDEX]);
        }
        
        if(conditionalFailures != null) {
            Assert.assertEquals("Correction: conditional failures do not match", 
                    (int)conditionalFailures, results[TestUtils.CORRECTION_CONDITIONAL_UPDATE_FAILURES_INDEX]);
        }
        
        if(errors != null) {
            Assert.assertEquals("Correction: unexpected errors do not match", 
                    (int)errors, results[TestUtils.CORRECTION_UNEXPECTED_ERRORS_INDEX]);
        }
    }

    private int[] runDetector(String tableName, String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, String gsiRangeKeyType, 
            String outputFile, boolean recordDetails, boolean recordGsiValue) throws IllegalArgumentException, IOException {
        Options options = getOptions();
        options.setGsiHashKeyName(gsiHashKeyName);
        options.setGsiHashKeyType(gsiHashKeyType);
        options.setGsiRangeKeyName(gsiRangeKeyName);
        options.setGsiRangeKeyType(gsiRangeKeyType);
        options.setTableName(tableName);
        options.setDetectionOutputPath(outputFile);
        options.setRecordDetails(recordDetails);
        options.setRecordGsiValueInViolationRecord(recordGsiValue);
        ViolationDetector violationDetector = getViolationDetector(options, tableName);
        violationDetector.violationDetection(false /*deleteViolations*/);
        
        return violationDetector.getViolationDectectionOutput();
    }
    
    private ViolationDetector getViolationDetector(Options options, String detectorTable) throws IllegalArgumentException, IOException {
        TableHelper tableHelper = new TableHelper(dynamoDBClient, detectorTable);
        TableReader tableReader = new TableReader(options, dynamoDBClient, tableHelper, TestUtils.RUN_TESTS_ON_DYNAMODB_LOCAL);
        Correction correction = new Correction(options, tableHelper, dynamoDBClient, TestUtils.RUN_TESTS_ON_DYNAMODB_LOCAL);
        return new ViolationDetector(options, null /*optionsLoader*/, awsConnection, tableHelper, tableReader, correction, TestUtils.RUN_TESTS_ON_DYNAMODB_LOCAL);
    }
    
    private Options getOptions() {
        Options options = Options.getInstance();
        options.setCorrectionInputPath("./gsi_violation_check.csv");
        options.setDetectionOutputPath("./gsi_violation_check.csv");
        options.setCredentialFilePath("./config/credentials");
        options.setCorrectionOutputPath(CORRECTION_OUTPUT_FILE);
        options.setDynamoDBRegion(DYNAMODB_REGION);
        options.setNumOfRecords(-1);
        options.setNumOfSegments(1);
        options.setNumOfViolations(-1);
        options.setReadWriteIOPSPercentage(100);
        options.setRecordDetails(false);
        options.setRecordGsiValueInViolationRecord(false);
        return options;
    }
    
    private static void generateTableAndDataForCorrectorTable() {
        boolean tableCreated = tableManager.createNewTable(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, 
                null, null, READ_CAPACITY, WRITE_CAPACITY);
        Assert.assertTrue("Table creation failed!", tableCreated);
        tablesToDelete.add(CORRECTION_TABLE);
        
        // load values for HSV
        Map<String, String> hsvAttributes = new HashMap<String, String>();
        hsvAttributes.put(CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSV_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsvAttributes, 6, HSV_NON_VIOLATION_COUNT);

        // load values for RSV
        Map<String, String> rsvAttributes = new HashMap<String, String>();
        rsvAttributes.put(CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSV_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsvAttributes, 6, RSV_NON_VIOLATION_COUNT);
        
        // load values for HSV2
        Map<String, String> hsv2Attributes = new HashMap<String, String>();
        hsv2Attributes.put(CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_2_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsv2Attributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSV_2_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsv2Attributes, 6, HSV_2_NON_VIOLATION_COUNT);

        // load values for RSV2
        Map<String, String> rsv2Attributes = new HashMap<String, String>();
        rsv2Attributes.put(CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_2_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsv2Attributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSV_2_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsv2Attributes, 6, RSV_2_NON_VIOLATION_COUNT);
        
        // load values for HSV3
        Map<String, String> hsv3Attributes = new HashMap<String, String>();
        hsv3Attributes.put(CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_3_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsv3Attributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSV_3_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsv3Attributes, 4, HSV_3_NON_VIOLATION_COUNT);

        // load values for RSV3
        Map<String, String> rsv3Attributes = new HashMap<String, String>();
        rsv3Attributes.put(CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_3_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsv3Attributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSV_3_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsv3Attributes, 5, RSV_3_NON_VIOLATION_COUNT);
        
        // load values for HSV4
        Map<String, String> hsv4Attributes = new HashMap<String, String>();
        hsv4Attributes.put(CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4, CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_4_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsv4Attributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSV_4_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hsv4Attributes, 4, HSV_4_NON_VIOLATION_COUNT);

        // load values for RSV4
        Map<String, String> rsv4Attributes = new HashMap<String, String>();
        rsv4Attributes.put(CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_4, CORRECTION_TABLE_RK_WITH_SIZE_VIOLATION_4_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsv4Attributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSV_4_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rsv4Attributes, 7, RSV_4_NON_VIOLATION_COUNT);
        
        // load values for HTV
        Map<String, String> htvAttributes = new HashMap<String, String>();
        htvAttributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htvAttributes, 5, HTV_VIOLATION_COUNT);
        htvAttributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htvAttributes, 5, HTV_NON_VIOLATION_COUNT);

        // load values for RTV
        Map<String, String> rtvAttributes = new HashMap<String, String>();
        rtvAttributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtvAttributes, 5, RTV_VIOLATION_COUNT);
        rtvAttributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtvAttributes, 5, RTV_NON_VIOLATION_COUNT);
        
        // load values for HTV2
        Map<String, String> htv2Attributes = new HashMap<String, String>();
        htv2Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv2Attributes, 5, HTV_2_VIOLATION_COUNT);
        htv2Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_2_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv2Attributes, 5, HTV_2_NON_VIOLATION_COUNT);

        // load values for RTV2
        Map<String, String> rtv2Attributes = new HashMap<String, String>();
        rtv2Attributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtv2Attributes, 5, RTV_2_VIOLATION_COUNT);
        rtv2Attributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_2_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtv2Attributes, 5, RTV_2_NON_VIOLATION_COUNT);
        
        // load values for HTV3
        Map<String, String> htv3Attributes = new HashMap<String, String>();
        htv3Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv3Attributes, 5, HTV_3_VIOLATION_COUNT);
        htv3Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_3_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv3Attributes, 5, HTV_3_NON_VIOLATION_COUNT);

        // load values for RTV3
        Map<String, String> rtv3Attributes = new HashMap<String, String>();
        rtv3Attributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtv3Attributes, 5, RTV_3_VIOLATION_COUNT);
        rtv3Attributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_3_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtv3Attributes, 5, RTV_3_NON_VIOLATION_COUNT);
        
        // load values for HTV4
        Map<String, String> htv4Attributes = new HashMap<String, String>();
        htv4Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_4, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_4_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv4Attributes, 5, HTV_4_VIOLATION_COUNT);
        htv4Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_4, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_4_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv4Attributes, 5, HTV_4_NON_VIOLATION_COUNT);

        // load values for RTV4
        Map<String, String> rtv4Attributes = new HashMap<String, String>();
        rtv4Attributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_4, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_4_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtv4Attributes, 5, RTV_4_VIOLATION_COUNT);
        rtv4Attributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_4, CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_4_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rtv4Attributes, 5, RTV_4_NON_VIOLATION_COUNT);
        
        // load values for HSTV
            // type violations
        Map<String, String> hstvAttributes = new HashMap<String, String>();
        hstvAttributes.put(CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hstvAttributes, 4, HSTV_VIOLATION_COUNT_TYPE);
            // size violations
        hstvAttributes.put(CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hstvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSTV_VIOLATION_COUNT_SIZE);
            // no violations
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hstvAttributes, 5, HSTV_NON_VIOLATION_COUNT);
    
        // load values for RSTV
            // type violations
        Map<String, String> rstvAttributes = new HashMap<String, String>();
        rstvAttributes.put(CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rstvAttributes, 4, RSTV_VIOLATION_COUNT_TYPE);
            // size violations
        rstvAttributes.put(CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, CORRECTION_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rstvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSTV_VIOLATION_COUNT_SIZE);
            // no violations
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rstvAttributes, 5, RSTV_NON_VIOLATION_COUNT);
        
        // load values for HVCS
        Map<String, String> hvcsAttributes = new HashMap<String, String>();
        hvcsAttributes.put(CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE);
        hvcsViolations = tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hvcsAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HVCS_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hvcsAttributes, 6, HVCS_NON_VIOLATION_COUNT);
        
        // load values for HVCB
        Map<String, String> hvcbAttributes = new HashMap<String, String>();
        hvcbAttributes.put(CORRECTION_TABLE_HK_WITH_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE);
        hvcbViolations = tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hvcbAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HVCB_VIOLATION_COUNT);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hvcbAttributes, 6, HVCB_NON_VIOLATION_COUNT);
        
        // load values for HVCN
        Map<String, String> hvcnAttributes = new HashMap<String, String>();
        hvcnAttributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.returnDifferentAttributeType(TestUtils.NUMBER_TYPE));
        hvcnViolations = tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hvcnAttributes, 5, HVCN_VIOLATION_COUNT);
        hvcnAttributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_NUMBER, TestUtils.NUMBER_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, hvcnAttributes, 5, HVCN_NON_VIOLATION_COUNT);
        
        // load values for RVCS
        Map<String, String> rvcsAttributes = new HashMap<String, String>();
        rvcsAttributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.returnDifferentAttributeType(TestUtils.STRING_TYPE));
        rvcsViolations = tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rvcsAttributes, 5, RVCS_VIOLATION_COUNT);
        rvcsAttributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_STRING, TestUtils.STRING_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rvcsAttributes, 5, RVCS_NON_VIOLATION_COUNT);
        
        // load values for RVCB
        Map<String, String> rvcbAttributes = new HashMap<String, String>();
        rvcbAttributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.returnDifferentAttributeType(TestUtils.BINARY_TYPE));
        rvcbViolations = tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rvcbAttributes, 5, RVCB_VIOLATION_COUNT);
        rvcbAttributes.put(CORRECTION_TABLE_RK_WITH_TYPE_VIOLATION_BINARY, TestUtils.BINARY_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, rvcbAttributes, 5, RVCB_NON_VIOLATION_COUNT);
        
        // load values for HTV5
        Map<String, String> htv5Attributes = new HashMap<String, String>();
        htv5Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, TestUtils.returnDifferentAttributeType(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE));
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv5Attributes, 5, HTV_5_VIOLATION_COUNT);
        htv5Attributes.put(CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5, CORRECTION_TABLE_HK_WITH_TYPE_VIOLATION_5_TYPE);
        tableManager.loadRandomData(CORRECTION_TABLE, CORRECTION_TABLE_HK, CORRECTION_TABLE_HK_TYPE, null, 
                null, htv5Attributes, 5, HTV_5_NON_VIOLATION_COUNT);
    }
}
