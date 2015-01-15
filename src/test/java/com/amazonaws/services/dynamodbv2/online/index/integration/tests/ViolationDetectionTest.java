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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVParser;
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
 * Functional tests for violation detection.
 *
 */
public class ViolationDetectionTest {

    private static TableManager tableManager;
    private static AWSConnection awsConnection;
    private static AmazonDynamoDBClient dynamoDBClient;
    
    private static final Set<String> tablesToDelete = new HashSet<String>();
    
    // AWS credentials and region
    private static final String AWS_CREDENTIAL_PATH = "./config/credentials";
    private static final Region DYNAMODB_REGION = Region.getRegion(Regions.US_WEST_2);
    
    private static final int READ_CAPACITY = 10;
    private static final int WRITE_CAPACITY = 10;
    
    // Detector output path
    private static final String DETECTOR_OP_FILE = "./test-detector-op-1.csv";
    
    // Definition for table attributes
    private static final String DETECTOR_TABLE = "detector-test-1";
    private static final String DETECTOR_DELETE_TEST_TABLE = "detector-test-2";
    private static final String DETECTOR_TABLE_HK = "HK";
    private static final String DETECTOR_TABLE_RK = "RK";
    private static final String DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS = "NVHK";
    private static final String DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS = "NVRK";
    private static final String DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION = "HSV";
    private static final String DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION = "RSV";
    private static final String DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION = "HTV";
    private static final String DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION = "RTV";
    private static final String DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION = "HSTV";
    private static final String DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION = "RSTV";
    private static final String DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_STRING = "ROSVS";
    private static final String DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_BINARY = "ROSVB";
    private static final String DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_SS = "HTVSS";
    private static final String DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_NS = "HTVNS";
    private static final String DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_BS = "HTVBS";
    private static final String DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_SS = "RTVSS";
    private static final String DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_NS = "RTVNS";
    private static final String DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_BS = "RTVBS";
    
    // Types for all above table attribute names
    private static final String DETECTOR_TABLE_HK_TYPE = "S";
    private static final String DETECTOR_TABLE_RK_TYPE = "S";
    private static final String DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS_TYPE = "S";
    private static final String DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS_TYPE = "N";
    private static final String DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE = "S";
    private static final String DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE = "B";
    private static final String DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE = "S";
    private static final String DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE = "N";
    private static final String DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE = "B";
    private static final String DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE = "B";
    private static final String DETECTOR_DELETE_TEST_TABLE_GSI_HK = "HD";
    private static final String DETECTOR_DELETE_TEST_TABLE_GSI_HK_TYPE = "S";
    private static final String DETECTOR_DELETE_TEST_TABLE_GSI_RK = "RD";
    private static final String DETECTOR_DELETE_TEST_TABLE_GSI_RK_TYPE = "N";
    
    // Counts for violating and non-violating items for each attribute
    private static final int HNV_ITEM_COUNT = 3;
    private static final int RNV_ITEM_COUNT = 11;
    private static final int HSV_VIOLATION_COUNT = 10;
    private static final int HSV_NON_VIOLATION_COUNT = 1;
    private static final int RSV_VIOLATION_COUNT = 5;
    private static final int RSV_NON_VIOLATION_COUNT = 10;
    private static final int HTV_VIOLATION_COUNT = 7;
    private static final int HTV_NON_VIOLATION_COUNT = 5;
    private static final int RTV_VIOLATION_COUNT = 2;
    private static final int RTV_NON_VIOLATION_COUNT = 10;
    private static final int HSTV_VIOLATION_COUNT_TYPE = 4;
    private static final int HSTV_VIOLATION_COUNT_SIZE = 2;
    private static final int HSTV_NON_VIOLATION_COUNT = 13;
    private static final int RSTV_VIOLATION_COUNT_TYPE = 1;
    private static final int RSTV_VIOLATION_COUNT_SIZE = 3;
    private static final int RSTV_NON_VIOLATION_COUNT = 8;
    private static final int ROSVS_VIOLATION_COUNT = 3;
    private static final int ROSVS_NON_VIOLATION_COUNT = 4;
    private static final int ROSVB_VIOLATION_COUNT = 7;
    private static final int ROSVB_NON_VIOLATION_COUNT = 8;
    private static final int HTVSS_VIOLATION_COUNT = 5;
    private static final int HTVSS_NON_VIOLATION_COUNT = 6;
    private static final int HTVNS_VIOLATION_COUNT = 4;
    private static final int HTVNS_NON_VIOLATION_COUNT = 3;
    private static final int HTVBS_VIOLATION_COUNT = 7;
    private static final int HTVBS_NON_VIOLATION_COUNT = 7;
    private static final int RTVSS_VIOLATION_COUNT = 2;
    private static final int RTVSS_NON_VIOLATION_COUNT = 4;
    private static final int RTVNS_VIOLATION_COUNT = 6;
    private static final int RTVNS_NON_VIOLATION_COUNT = 10;
    private static final int RTVBS_VIOLATION_COUNT = 9;
    private static final int RTVBS_NON_VIOLATION_COUNT = 12;
    private static final int HD_VIOLATION_COUNT = 3;
    private static final int HD_NON_VIOLATION_COUNT = 5;
    private static final int RD_VIOLATION_COUNT = 2;
    private static final int RD_NON_VIOLATION_COUNT = 4;
    private static int totalDetectorTableRecords = 0;
    private static int totalDetectorDeleteTableRecords = 0;
    
    // List of violations for some attributes
    private static List<Map<String, AttributeValue>> hsvViolations;
    private static List<Map<String, AttributeValue>> rsvViolations;
    private static List<Map<String, AttributeValue>> htvViolations;
    private static List<Map<String, AttributeValue>> rtvViolations;
    private static List<Map<String, AttributeValue>> hstvSizeViolations;
    private static List<Map<String, AttributeValue>> hstvTypeViolations;
    private static List<Map<String, AttributeValue>> rstvSizeViolations;
    private static List<Map<String, AttributeValue>> rstvTypeViolations;
    private static List<Map<String, AttributeValue>> hdNonViolations;
    private static List<Map<String, AttributeValue>> rdNonViolations;
    
    // Saving state of security manager to revert after tests
    private static final SecurityManager securityManager = System.getSecurityManager();

    @BeforeClass
    public static void setup() throws FileNotFoundException, IOException {
        awsConnection = new AWSConnection(AWS_CREDENTIAL_PATH);
        dynamoDBClient = awsConnection.getDynamoDBClient(DYNAMODB_REGION, TestUtils.RUN_TESTS_ON_DYNAMODB_LOCAL);
        tableManager = new TableManager(dynamoDBClient);
        generateTableAndDataForDetectorTable();
        generateTableAndDataForDetectorDeleteTestTable();
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
        String[] fileNames = {DETECTOR_OP_FILE};
        TestUtils.deleteFiles(fileNames);
    }

    /**
     * If detection is run with GSI hash key name same as the table hash key name, it should throw an error
     * and the program should exit.
     */
    @Test
    public void detectorWithGsiHashKeyNameSameAsTableHashKeyNameTest() throws IllegalArgumentException, IOException {
        try {
            runDetector(DETECTOR_TABLE_HK /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                    DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                    HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT /*violationsFound*/);
            Assert.fail("System did not exit!");
        } catch(ExitException e) {
            Assert.assertEquals("Exit status", 1, e.status);
        }
    }
    
    /**
     * If detection is run with GSI hash key name same as the table range key name, it should throw an error
     * and the program should exit.
     */
    @Test
    public void detectorWithGsiHashKeyNameSameAsTableRangeKeyNameTest() throws IllegalArgumentException, IOException {
        try {
            runDetector(DETECTOR_TABLE_RK /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                    DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                    HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT /*violationsFound*/);
            Assert.fail("System did not exit!");
        } catch(ExitException e) {
            Assert.assertEquals("Exit status", 1, e.status);
        }
    }
    
    /**
     * If detection is run with GSI range key name same as the table hash key name, it should throw an error
     * and the program should exit.
     */
    @Test
    public void detectorWithGsiRangeKeyNameSameAsTableHashKeyNameTest() throws IllegalArgumentException, IOException {
        try {
            runDetector(DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                    DETECTOR_TABLE_HK  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                    HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT /*violationsFound*/);
            Assert.fail("System did not exit!");
        } catch(ExitException e) {
            Assert.assertEquals("Exit status", 1, e.status);
        }
    }

    /**
     * If detection is run with GSI range key name same as the table range key name, it should throw an error
     * and the program should exit.
     */
    @Test
    public void detectorWithGsiRangeKeyNameSameAsTableRangeKeyNameTest() throws IllegalArgumentException, IOException {
        try {
            runDetector(DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                    DETECTOR_TABLE_RK  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                    HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT /*violationsFound*/);
            Assert.fail("System did not exit!");
        } catch(ExitException e) {
            Assert.assertEquals("Exit status", 1, e.status);
        }
    }
    
    /**
     * Run detection when there is size violation on gsi hash and range key.
     */
    @Test
    public void detectorSizeViolationOnHashAndRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with size violation on only Gsi hash key
     */
    @Test
    public void detectorSizeViolationOnHashKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HSV_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with size violation on only gsi range key
     */
    @Test
    public void detectorSizeViolationOnRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, RSV_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on gsi hash and range key
     */
    @Test
    public void detectorTypeViolationOnHashAndRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                HTV_VIOLATION_COUNT + RTV_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on only Gsi hash key
     */
    @Test
    public void detectorTypeViolationOnHashKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HTV_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on only Gsi range key
     */
    @Test
    public void detectorTypeViolationOnRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION  /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE  /*gsiRangeKeyType*/, RTV_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with both size and type violations on both Gsi hash key and Gsi range key.
     */
    @Test
    public void detectorSizeAndTypeViolationOnHashAndRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                HSTV_VIOLATION_COUNT_SIZE + HSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_SIZE /*violationsFound*/);
    }
    
    /**
     * Run detector with both size and type violations on only Gsi Hash Key
     */
    @Test
    public void detectorSizeAndTypeViolationOnHashKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HSTV_VIOLATION_COUNT_SIZE + HSTV_VIOLATION_COUNT_TYPE /*violationsFound*/);
    }
    
    /**
     * Run detector with both size and type violations on only Gsi Range Key
     */
    @Test
    public void detectorSizeAndTypeViolationOnRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION  /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE  /*gsiRangeKeyType*/, RSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_SIZE /*violationsFound*/);
    }
    
    /**
     * Run detection with size violation on Gsi Range Key of type String with the violated size being less than max
     * hash key size but more than max range key size
     */
    @Test
    public void detectorSizeViolationOnStringRangeKeyWithSizeLessThanMaxHashKeySizeTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_STRING  /*gsiRangeKeyName*/, 
                TestUtils.STRING_TYPE  /*gsiRangeKeyType*/, ROSVS_VIOLATION_COUNT /*violationsFound*/);
        
        // make sure this same attribute does not violate for hash key
        runDetector(DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_STRING /*gsiHashKeyName*/, TestUtils.STRING_TYPE /*gsiHashKeyType*/,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, 0 /*violationsFound*/);
    }
    
    /**
     * Run detection with size violation on Gsi Range key of type Binary with the violated size being less than max
     * hash key size but more than max range key size
     */
    @Test
    public void detectorSizeViolationOnBinaryRangeKeyWithSizeLessThanMaxHashKeySizeTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_BINARY  /*gsiRangeKeyName*/, 
                TestUtils.BINARY_TYPE  /*gsiRangeKeyType*/, ROSVB_VIOLATION_COUNT /*violationsFound*/);
        
        // make sure this same attribute does not violate for hash key
        runDetector(DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_BINARY /*gsiHashKeyName*/, TestUtils.BINARY_TYPE /*gsiHashKeyType*/,
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, 0 /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on Gsi Hash key of type String
     */
    @Test
    public void detectorStringSetTypeViolationOnHashKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_SS /*gsiHashKeyName*/, TestUtils.STRING_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HTVSS_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on Gsi Hash key of type Binary
     */
    @Test
    public void detectorBinarySetTypeViolationOnHashKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_BS /*gsiHashKeyName*/, TestUtils.BINARY_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HTVBS_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on Gsi Hash key of type Number
     */
    @Test
    public void detectorNumberSetTypeViolationOnHashKeyTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_NS /*gsiHashKeyName*/, TestUtils.NUMBER_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HTVNS_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on Gsi Range key with having violating type as a string set
     */
    @Test
    public void detectorStringSetTypeViolationOnRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_SS  /*gsiRangeKeyName*/, 
                TestUtils.STRING_TYPE  /*gsiRangeKeyType*/, RTVSS_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on Gsi Range key with having violating type as a binary set
     */
    @Test
    public void detectorBinarySetTypeViolationOnRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_BS  /*gsiRangeKeyName*/, 
                TestUtils.BINARY_TYPE  /*gsiRangeKeyType*/, RTVBS_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection with type violation on Gsi Range key with having violating type as a number set
     */
    @Test
    public void detectorNumberSetTypeViolationOnRangeKeyTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_NS  /*gsiRangeKeyName*/, 
                TestUtils.NUMBER_TYPE  /*gsiRangeKeyType*/, RTVNS_VIOLATION_COUNT /*violationsFound*/);
    }
    
    /**
     * Run detection on Gsi Range key with no violations
     */
    @Test
    public void detectorWithNoViolationsOnRangeTest() throws IllegalArgumentException, IOException {
        runDetector(null /*gsiHashKeyName*/, null /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS  /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS_TYPE  /*gsiRangeKeyType*/, 0 /*violationsFound*/);
    }
    
    /**
     * Run detection on Gsi Hash key with no violations
     */
    @Test
    public void detectorWithNoViolationsOnHashTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS_TYPE /*gsiHashKeyType*/, 
                null /*gsiRangeKeyName*/, null /*gsiRangeKeyType*/, 0 /*violationsFound*/);
    }
    
    /**
     * Run detection on Gsi Hash and Range key with both having no violations
     */
    @Test
    public void detectorWithNoViolationsOnHashAndRangeTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS_TYPE /*gsiRangeKeyType*/, 0 /*violationsFound*/);
    }
    
    /**
     * Run detector with a specified number of violations to scan set as lower than the violations that exist.
     */
    @Test
    public void detectorWithNumOfViolationsSetTest() throws IllegalArgumentException, IOException {
        int numOfViolationsToScan = 5;
        int[] detectionOutput = runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                numOfViolationsToScan /*violationsFound*/, null /*recordsScanned*/, 0 /*violationsDeleted*/ ,
                -1 /*numOfRecords*/, numOfViolationsToScan, 1 /*numOfSegments*/, 
                false /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
        Assert.assertTrue("Records scanned must be greater than or equal to the value set for numOfViolations in options", 
                detectionOutput[TestUtils.DETECTOR_ITEMS_SCANNED_INDEX] >= numOfViolationsToScan);
    }
    
    /**
     * Run detector with a specified number of violations to scan set as higher than the violations that exist.
     */
    @Test
    public void detectorWithNumOfViolationsSetGreaterThanExistingViolationsTest() throws IllegalArgumentException, IOException {
        int numOfViolationsToScan = HSV_VIOLATION_COUNT + 1;
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HSV_VIOLATION_COUNT /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ ,
                -1 /*numOfRecords*/, numOfViolationsToScan, 1 /*numOfSegments*/, 
                false /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
    }
    
    /**
     * Run detector with number of records to scan set as greater than the existing records in the table.
     */
    @Test
    public void detectorWithNumOfRecordsSetGreaterThanExistingRecordsTest() throws IllegalArgumentException, IOException {
        int numOfRecordsToScan = totalDetectorTableRecords + 1;
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                null  /*gsiRangeKeyName*/, null  /*gsiRangeKeyType*/, HSV_VIOLATION_COUNT /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ ,
                numOfRecordsToScan /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, 
                false /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
    }
    
    /**
     * Run detector with number of records to scan set as lesser than the existing records in the table.
     */
    @Test
    public void detectorWithNumOfRecordsSetTest() throws IllegalArgumentException, IOException {
        int numOfRecordsToScan = 7;
        int[] detectionOutput = runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                null /*violationsFound*/, numOfRecordsToScan /*recordsScanned*/, 0 /*violationsDeleted*/ ,
                numOfRecordsToScan /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, 
                false /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
        Assert.assertTrue("Violations found must be less than or equal to the value set for numOfRecords in options", 
                detectionOutput[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX] <= numOfRecordsToScan);
    }
    
    /**
     * Run detection specifying both number of records to scan and number of violations to scan.
     */
    @Test
    public void detectorWithBothNumOfRecordsAndViolationsSetTest() throws IllegalArgumentException, IOException {
        int numOfRecordsToScan = 7;
        int numOfViolationsToFind = 5;
        int[] detectionOutput = runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                null /*violationsFound*/, null /*recordsScanned*/, 0 /*violationsDeleted*/ ,
                numOfRecordsToScan /*numOfRecords*/, numOfViolationsToFind, 1 /*numOfSegments*/, 
                false /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
        Assert.assertTrue("Violations found must be less than or equal to the value set for numOfRecords in options", 
                detectionOutput[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX] <= numOfRecordsToScan);
        Assert.assertTrue("Violations found must be less than or equal to the value set for numOfViolations in options", 
                detectionOutput[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX] <= numOfViolationsToFind);
        Assert.assertTrue("Number of records scanned must be less than or equal to the value set for numOfRecords in options", 
               detectionOutput[TestUtils.DETECTOR_ITEMS_SCANNED_INDEX] <= numOfRecordsToScan);
    }
    
    /**
     * Run detection specifying the number of segments
     */
    @Test
    public void detectorWithNumOfSegmentsSetTest() throws IllegalArgumentException, IOException {
        int numOfSegments = 3;
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT /*violationsFound*/, totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ ,
                -1 /*numOfRecords*/, -1, numOfSegments /*numOfSegments*/, false /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
    }
    
    /**
     * Run detection in delete all violations mode.
     */
    @Test
    public void detectorWithDeleteViolationsSetTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_DELETE_TEST_TABLE, DETECTOR_DELETE_TEST_TABLE_GSI_HK /*gsiHashKeyName*/, 
                DETECTOR_DELETE_TEST_TABLE_GSI_HK_TYPE /*gsiHashKeyType*/, DETECTOR_DELETE_TEST_TABLE_GSI_RK  /*gsiRangeKeyName*/, 
                DETECTOR_DELETE_TEST_TABLE_GSI_RK_TYPE /*gsiRangeKeyType*/, (HD_VIOLATION_COUNT + RD_VIOLATION_COUNT) /*violationsFound*/, 
                totalDetectorDeleteTableRecords /*recordsScanned*/, (HD_VIOLATION_COUNT + RD_VIOLATION_COUNT) /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, true /*deleteViolations*/, 100 /*readWriteIOPSPercent*/);
        
        List<Map<String, AttributeValue>> itemsLeftInTable = tableManager.getItems(DETECTOR_DELETE_TEST_TABLE);
        
        // make sure that the items were physically deleted from the table
        Assert.assertTrue("Number of items left in the table must be equal to number of non-violating items", 
                itemsLeftInTable.size() == (totalDetectorDeleteTableRecords - (HD_VIOLATION_COUNT + RD_VIOLATION_COUNT)));
        
        // make sure non-violating items only exist in the table
        for(Map<String, AttributeValue> item : hdNonViolations) {
            Assert.assertTrue("Non violationg item deleted from the table!", itemsLeftInTable.remove(item));
        }
        
        for(Map<String, AttributeValue> item : rdNonViolations) {
            Assert.assertTrue("Non violationg item deleted from the table!", itemsLeftInTable.remove(item));
        }
        
        Assert.assertTrue("Unknown items found in table!", itemsLeftInTable.size() == 0);
    }
    
    /**
     * Run detection with keys having size violation and with violation output file generated.
     * Also verify the contents of the output file.
     */
    @Test
    public void detectorWithSizeViolationsOutputToFileTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE /*gsiRangeKeyType*/, (HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT) /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, false /*deleteViolations*/, DETECTOR_OP_FILE, false /*recordGSIValue*/);
        
        checkViolationFileOutput(DETECTOR_OP_FILE, DETECTOR_TABLE_HK, DETECTOR_TABLE_RK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK_TYPE, 
                false /*violatedValueExists*/, hsvViolations, rsvViolations, null, null, TestUtils.MAX_HASH_KEY_SIZE + 1, 
                DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE,
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE);
    }
    
    /**
     * Run detection with keys having type violation and with violation output file generated.
     * Also verify the contents of the output file.
     */
    @Test
    public void detectorWithTypeViolationsOutputToFileTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE /*gsiRangeKeyType*/, (HTV_VIOLATION_COUNT + RTV_VIOLATION_COUNT) /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, false /*deleteViolations*/, DETECTOR_OP_FILE, false /*recordGSIValue*/);
        
        checkViolationFileOutput(DETECTOR_OP_FILE, DETECTOR_TABLE_HK, DETECTOR_TABLE_RK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK_TYPE, 
                false /*violatedValueExists*/, null, null, htvViolations, rtvViolations, TestUtils.MAX_HASH_KEY_SIZE + 1, 
                DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE,
                DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE);
    }
    
    /**
     * Run detection with keys having both size and type violation and with violation output file generated.
     * Also verify the contents of the output file.
     */
    @Test
    public void detectorWithSizeAndTypeViolationsOutputToFileTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE /*gsiRangeKeyType*/, 
                (HSTV_VIOLATION_COUNT_SIZE + RSTV_VIOLATION_COUNT_SIZE + HSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_TYPE) /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, false /*deleteViolations*/, DETECTOR_OP_FILE, false /*recordGSIValue*/);
        
        checkViolationFileOutput(DETECTOR_OP_FILE, DETECTOR_TABLE_HK, DETECTOR_TABLE_RK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK_TYPE, 
                false /*violatedValueExists*/, hstvSizeViolations, rstvSizeViolations, hstvTypeViolations, rstvTypeViolations, TestUtils.MAX_HASH_KEY_SIZE + 1, 
                DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, 
                DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE);
    }
    
    /**
     * Run detection with keys having size violation and with violation output file generated (with the gsi key value).
     * Also verify the contents of the output file.
     */
    @Test
    public void detectorWithSizeViolationsOutputToFileWithGsiValuesTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE /*gsiRangeKeyType*/, (HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT) /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, false /*deleteViolations*/, DETECTOR_OP_FILE, true /*recordGSIValue*/);
        
        checkViolationFileOutput(DETECTOR_OP_FILE, DETECTOR_TABLE_HK, DETECTOR_TABLE_RK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK_TYPE, 
                true /*violatedValueExists*/, hsvViolations, rsvViolations, null, null, TestUtils.MAX_HASH_KEY_SIZE + 1, 
                DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE,
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE);
    }
    
    /**
     * Run detection with keys having type violation and with violation output file generated (with the gsi key value).
     * Also verify the contents of the output file.
     */
    @Test
    public void detectorWithTypeViolationsOutputToFileWithGsiValuesTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE /*gsiRangeKeyType*/, (HTV_VIOLATION_COUNT + RTV_VIOLATION_COUNT) /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, false /*deleteViolations*/, DETECTOR_OP_FILE, true /*recordGSIValue*/);
        
        checkViolationFileOutput(DETECTOR_OP_FILE, DETECTOR_TABLE_HK, DETECTOR_TABLE_RK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK_TYPE, 
                true /*violatedValueExists*/, null, null, htvViolations, rtvViolations, TestUtils.MAX_HASH_KEY_SIZE + 1, 
                DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE,
                DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE);
    }
    
    /**
     * Run detection with keys having both type and size violations and with violation output file generated (with the gsi key value).
     * Also verify the contents of the output file.
     */
    @Test
    public void detectorWithSizeAndTypeViolationsOutputToFileWithGsiValuesTest() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION /*gsiHashKeyName*/, 
                DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE /*gsiHashKeyType*/, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION /*gsiRangeKeyName*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE /*gsiRangeKeyType*/, 
                (HSTV_VIOLATION_COUNT_SIZE + RSTV_VIOLATION_COUNT_SIZE + HSTV_VIOLATION_COUNT_TYPE + RSTV_VIOLATION_COUNT_TYPE) /*violationsFound*/, 
                totalDetectorTableRecords /*recordsScanned*/, 0 /*violationsDeleted*/ , 
                -1 /*numOfRecords*/, -1 /*numOfViolations*/, 1 /*numOfSegments*/, false /*deleteViolations*/, DETECTOR_OP_FILE, true /*recordGSIValue*/);
        
        checkViolationFileOutput(DETECTOR_OP_FILE, DETECTOR_TABLE_HK, DETECTOR_TABLE_RK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK_TYPE, 
                true /*violatedValueExists*/, hstvSizeViolations, rstvSizeViolations, hstvTypeViolations, rstvTypeViolations, TestUtils.MAX_HASH_KEY_SIZE + 1, 
                DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE, 
                DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE);
    }
    
    /**
     * Run detection with rate limiting on
     */
    @Test
    public void detectorWithRateLimitingOn() throws IllegalArgumentException, IOException {
        runDetector(DETECTOR_TABLE, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION /*gsiHashKeyName*/, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE /*gsiHashKeyType*/, 
                DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION  /*gsiRangeKeyName*/, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE  /*gsiRangeKeyType*/, 
                (HSV_VIOLATION_COUNT + RSV_VIOLATION_COUNT) /*violationsFound*/, totalDetectorTableRecords /*recordsScanned*/, 
                0 /*violationsDeleted*/ , -1 /*numOfRecords*/, -1 /*numOfViolationsToScan*/, 1 /*numOfSegments*/, 
                false /*deleteViolations*/, 10 /*readWriteIOPSPercent*/);
    }
    
    /**
     * Verifies the contents of the violation detection output file
     */
    private void checkViolationFileOutput(String detectorOpFile, String tableHashKey, String tableRangeKey, 
            String tableHashKeyType, String tableRangeKeyType, boolean violatedValueExists,
            List<Map<String, AttributeValue>> hashKeySizeViolations, List<Map<String, AttributeValue>> rangeKeySizeViolations, 
            List<Map<String, AttributeValue>> hashKeyTypeViolations, List<Map<String, AttributeValue>> rangeKeyTypeViolations, 
            int violatedSize, String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, String gsiRangeKeyType) throws IOException {
        
        Map<String, String> hashToRangeHashSizeViolationsMap = new HashMap<String, String>();
        Map<String, String> hashToRangeRangeSizeViolationsMap = new HashMap<String, String>();
        Map<String, String> hashToRangeHashTypeViolationsMap = new HashMap<String, String>();
        Map<String, String> hashToRangeRangeTypeViolationsMap = new HashMap<String, String>();
        
        Map<String, String> hashToGsiHashSizeViolationsMap = new HashMap<String, String>();
        Map<String, String> hashToGsiRangeSizeViolationsMap = new HashMap<String, String>();
        Map<String, String> hashToGsiHashTypeViolationsMap = new HashMap<String, String>();
        Map<String, String> hashToGsiRangeTypeViolationsMap = new HashMap<String, String>();
        
        BufferedReader br = null;
        CSVParser parser = null;
        try {
            br = new BufferedReader(new FileReader(new File(detectorOpFile)));
            parser = new CSVParser(br, TestUtils.csvFormat);
            List<CSVRecord> csvRecords = parser.getRecords();
            for(CSVRecord csvRecord : csvRecords) {
                String hashKey = csvRecord.get(ViolationRecord.TABLE_HASH_KEY);
                String rangeKey = csvRecord.get(ViolationRecord.TABLE_RANGE_KEY);
                String gsiHashKeyValue = null;
                if(violatedValueExists) {
                    gsiHashKeyValue = csvRecord.get(ViolationRecord.GSI_HASH_KEY);
                }
                String hashKeyViolationType = csvRecord.get(ViolationRecord.GSI_HASH_KEY_VIOLATION_TYPE);
                String hashKeyViolationDesc = csvRecord.get(ViolationRecord.GSI_HASH_KEY_VIOLATION_DESC);
                String gsiRangeKeyValue = null;
                if(violatedValueExists) {
                    gsiRangeKeyValue = csvRecord.get(ViolationRecord.GSI_RANGE_KEY);
                }
                String rangeKeyViolationType = csvRecord.get(ViolationRecord.GSI_RANGE_KEY_VIOLATION_TYPE);
                String rangeKeyViolationDesc = csvRecord.get(ViolationRecord.GSI_RANGE_KEY_VIOLATION_DESC);
                boolean foundViolation = false;
                if(hashKeyViolationType.equals("Size Violation")) {
                    foundViolation = true;
                    hashToRangeHashSizeViolationsMap.put(hashKey, rangeKey);
                    if(violatedValueExists) {
                        hashToGsiHashSizeViolationsMap.put(hashKey, gsiHashKeyValue);
                    }
                    Assert.assertTrue("Gsi hash key size violation description is incorrect", 
                            hashKeyViolationDesc.equals("Max Bytes Allowed: " + TestUtils.MAX_HASH_KEY_SIZE + " Found: " + violatedSize));
                } else if(hashKeyViolationType.equals("Type Violation")) {
                    foundViolation = true;
                    hashToRangeHashTypeViolationsMap.put(hashKey, rangeKey);
                    if(violatedValueExists) {
                        hashToGsiHashTypeViolationsMap.put(hashKey, gsiHashKeyValue);
                    }
                    Assert.assertTrue("Gsi hash key type violation description is incorrect", 
                            hashKeyViolationDesc.equals("Expected: " + gsiHashKeyType + " Found: " + TestUtils.returnDifferentAttributeType(gsiHashKeyType)));
                } else {
                    Assert.assertTrue("Hash key violation description exists even when there is no violation type", hashKeyViolationDesc.equals(""));
                }
                
                if(rangeKeyViolationType.equals("Size Violation")) {
                    foundViolation = true;
                    hashToRangeRangeSizeViolationsMap.put(hashKey, rangeKey);
                    if(violatedValueExists) {
                        hashToGsiRangeSizeViolationsMap.put(hashKey, gsiRangeKeyValue);
                    }
                    Assert.assertTrue("GSI range key size violation description is incorrect", 
                            rangeKeyViolationDesc.equals("Max Bytes Allowed: " + TestUtils.MAX_RANGE_KEY_SIZE + " Found: " + violatedSize));
                } else if(rangeKeyViolationType.equals("Type Violation")) {
                    foundViolation = true;
                    hashToRangeRangeTypeViolationsMap.put(hashKey, rangeKey);
                    if(violatedValueExists) {
                        hashToGsiRangeTypeViolationsMap.put(hashKey, gsiRangeKeyValue);
                    }
                    Assert.assertTrue("Gsi range key type violation description is incorrect", 
                            rangeKeyViolationDesc.equals("Expected: " + gsiRangeKeyType + " Found: " + TestUtils.returnDifferentAttributeType(gsiRangeKeyType)));
                } else {
                    Assert.assertTrue("Range key violation description exists even when there is no violation type", rangeKeyViolationDesc.equals(""));
                }
                
                Assert.assertTrue("No violation found in a row!", foundViolation);
            }
            
            if(hashKeySizeViolations != null) {
                for(Map<String, AttributeValue> item : hashKeySizeViolations) {
                    AttributeValue tableHashAttr = item.get(tableHashKey);
                    String expectedTableHashKey = AttributeValueConverter.toBlankString(tableHashAttr);
                    
                    if(hashToRangeHashSizeViolationsMap.containsKey(expectedTableHashKey)) {
                        if(tableRangeKey != null) {
                            AttributeValue tableRangeAttr = item.get(tableRangeKey);
                            String expectedTableRangeKey = AttributeValueConverter.toBlankString(tableRangeAttr);
                            Assert.assertEquals("Size violated GSI hash key's table's hash key's range key does not match in the output!", 
                                    expectedTableRangeKey, hashToRangeHashSizeViolationsMap.get(expectedTableHashKey));
                        }
                        hashToRangeHashSizeViolationsMap.remove(expectedTableHashKey);
                    } else {
                        Assert.fail("Expected size violation on hash key not found!");
                    }
                    
                    // Check for gsi hash value
                    if(violatedValueExists) {
                        AttributeValue gsiHashAttr = item.get(gsiHashKeyName);
                        String expectedGsiHashValue = AttributeValueConverter.toStringWithAttributeType(gsiHashAttr);
                        Assert.assertEquals("Size violated Gsi hash value mis-match", expectedGsiHashValue, 
                                hashToGsiHashSizeViolationsMap.get(expectedTableHashKey));
                        hashToGsiHashSizeViolationsMap.remove(expectedTableHashKey);
                    }
                }
                Assert.assertEquals("Extra entries found for gsi hash key size violations", 0, hashToRangeHashSizeViolationsMap.size());
                Assert.assertEquals("Extra entries found for gsi hash key size violation values", 0, hashToGsiHashSizeViolationsMap.size());
            }
            
            if(rangeKeySizeViolations != null) {
                for(Map<String, AttributeValue> item : rangeKeySizeViolations) {
                    AttributeValue tableHashAttr = item.get(tableHashKey);
                    String expectedTableHashKey = AttributeValueConverter.toBlankString(tableHashAttr);
                    
                    if(hashToRangeRangeSizeViolationsMap.containsKey(expectedTableHashKey)) {
                        if(tableRangeKey != null) {
                            AttributeValue tableRangeAttr = item.get(tableRangeKey);
                            String expectedTableRangeKey = AttributeValueConverter.toBlankString(tableRangeAttr);
                            Assert.assertEquals("Size violated GSI range key's table's hash key's range key does not match in the output!", 
                                    expectedTableRangeKey, hashToRangeRangeSizeViolationsMap.get(expectedTableHashKey));
                        }
                        hashToRangeRangeSizeViolationsMap.remove(expectedTableHashKey);
                    } else {
                        Assert.fail("Expected size violation on range key not found!");
                    }
                    
                    // Check for gsi range value
                    if(violatedValueExists) {
                        AttributeValue gsiRangeAttr = item.get(gsiRangeKeyName);
                        String expectedGsiRangeValue = AttributeValueConverter.toStringWithAttributeType(gsiRangeAttr);
                        Assert.assertEquals("Size violated Gsi range value mis-match", expectedGsiRangeValue, 
                                hashToGsiRangeSizeViolationsMap.get(expectedTableHashKey));
                        hashToGsiRangeSizeViolationsMap.remove(expectedTableHashKey);
                    }
                }
                
                Assert.assertEquals("Extra entries found for gsi range key size violations", 0, hashToRangeRangeSizeViolationsMap.size());
                Assert.assertEquals("Extra entries found for gsi range key size violation values", 0, hashToGsiRangeSizeViolationsMap.size());
            }
            
            if(hashKeyTypeViolations != null) {
                for(Map<String, AttributeValue> item : hashKeyTypeViolations) {
                    AttributeValue tableHashAttr = item.get(tableHashKey);
                    String expectedTableHashKey = AttributeValueConverter.toBlankString(tableHashAttr);
                    
                    if(hashToRangeHashTypeViolationsMap.containsKey(expectedTableHashKey)) {
                        if(tableRangeKey != null) {
                            AttributeValue tableRangeAttr = item.get(tableRangeKey);
                            String exptectedTableRangeKey = AttributeValueConverter.toBlankString(tableRangeAttr);
                            Assert.assertEquals("Type violated GSI hash key's table's hash key's range key does not match in the output!", 
                                    exptectedTableRangeKey, hashToRangeHashTypeViolationsMap.get(expectedTableHashKey));
                        }
                        hashToRangeHashTypeViolationsMap.remove(expectedTableHashKey);
                    } else {
                        Assert.fail("Expected type violation on hash key not found!");
                    }
                    
                    // Check for gsi hash value
                    if(violatedValueExists) {
                        AttributeValue gsiHashAttr = item.get(gsiHashKeyName);
                        String expectedGsiHashValue = AttributeValueConverter.toStringWithAttributeType(gsiHashAttr);
                        Assert.assertEquals("Type violated Gsi hash value mis-match", expectedGsiHashValue, 
                                hashToGsiHashTypeViolationsMap.get(expectedTableHashKey));
                        hashToGsiHashTypeViolationsMap.remove(expectedTableHashKey);
                    }
                }
                Assert.assertEquals("Extra entries found for gsi hash key type violations", 0, hashToRangeHashTypeViolationsMap.size());
                Assert.assertEquals("Extra entries found for gsi hash key type violation values", 0, hashToGsiHashTypeViolationsMap.size());
            }
            
            if(rangeKeyTypeViolations != null) {
                for(Map<String, AttributeValue> item : rangeKeyTypeViolations) {
                    AttributeValue tableHashAttr = item.get(tableHashKey);
                    String expectedTableHashKey = AttributeValueConverter.toBlankString(tableHashAttr);
                    
                    if(hashToRangeRangeTypeViolationsMap.containsKey(expectedTableHashKey)) {
                        if(tableRangeKey != null) {
                            AttributeValue tableRangeAttr = item.get(tableRangeKey);
                            String exptectedTableRangeKey = AttributeValueConverter.toBlankString(tableRangeAttr);
                            Assert.assertEquals("Type violated GSI range key's table's hash key's range key does not match in the output!", 
                                    exptectedTableRangeKey, hashToRangeRangeTypeViolationsMap.get(expectedTableHashKey));
                        }
                        hashToRangeRangeTypeViolationsMap.remove(expectedTableHashKey);
                    } else {
                        Assert.fail("Expected type violation on range key not found!");
                    }
                    
                    // Check for gsi range value
                    if(violatedValueExists) {
                        AttributeValue gsiRangeAttr = item.get(gsiRangeKeyName);
                        String expectedGsiRangeValue = AttributeValueConverter.toStringWithAttributeType(gsiRangeAttr);
                        Assert.assertEquals("Type violated Gsi range value mis-match", expectedGsiRangeValue, 
                                hashToGsiRangeTypeViolationsMap.get(expectedTableHashKey));
                        hashToGsiRangeTypeViolationsMap.remove(expectedTableHashKey);
                    }
                }
                Assert.assertEquals("Extra entries found for gsi range key type violations", 0, hashToRangeRangeTypeViolationsMap.size());
                Assert.assertEquals("Extra entries found for gsi range key type violation values", 0, hashToGsiRangeTypeViolationsMap.size());
            }
            
        } finally {
            br.close();
            parser.close();
        }
    }
    
    /**
     * Run detection with the given inputs.
     * Also verify the detection output against the expected output.
     */
    private void runDetector(String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, String gsiRangeKeyType, 
            int violationsFound) throws IllegalArgumentException, IOException {
        Options options = getOptions();
        options.setGsiHashKeyName(gsiHashKeyName);
        options.setGsiHashKeyType(gsiHashKeyType);
        options.setGsiRangeKeyName(gsiRangeKeyName);
        options.setGsiRangeKeyType(gsiRangeKeyType);
        options.setTableName(DETECTOR_TABLE);
        ViolationDetector violationDetector = getViolationDetector(options, DETECTOR_TABLE);
        violationDetector.violationDetection(false);
        verifyDetectorCounts(violationDetector.getViolationDectectionOutput(), 
                totalDetectorTableRecords, violationsFound, 0 /* violationsDeleted */);
    }
    
    /**
     * Run detection with the given inputs.
     * Also verify the detection output against the expected output.
     * Returns the detection results.
     */
    private int[] runDetector(String tableName, String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, 
            String gsiRangeKeyType, Integer violationsFound, Integer recordsScanned, Integer violationsDeleted, 
            int numOfRecords, int numOfViolationsToScan, int numOfSegments, boolean deleteViolations, 
            int readWriteIOPSPercent) throws IllegalArgumentException, IOException {
        Options options = getOptions();
        options.setGsiHashKeyName(gsiHashKeyName);
        options.setGsiHashKeyType(gsiHashKeyType);
        options.setGsiRangeKeyName(gsiRangeKeyName);
        options.setGsiRangeKeyType(gsiRangeKeyType);
        options.setTableName(tableName);
        options.setNumOfRecords(numOfRecords);
        options.setNumOfSegments(numOfSegments);
        options.setNumOfViolations(numOfViolationsToScan);
        options.setReadWriteIOPSPercentage(readWriteIOPSPercent);
        ViolationDetector violationDetector = getViolationDetector(options, tableName);
        violationDetector.violationDetection(deleteViolations);
        verifyDetectorCounts(violationDetector.getViolationDectectionOutput(), 
                recordsScanned, violationsFound, violationsDeleted);
        return violationDetector.getViolationDectectionOutput();
    }
    
    /**
     * Run detection with the given inputs.
     * Also verify the detection output against the expected output.
     */
    private void runDetector(String tableName, String gsiHashKeyName, String gsiHashKeyType, String gsiRangeKeyName, String gsiRangeKeyType, 
            Integer violationsFound, Integer recordsScanned, Integer violationsDeleted, int numOfRecords, 
            int numOfViolationsToScan, int numOfSegments, boolean deleteViolations, String outputFile,
            boolean recordGsiValue) throws IllegalArgumentException, IOException {
        Options options = getOptions();
        options.setGsiHashKeyName(gsiHashKeyName);
        options.setGsiHashKeyType(gsiHashKeyType);
        options.setGsiRangeKeyName(gsiRangeKeyName);
        options.setGsiRangeKeyType(gsiRangeKeyType);
        options.setTableName(tableName);
        options.setNumOfRecords(numOfRecords);
        options.setNumOfSegments(numOfSegments);
        options.setNumOfViolations(numOfViolationsToScan);
        options.setDetectionOutputPath(outputFile);
        options.setRecordDetails(true);
        options.setRecordGsiValueInViolationRecord(recordGsiValue);
        ViolationDetector violationDetector = getViolationDetector(options, tableName);
        violationDetector.violationDetection(deleteViolations);
        verifyDetectorCounts(violationDetector.getViolationDectectionOutput(), 
                recordsScanned, violationsFound, violationsDeleted);
    }
    
    private void verifyDetectorCounts(int[] violationDectectionOutput, Integer recordsExpected, 
            Integer violationsExpected, Integer deleteExpected) {
        if(recordsExpected != null) {
            Assert.assertEquals("Records Scanned count does not match", (int)recordsExpected, violationDectectionOutput[TestUtils.DETECTOR_ITEMS_SCANNED_INDEX]);
        }
        
        if(violationsExpected != null) {
            Assert.assertEquals("Violation count does not match", (int)violationsExpected, violationDectectionOutput[TestUtils.DETECTOR_VIOLATIONS_FOUND_INDEX]);
        }
        
        if(deleteExpected != null) {
            Assert.assertEquals("Violation delete count does not match", (int)deleteExpected, violationDectectionOutput[TestUtils.DETECTOR_VIOLATIONS_DELETED_INDEX]);
        }
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
        options.setDynamoDBRegion(DYNAMODB_REGION);
        options.setNumOfRecords(-1);
        options.setNumOfSegments(1);
        options.setNumOfViolations(-1);
        options.setReadWriteIOPSPercentage(100);
        options.setRecordDetails(false);
        options.setRecordGsiValueInViolationRecord(false);
        return options;
    }


    private static void generateTableAndDataForDetectorTable() {
        boolean tableCreated = tableManager.createNewTable(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, 
                DETECTOR_TABLE_RK, DETECTOR_TABLE_RK_TYPE, READ_CAPACITY, WRITE_CAPACITY);
        Assert.assertTrue("Table creation failed!", tableCreated);
        tablesToDelete.add(DETECTOR_TABLE);
        
        // load values for HSV
        Map<String, String> hsvAttributes = new HashMap<String, String>();
        hsvAttributes.put(DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION, DETECTOR_TABLE_HK_WITH_SIZE_VIOLATION_TYPE);
        hsvViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hsvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSV_VIOLATION_COUNT);
        totalDetectorTableRecords += HSV_VIOLATION_COUNT;
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hsvAttributes, TestUtils.MAX_HASH_KEY_SIZE - 1, HSV_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += HSV_NON_VIOLATION_COUNT;

        // load values for RSV
        Map<String, String> rsvAttributes = new HashMap<String, String>();
        rsvAttributes.put(DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION, DETECTOR_TABLE_RK_WITH_SIZE_VIOLATION_TYPE);
        rsvViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rsvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSV_VIOLATION_COUNT);
        totalDetectorTableRecords += RSV_VIOLATION_COUNT;
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rsvAttributes, TestUtils.MAX_RANGE_KEY_SIZE - 1, RSV_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += RSV_NON_VIOLATION_COUNT;

        // load values for HTV
        Map<String, String> htvAttributes = new HashMap<String, String>();
        htvAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE));
        htvViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvAttributes, 5, HTV_VIOLATION_COUNT);
        totalDetectorTableRecords += HTV_VIOLATION_COUNT;
        htvAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION, DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvAttributes, 5, HTV_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += HTV_NON_VIOLATION_COUNT;
        
        // load values for RTV
        Map<String, String> rtvAttributes = new HashMap<String, String>();
        rtvAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE));
        rtvViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvAttributes, 5, RTV_VIOLATION_COUNT);
        totalDetectorTableRecords += RTV_VIOLATION_COUNT;
        rtvAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION, DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvAttributes, 5, RTV_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += RTV_NON_VIOLATION_COUNT;
        
        // load values for HSTV
            // type violations
        Map<String, String> hstvAttributes = new HashMap<String, String>();
        hstvAttributes.put(DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE));
        hstvTypeViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hstvAttributes, 4, HSTV_VIOLATION_COUNT_TYPE);
        totalDetectorTableRecords += HSTV_VIOLATION_COUNT_TYPE;
            // size violations
        hstvAttributes.put(DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION, DETECTOR_TABLE_HK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE);
        hstvSizeViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hstvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, HSTV_VIOLATION_COUNT_SIZE);
        totalDetectorTableRecords += HSTV_VIOLATION_COUNT_SIZE;
            // no violations
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hstvAttributes, 5, HSTV_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += HSTV_NON_VIOLATION_COUNT;
        
        // load values for RSTV
            // type violations
        Map<String, String> rstvAttributes = new HashMap<String, String>();
        rstvAttributes.put(DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, TestUtils.returnDifferentAttributeType(DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE));
        rstvTypeViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rstvAttributes, 4, RSTV_VIOLATION_COUNT_TYPE);
        totalDetectorTableRecords += RSTV_VIOLATION_COUNT_TYPE;
            // size violations
        rstvAttributes.put(DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION, DETECTOR_TABLE_RK_WITH_SIZE_AND_TYPE_VIOLATION_TYPE);
        rstvSizeViolations = tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rstvAttributes, TestUtils.MAX_HASH_KEY_SIZE + 1, RSTV_VIOLATION_COUNT_SIZE);
        totalDetectorTableRecords += RSTV_VIOLATION_COUNT_SIZE;
            // no violations
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rstvAttributes, 5, RSTV_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += RSTV_NON_VIOLATION_COUNT;
        
        // load values for ROSVS
        Map<String, String> rosvsAttributes = new HashMap<String, String>();
        rosvsAttributes.put(DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_STRING, TestUtils.STRING_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rosvsAttributes, TestUtils.MAX_RANGE_KEY_SIZE + 1, ROSVS_VIOLATION_COUNT);
        totalDetectorTableRecords += ROSVS_VIOLATION_COUNT;
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rosvsAttributes, TestUtils.MAX_RANGE_KEY_SIZE, ROSVS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += ROSVS_NON_VIOLATION_COUNT;
        
        // load values for ROSVB
        Map<String, String> rosvbAttributes = new HashMap<String, String>();
        rosvbAttributes.put(DETECTOR_TABLE_RK_ONLY_SIZE_VIOLATION_BINARY, TestUtils.BINARY_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rosvbAttributes, TestUtils.MAX_RANGE_KEY_SIZE + 1, ROSVB_VIOLATION_COUNT);
        totalDetectorTableRecords += ROSVB_VIOLATION_COUNT;
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rosvbAttributes, TestUtils.MAX_RANGE_KEY_SIZE, ROSVB_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += ROSVB_NON_VIOLATION_COUNT;
        
        // load values for HTVSS
        Map<String, String> htvssAttributes = new HashMap<String, String>();
        htvssAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_SS, TestUtils.STRING_SET_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvssAttributes, 5, HTVSS_VIOLATION_COUNT);
        totalDetectorTableRecords += HTVSS_VIOLATION_COUNT;
        htvssAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_SS, TestUtils.STRING_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvssAttributes, 5, HTVSS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += HTVSS_NON_VIOLATION_COUNT;
        
        // load values for HTVBS
        Map<String, String> htvbsAttributes = new HashMap<String, String>();
        htvbsAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_BS, TestUtils.BINARY_SET_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvbsAttributes, 5, HTVBS_VIOLATION_COUNT);
        totalDetectorTableRecords += HTVBS_VIOLATION_COUNT;
        htvbsAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_BS, TestUtils.BINARY_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvbsAttributes, 5, HTVBS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += HTVBS_NON_VIOLATION_COUNT;
        
        // load values for HTVNS
        Map<String, String> htvnsAttributes = new HashMap<String, String>();
        htvnsAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_NS, TestUtils.NUMBER_SET_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvnsAttributes, 5, HTVNS_VIOLATION_COUNT);
        totalDetectorTableRecords += HTVNS_VIOLATION_COUNT;
        htvnsAttributes.put(DETECTOR_TABLE_HK_WITH_TYPE_VIOLATION_NS, TestUtils.NUMBER_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, htvnsAttributes, 5, HTVNS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += HTVNS_NON_VIOLATION_COUNT;
        
        // load values for RTVSS
        Map<String, String> rtvssAttributes = new HashMap<String, String>();
        rtvssAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_SS, TestUtils.STRING_SET_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvssAttributes, 5, RTVSS_VIOLATION_COUNT);
        totalDetectorTableRecords += RTVSS_VIOLATION_COUNT;
        rtvssAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_SS, TestUtils.STRING_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvssAttributes, 5, RTVSS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += RTVSS_NON_VIOLATION_COUNT;
        
        // load values for RTVBS
        Map<String, String> rtvbsAttributes = new HashMap<String, String>();
        rtvbsAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_BS, TestUtils.BINARY_SET_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvbsAttributes, 5, RTVBS_VIOLATION_COUNT);
        totalDetectorTableRecords += RTVBS_VIOLATION_COUNT;
        rtvbsAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_BS, TestUtils.BINARY_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvbsAttributes, 5, RTVBS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += RTVBS_NON_VIOLATION_COUNT;
        
        // load values for RTVNS
        Map<String, String> rtvnsAttributes = new HashMap<String, String>();
        rtvnsAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_NS, TestUtils.NUMBER_SET_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvnsAttributes, 5, RTVNS_VIOLATION_COUNT);
        totalDetectorTableRecords += RTVNS_VIOLATION_COUNT;
        rtvnsAttributes.put(DETECTOR_TABLE_RK_WITH_TYPE_VIOLATION_NS, TestUtils.NUMBER_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rtvnsAttributes, 5, RTVNS_NON_VIOLATION_COUNT);
        totalDetectorTableRecords += RTVNS_NON_VIOLATION_COUNT;
        
        // load values for HNV
        Map<String, String> hnvAttributes = new HashMap<String, String>();
        hnvAttributes.put(DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS, DETECTOR_TABLE_HK_WITH_NO_VIOLATIONS_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hnvAttributes, 5, HNV_ITEM_COUNT);
        totalDetectorTableRecords += HNV_ITEM_COUNT;
        
        // load values for RNV
        Map<String, String> rnvAttributes = new HashMap<String, String>();
        rnvAttributes.put(DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS, DETECTOR_TABLE_RK_WITH_NO_VIOLATIONS_TYPE);
        tableManager.loadRandomData(DETECTOR_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rnvAttributes, 5, RNV_ITEM_COUNT);
        totalDetectorTableRecords += RNV_ITEM_COUNT;
    }
    
    private static void generateTableAndDataForDetectorDeleteTestTable() {
        boolean tableCreated = tableManager.createNewTable(DETECTOR_DELETE_TEST_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, 
                DETECTOR_TABLE_RK, DETECTOR_TABLE_RK_TYPE, READ_CAPACITY, WRITE_CAPACITY);
        Assert.assertTrue("Table creation failed!", tableCreated);
        tablesToDelete.add(DETECTOR_DELETE_TEST_TABLE);
        
        // load values for HD
        Map<String, String> hdAttributes = new HashMap<String, String>();
        hdAttributes.put(DETECTOR_DELETE_TEST_TABLE_GSI_HK, TestUtils.returnDifferentAttributeType(DETECTOR_DELETE_TEST_TABLE_GSI_HK_TYPE));
        tableManager.loadRandomData(DETECTOR_DELETE_TEST_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hdAttributes, 5, HD_VIOLATION_COUNT);
        totalDetectorDeleteTableRecords += HD_VIOLATION_COUNT;
        hdAttributes.put(DETECTOR_DELETE_TEST_TABLE_GSI_HK, DETECTOR_DELETE_TEST_TABLE_GSI_HK_TYPE);
        hdNonViolations = tableManager.loadRandomData(DETECTOR_DELETE_TEST_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, hdAttributes, 5, HD_NON_VIOLATION_COUNT);
        totalDetectorDeleteTableRecords += HD_NON_VIOLATION_COUNT;
        
        // load values for RD
        Map<String, String> rdAttributes = new HashMap<String, String>();
        rdAttributes.put(DETECTOR_DELETE_TEST_TABLE_GSI_RK, TestUtils.returnDifferentAttributeType(DETECTOR_DELETE_TEST_TABLE_GSI_RK_TYPE));
        tableManager.loadRandomData(DETECTOR_DELETE_TEST_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rdAttributes, 5, RD_VIOLATION_COUNT);
        totalDetectorDeleteTableRecords += RD_VIOLATION_COUNT;
        rdAttributes.put(DETECTOR_DELETE_TEST_TABLE_GSI_RK, DETECTOR_DELETE_TEST_TABLE_GSI_RK_TYPE);
        rdNonViolations = tableManager.loadRandomData(DETECTOR_DELETE_TEST_TABLE, DETECTOR_TABLE_HK, DETECTOR_TABLE_HK_TYPE, DETECTOR_TABLE_RK, 
                DETECTOR_TABLE_RK_TYPE, rdAttributes, 5, RD_NON_VIOLATION_COUNT);
        totalDetectorDeleteTableRecords += RD_NON_VIOLATION_COUNT;
    }

}
