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

import java.io.File;
import java.security.Permission;

import org.apache.commons.csv.CSVFormat;

/**
 * Utility methods for functional tests
 * 
 */
public class TestUtils {
    
    // Set this to false if you do not wish to run tests on DynamoDB Local.
    // Make sure that the server is running on port 8000 before kicking off the tests.
    public static final boolean RUN_TESTS_ON_DYNAMODB_LOCAL = true;
    
    public static final String STRING_TYPE = "S";
    public static final String NUMBER_TYPE = "N";
    public static final String BINARY_TYPE = "B";
    public static final String STRING_SET_TYPE = "SS";
    public static final String NUMBER_SET_TYPE = "NS";
    public static final String BINARY_SET_TYPE = "BS";
    
    public static final int MAX_HASH_KEY_SIZE = 2048;
    public static final int MAX_RANGE_KEY_SIZE = 1024;
    
    public static final CSVFormat csvFormat = CSVFormat.RFC4180.withHeader().withDelimiter(',').withIgnoreEmptyLines(true);
    
    public static final int DETECTOR_ITEMS_SCANNED_INDEX = 0;
    public static final int DETECTOR_VIOLATIONS_FOUND_INDEX = 1;
    public static final int DETECTOR_VIOLATIONS_DELETED_INDEX = 2;
    
    public static final int CORRECTION_SUCCESSFUL_UPDATES_INDEX = 0;
    public static final int CORRECTION_UPDATE_REQUESTS_INDEX = 1;
    public static final int CORRECTION_CONDITIONAL_UPDATE_FAILURES_INDEX = 2;
    public static final int CORRECTION_UNEXPECTED_ERRORS_INDEX = 3;
    
    
    @SuppressWarnings("serial")
	protected static class ExitException extends SecurityException {
        public final int status;
        public ExitException(int status) {
            this.status = status;
        }
    }

    protected static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
            // allow anything.
        }
        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }
        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }
    
    public static String returnDifferentAttributeType(String givenType) {
        if(STRING_TYPE.equals(givenType)) {
            return NUMBER_TYPE;
        } else if (NUMBER_TYPE.equals(givenType)) {
            return BINARY_TYPE;
        } else if (BINARY_TYPE.equals(givenType)) {
            return STRING_TYPE;
        }
        return null;
    }
    
    public static void deleteFiles(String[] fileNames) {
        for(String fileName : fileNames) {
            File file = new File(fileName);
            if(file.exists()) {
                file.delete();
            }
        }
    }
}
