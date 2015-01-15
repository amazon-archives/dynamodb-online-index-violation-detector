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

import java.util.List;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.online.index.ViolationRecord;

/**
 * 
 * Unit test ViolationRecords.
 * 
 */
public class ViolationRecordTest {
    private boolean tableHasRangeKey;
    private boolean checkGSIHashkey;
    private boolean checkGSIRangeKey;
    private RandomDataGenerator randDataGenerator = new RandomDataGenerator();

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSet() {
        tableHasRangeKey = false;
        checkGSIHashkey = true;
        checkGSIRangeKey = true;
        ViolationRecord record = new ViolationRecord(tableHasRangeKey, checkGSIHashkey, checkGSIRangeKey, true);
        record.setTableRangeKey(randDataGenerator.nextRadomString(10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckNoGSIKey() {
        tableHasRangeKey = true;
        checkGSIHashkey = false;
        checkGSIRangeKey = false;
        new ViolationRecord(tableHasRangeKey, checkGSIHashkey, checkGSIRangeKey, true);
    }

    @Test
    public void testToList() {
        tableHasRangeKey = true;
        checkGSIHashkey = true;
        checkGSIRangeKey = true;
        ViolationRecord record = new ViolationRecord(tableHasRangeKey, checkGSIHashkey, checkGSIRangeKey, true);
        List<String> randomStrArray = randDataGenerator.nextRandomStringArray(10, 4);
        record.setTableHashKey(randomStrArray.get(0));
        record.setTableRangeKey(randomStrArray.get(1));
        record.setGSIHashKey(randomStrArray.get(2));
        record.setGSIRangeKey(randomStrArray.get(3));
        List<String> returnedArray = record.toStringList();
        for (String s : randomStrArray) {
            assertTrue(returnedArray.contains(s));
        }
    }
}
