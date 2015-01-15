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

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.online.index.ViolationChecker;
import com.amazonaws.services.dynamodbv2.online.index.ViolationRecord;
import com.amazonaws.services.dynamodbv2.online.index.ViolationWriter;

/**
 * 
 * Unit test violation checker.
 * 
 */
public class ViolationCheckerTest {
    /** Mock objects */
    private ViolationRecord mockViolationRecord = Mockito.mock(ViolationRecord.class);
    private ViolationWriter mockViolationWriter = Mockito.mock(ViolationWriter.class);

    private ViolationChecker violationChecker;

    private static RandomDataGenerator dataGenerator = new RandomDataGenerator();

    private static String outputFilePath = "/tmp/testoutput.csv";
    private static String hashKeyName = "hashKey";
    private static String rangeKeyName = "rangeKey";
    private static String attributeName1 = "attribute1";
    private static String attributeName1Type = "S";
    private static String attributeName2 = "attribute2";
    private static String attributeName2Type = "N";

    @Before
    public void setupBeforeTest() {
        violationChecker = new ViolationChecker(outputFilePath, hashKeyName, rangeKeyName, attributeName1, attributeName1Type, attributeName2,
                attributeName2Type, true, true, mockViolationRecord, mockViolationWriter);
    }

    @Test
    public void testCheckAttributeViolationWithHashKeySizeViolation() {
        String value = dataGenerator.nextRadomString(ViolationChecker.MAX_HASH_KEY_SIZE + 10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "S", KeyType.HASH));
    }

    @Test
    public void testCheckAttributeViolationWithRangeKeySizeViolation() {
        String value = dataGenerator.nextRadomString(ViolationChecker.MAX_RANGE_KEY_SIZE + 10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "S", KeyType.RANGE));
    }

    @Test
    public void testCheckAttributeViolationWithHashTypeWiolation() {
        String value = dataGenerator.nextRadomString(10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "N", KeyType.HASH));
    }

    @Test
    public void testCheckAttributeViolationWithRangeTypeViolation() {
        String value = dataGenerator.nextRadomString(10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "N", KeyType.RANGE));
    }

    @Test
    public void testCheckAttribtueViolationWithSSTypeViolation() {
        List<String> value = dataGenerator.nextRandomStringArray(10, 1);
        AttributeValue keyValue = new AttributeValue().withSS(value);
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "S", KeyType.RANGE));
    }

    @Test
    public void testCheckAttributeViolationWithBSTypeViolation() {
        AttributeValue keyValue = new AttributeValue().withBS(dataGenerator.nextRandomBinaryArray(10));
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "B", KeyType.RANGE));
    }

    @Test
    public void testCheckAttributViolationWithNSTypeViolation() {
        AttributeValue keyValue = new AttributeValue().withNS(dataGenerator.nextRandomIntArray());
        assertTrue(violationChecker.checkAttributeViolation(keyValue, "N", KeyType.RANGE));
    }


    @Test
    public void testRecordItemTableHashKeyWithS() {
        String value = dataGenerator.nextRadomString(10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        violationChecker.recordItemTableHashKey(keyValue);
        Mockito.verify(mockViolationRecord).setTableHashKey(value);
    }

    @Test
    public void testRecordItemTableHashKeyWithN() {
        String value = String.valueOf(dataGenerator.nextRandomInt());
        AttributeValue keyValue = new AttributeValue().withN(value);
        violationChecker.recordItemTableHashKey(keyValue);
        Mockito.verify(mockViolationRecord).setTableHashKey(value);
    }

    @Test
    public void testRecordItemTableHashKeyWithB() {
        ByteBuffer value = dataGenerator.nextRandomBinary(10);
        AttributeValue keyValue = new AttributeValue().withB(value);
        violationChecker.recordItemTableHashKey(keyValue);
        Mockito.verify(mockViolationRecord).setTableHashKey(new String(value.array()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRecordItemTableHashKeyWithSS() {
        List<String> value = dataGenerator.nextRandomStringArray(10);
        AttributeValue keyValue = new AttributeValue().withSS(value);
        violationChecker.recordItemTableHashKey(keyValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRecordItemTableHashKeyWithNS() {
        List<String> value = dataGenerator.nextRandomIntArray();
        AttributeValue keyValue = new AttributeValue().withSS(value);
        violationChecker.recordItemTableHashKey(keyValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRecordItemTableHashKeyWithBS() {
        List<ByteBuffer> value = dataGenerator.nextRandomBinaryArray(10);
        AttributeValue keyValue = new AttributeValue().withBS(value);
        violationChecker.recordItemTableHashKey(keyValue);
    }

    @Test
    public void testRecordItemTableRangeKeyWithS() {
        String value = dataGenerator.nextRadomString(10);
        AttributeValue keyValue = new AttributeValue().withS(value);
        violationChecker.recordItemTableRangeKey(keyValue);
        Mockito.verify(mockViolationRecord).setTableRangeKey(value);
    }

    @Test
    public void testRecordItemTableRangeKeyWithB() {
        ByteBuffer value = dataGenerator.nextRandomBinary(10);
        AttributeValue keyValue = new AttributeValue().withB(value);
        violationChecker.recordItemTableRangeKey(keyValue);
        Mockito.verify(mockViolationRecord).setTableRangeKey(new String(value.array()));
    }

    @Test
    public void testRecordItemTableRangeKeyWithN() {
        int value = dataGenerator.nextRandomInt();
        AttributeValue keyValue = new AttributeValue().withN(String.valueOf(value));
        violationChecker.recordItemTableRangeKey(keyValue);
        Mockito.verify(mockViolationRecord).setTableRangeKey(String.valueOf(value));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRecordItemTableRangeKeyWithSS() {
        AttributeValue keyValue = new AttributeValue().withSS(dataGenerator.nextRandomStringArray(10));
        violationChecker.recordItemTableRangeKey(keyValue);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testRecordItemTableRangeKeyWithNS() {
        AttributeValue keyValue = new AttributeValue().withNS(dataGenerator.nextRandomIntArray());
        violationChecker.recordItemTableRangeKey(keyValue);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testRecordItemTableRangeKeyWithBS() {
        AttributeValue keyValue = new AttributeValue().withBS(dataGenerator.nextRandomBinaryArray(10));
        violationChecker.recordItemTableRangeKey(keyValue);
    }
}
