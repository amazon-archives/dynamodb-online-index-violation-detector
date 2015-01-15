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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.online.index.Correction;
import com.amazonaws.services.dynamodbv2.online.index.CorrectionReader;
import com.amazonaws.services.dynamodbv2.online.index.Options;
import com.amazonaws.services.dynamodbv2.online.index.TableHelper;
import com.amazonaws.services.dynamodbv2.online.index.TableWriter;
import com.amazonaws.services.dynamodbv2.online.index.ViolationRecord;

/**
 * 
 * Unit tests for Correction.java
 * 
 */
public class CorrectionTest {
    private RandomDataGenerator randDataGenerator = new RandomDataGenerator();

    /** Mock objects */
    private Options mockOptions = Mockito.mock(Options.class);
    private CorrectionReader mockCorrectionReader = Mockito.mock(CorrectionReader.class);
    private TableHelper mockTableHelper = Mockito.mock(TableHelper.class);
    private TableWriter mockTableWriter = Mockito.mock(TableWriter.class);

    private Correction correction;
    private String rangeKeyName = "rangeKey";
    private String gsiHashKeyName = "gsiHashKey";
    private String gsiRangeKeyName = "gsiRangeKey";
    private String gsiHashKeyType = "S";
    private String gsiRangeKyeType = "N";
    private String correctionFilePath = "/correction/file";

    @Before
    public void setupBeforeTests() throws Exception {
        correction = new Correction(mockOptions, mockCorrectionReader, mockTableHelper, mockTableWriter);
        /** Setup stubs for testing */
        Mockito.when(mockOptions.getCorrectionInputPath()).thenReturn("randompath");
        Mockito.when(mockOptions.isCorrectionInputS3Path()).thenReturn(false);
        Mockito.when(mockOptions.getGsiHashKeyName()).thenReturn(gsiHashKeyName);
        Mockito.when(mockOptions.getGsiHashKeyType()).thenReturn(gsiHashKeyType);
        Mockito.when(mockOptions.getGsiRangeKeyName()).thenReturn(gsiRangeKeyName);
        Mockito.when(mockOptions.getGsiRangeKeyType()).thenReturn(gsiRangeKyeType);
    }

    @Test
    public void testLoadCorrectionFile() throws IOException {
        correction.loadRecordsFromCorrectionFile(correctionFilePath);
        Mockito.verify(mockCorrectionReader).loadCSVFile(correctionFilePath);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadCorrectionFileWithInvalidFilePath() throws IOException {
        Mockito.doThrow(new FileNotFoundException()).when(mockCorrectionReader).loadCSVFile(Mockito.anyString());
        correction.loadRecordsFromCorrectionFile(correctionFilePath);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadCorrectionFileWithIOException() throws IOException {
        Mockito.doThrow(new IOException()).when(mockCorrectionReader).loadCSVFile(Mockito.anyString());
        correction.loadRecordsFromCorrectionFile(correctionFilePath);
    }

    @Test
    public void testGetNextTableHashKey() {
        String testTableHashKey = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.TABLE_HASH_KEY)).thenReturn(testTableHashKey);
        assertEquals("Should return the next table hash key", testTableHashKey, correction.getNextTableHashKey());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNextTableHashKeyWithValueNotFound() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.TABLE_HASH_KEY)).thenReturn(null);
        correction.getNextTableHashKey();
    }

    @Test
    public void testGetNextTableRangeKey() {
        String testTableRangeKey = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.TABLE_RANGE_KEY)).thenReturn(testTableRangeKey);
        Mockito.when(mockTableHelper.getTableRangeKeyName()).thenReturn(rangeKeyName);
        assertEquals("Should get the next table range key", testTableRangeKey, correction.getNextTableRangeKey());
    }

    @Test
    public void testGetNextTableRangeKeyWithNoRangeKeyOnTable() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.TABLE_RANGE_KEY)).thenReturn(null);
        Mockito.when(mockTableHelper.getTableRangeKeyName()).thenReturn(null);
        assertEquals("Should get the next table range key", null, correction.getNextTableRangeKey());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNextTableRangeKeyWithNoTableRangeKeyButFoundOnInputFile() {
        String testTableRangeKey = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.TABLE_RANGE_KEY)).thenReturn(testTableRangeKey);
        Mockito.when(mockTableHelper.getTableRangeKeyName()).thenReturn(null);
        correction.getNextTableRangeKey();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNextTableRangeKeyWithTableRangeKeyButNotFoundOnFile() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.TABLE_RANGE_KEY)).thenReturn(null);
        Mockito.when(mockTableHelper.getTableRangeKeyName()).thenReturn(rangeKeyName);
        correction.getNextTableRangeKey();
    }

    @Test
    public void testGetNextGsiHashKeyUpdateValue() {
        String testGsiHashKeyUpdateValue = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE)).thenReturn(testGsiHashKeyUpdateValue);
        Mockito.when(mockOptions.getGsiHashKeyName()).thenReturn(gsiHashKeyName);
        assertEquals("Should return the given gsi hash key update value", testGsiHashKeyUpdateValue, correction.getNextGsiHashKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiHashKeyUpdateValueWithNoGsiHashKey() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE)).thenReturn(null);
        Mockito.when(mockOptions.getGsiHashKeyName()).thenReturn(null);
        assertEquals("Should return the given gsi hash key update value", null, correction.getNextGsiHashKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiHashKeyUpdateValueWithNoGsiHashKeyButProvidedOnInputFile() {
        String testGsiHashKeyUpdateValue = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE)).thenReturn(testGsiHashKeyUpdateValue);
        Mockito.when(mockOptions.getGsiHashKeyName()).thenReturn(null);
        assertEquals("Should return the given gsi hash key update value", testGsiHashKeyUpdateValue, correction.getNextGsiHashKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiHashKeyUpdateValueWithGsiHashKeyButNotProvidedOnInputFile() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE)).thenReturn(null);
        Mockito.when(mockOptions.getGsiHashKeyName()).thenReturn(gsiHashKeyName);
        assertEquals("Should return null", null, correction.getNextGsiHashKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiRangeKeyUpdateValue() {
        String testGsiRangeKeyUpdateValue = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE)).thenReturn(testGsiRangeKeyUpdateValue);
        Mockito.when(mockOptions.getGsiRangeKeyName()).thenReturn(gsiRangeKeyName);
        assertEquals("Should return next range key update value", testGsiRangeKeyUpdateValue, correction.getNextGsiRangeKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiRangeKeyUpdateValueWithNoGsiRangeKey() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE)).thenReturn(null);
        Mockito.when(mockOptions.getGsiRangeKeyName()).thenReturn(null);
        assertEquals("Should return next range key update value", null, correction.getNextGsiRangeKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiRangeKeyUpdateValueWithNoGsiRangeKeyButFoundOnInputFile() {
        String testGsiRangeKeyUpdateValue = randDataGenerator.nextRadomString(10);
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE)).thenReturn(testGsiRangeKeyUpdateValue);
        Mockito.when(mockOptions.getGsiRangeKeyName()).thenReturn(null);
        assertEquals("Should return next range key update value", testGsiRangeKeyUpdateValue, correction.getNextGsiRangeKeyUpdateValue());
    }

    @Test
    public void testGetNextGsiRangeKeyUpdateValueWithGsiRangeKeyButNotFoundOnInputFile() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE)).thenReturn(null);
        Mockito.when(mockOptions.getGsiRangeKeyName()).thenReturn(gsiRangeKeyName);
        assertEquals("Should have returned null", null, correction.getNextGsiRangeKeyUpdateValue());
    }

    @Test
    public void testGetNextDeleteBlankAttributeYes() {
        String testNextDeleteBlankAttributeString = "Y";
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_CORRECTION_DELETE_BLANK)).thenReturn(testNextDeleteBlankAttributeString);
        assertTrue(correction.getNextDeleteBlankAttribute());
    }
    
    @Test
    public void testGetNextDeleteBlankAttributeNO() {
        String testNextDeleteBlankAttributeString = "N";
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_CORRECTION_DELETE_BLANK)).thenReturn(testNextDeleteBlankAttributeString);
        assertFalse(correction.getNextDeleteBlankAttribute());
    } 
    
    @Test
    public void testGetNextDeleteBlankAttributeEmpty() {
        String testNextDeleteBlankAttributeString = "";
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_CORRECTION_DELETE_BLANK)).thenReturn(testNextDeleteBlankAttributeString);
        assertFalse(correction.getNextDeleteBlankAttribute());
    } 
    @Test
    public void testGetNextDeleteBlankAttributeWithNoColumn() {
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_CORRECTION_DELETE_BLANK)).thenReturn(null);
        correction.getNextDeleteBlankAttribute();
        assertFalse(correction.getNextDeleteBlankAttribute());
    } 
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetNextDeleteBlankAttributeWithInvalidValue() {
        String testNextDeleteBlankAttributeString = "invalidValue";
        Mockito.when(mockCorrectionReader.getValueInRecordByName(ViolationRecord.GSI_CORRECTION_DELETE_BLANK)).thenReturn(testNextDeleteBlankAttributeString);
        assertFalse(correction.getNextDeleteBlankAttribute());
    }
    
    
}
