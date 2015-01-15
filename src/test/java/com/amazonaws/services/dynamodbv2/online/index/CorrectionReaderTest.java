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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.online.index.CorrectionReader;

/**
 * 
 * Unit test for ViolationWriter
 * 
 */
public class CorrectionReaderTest {

    /** Mock objects */
    @SuppressWarnings("unchecked")
    Iterator<CSVRecord> mockIterator = Mockito.mock(Iterator.class);
    Reader mockReader = Mockito.mock(Reader.class);

    CorrectionReader correctionReader;
    RandomDataGenerator randDataGenerator = new RandomDataGenerator();

    @Before
    public void setupBeforeTest() {
        correctionReader = new CorrectionReader(mockReader, mockIterator);
    }

    @Test(expected = FileNotFoundException.class)
    public void testLoadCSVFileWithInvalidFilePath() throws IOException {
        CorrectionReader correctionReader = new CorrectionReader();
        correctionReader.loadCSVFile("invalidFilePath");
    }

    @Test
    public void testMovoToNextRecordIfHasWithValidNextValue() {
        Mockito.when(mockIterator.hasNext()).thenReturn(true);
        Mockito.when(mockIterator.next()).thenReturn(null);
        assertTrue(correctionReader.moveToNextRecordIfHas());
    }

    @Test
    public void testMovoToNextRecordIfHasWithNoNextValue() {
        Mockito.when(mockIterator.hasNext()).thenReturn(false);
        assertFalse(correctionReader.moveToNextRecordIfHas());
    }

    @Test
    public void testGetValueInRecordByName() throws IOException {
        // CSVRecord cannot be constructed and mocked
    }

    @Test
    public void testGetValueInRecordByNameWithIllegalName() {
        // CSVRecord cannot be constructed and mocked
    }
}
