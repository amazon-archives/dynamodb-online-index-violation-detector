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

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Read input correction file.
 * 
 */
public class CorrectionReader {
    private Reader reader = null;
    private CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',').withIgnoreEmptyLines(true);
    private CSVParser parser = null;
    private Iterator<CSVRecord> recordIterator;
    private CSVRecord currentRecord;

    /**
     * Constructor for unit test
     */
    protected CorrectionReader(Reader reader, Iterator<CSVRecord> recordIterator) {
        this.reader = reader;
        this.recordIterator = recordIterator;
    }

    public CorrectionReader() {}

    public void loadCSVFile(String csvFilePath) throws IOException {
        reader = new FileReader(csvFilePath);
        parser = new CSVParser(reader, format);
        recordIterator = parser.iterator();
    }
    
    public boolean ifContainsColumn(String columnName) {
         return parser.getHeaderMap().containsKey(columnName);
    }

    public boolean moveToNextRecordIfHas() {
        if (recordIterator.hasNext()) {
            currentRecord = recordIterator.next();
            return true;
        }
        return false;
    }

    public String getValueInRecordByName(String recordColumnName) {
        try {
            String value = currentRecord.get(recordColumnName);
            if(value.equals("")) {
                return null;
            }
            return value;
        } catch (IllegalArgumentException iae) {
            return null;
        }
    }
    
    public List<String> getHeader() {
        return new ArrayList<String>(parser.getHeaderMap().keySet());
    }
    
    public List<String> getCurrentRecord() {
        List<String> record = new ArrayList<String>();
        for(int i = 0 ; i < currentRecord.size() ; i++) {
            record.add(i, currentRecord.get(i));
        }
        return record;
    }

}
