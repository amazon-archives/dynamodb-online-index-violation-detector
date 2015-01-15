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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * Write violation records to file.
 * 
 */
public class ViolationWriter {
    private BufferedWriter bufferWriter = null;
    private CSVPrinter printer = null;
    private CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',').withIgnoreEmptyLines(true);
    private static ViolationWriter instance = new ViolationWriter();

    private ViolationWriter() {
    };

    public static ViolationWriter getInstance() {
        return instance;
    }

    public void createOutputFile(String outputFilePath) throws IOException {
        File outputFile = new File(outputFilePath);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        outputFile.createNewFile();
        FileWriter out = new FileWriter(outputFilePath, true);
        bufferWriter = new BufferedWriter(out);
        printer = new CSVPrinter(bufferWriter, format);
    }

    public void addViolationRecord(ViolationRecord violationRecord) throws IOException {
        synchronized (this) {
            if (violationRecord != null) {
                printer.printRecord(violationRecord.toStringList());
            }
        }
    }
    
    public void addViolationRecord(List<String> record) throws IOException {
        synchronized (this) {
            if (record != null) {
                printer.printRecord(record);
            }
        }
    }

    public void flushAndCloseWriter() throws IOException {
        if(bufferWriter != null) {
            bufferWriter.flush();
            bufferWriter.close();
        }
        
        if(printer != null) {
            printer.close();
        }
    }
}
