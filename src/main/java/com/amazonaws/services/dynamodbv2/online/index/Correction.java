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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;

/**
 * Control violation correction.
 * 
 */
public class Correction {
    private Options options;
    private TableHelper tableHelper;
    private CorrectionReader correctionReader;
    private TableWriter tableWriter;
    private long violationUpdateRequests = 0; // includes both delete and update
    private long successfulUpdates = 0;
    private long conditionalUpdateFailures = 0;
    private long unexpectedErrors = 0;
    
    /*Correction runs as a single thread, so number of tasks set for rate limiter is 1*/
    private static final int NUM_OF_TASKS_FOR_CORRECTION = 1;

    /**
     * Constructor for Unit test only.
     */
    protected Correction(Options options, CorrectionReader correctionReader, TableHelper tableHelper, TableWriter tableWriter) {
        this.options = options;
        this.correctionReader = correctionReader;
        this.tableHelper = tableHelper;
        this.tableWriter = tableWriter;
    }

    public Correction(Options options, TableHelper tableHelper, AmazonDynamoDBClient dynamoDBClient, boolean isRunningOnDDBLocal) throws IOException {
        this.options = options;
        this.tableHelper = tableHelper;
        this.correctionReader = new CorrectionReader();
        this.tableWriter = new TableWriter(options, tableHelper, dynamoDBClient, NUM_OF_TASKS_FOR_CORRECTION, isRunningOnDDBLocal);
    }
    
    protected void createViolationWriter() throws IOException {
        String outputFilePath;
        if (options.isCorrectionOutputS3Path()) {
            outputFilePath = options.getTmpCorrectionOutputPath();
        } else {
            outputFilePath = options.getCorrectionOutputPath();
        }
        ViolationWriter.getInstance().createOutputFile(outputFilePath);
    }

    public void deleteFromFile() throws IllegalArgumentException {
        String correctionFilePath = options.getCorrectionInputPath();
        PrintHelper.printDeleteStartInfo(correctionFilePath);

        if (options.isCorrectionInputS3Path())
            correctionFilePath = options.getTmpCorrectionInputPath();
        loadRecordsFromCorrectionFile(correctionFilePath);
        while (correctionReader.moveToNextRecordIfHas()) {
            addRecordToDeleteRequest();
        }
        sendDeleteRequests();

        PrintHelper.printCorrectionDeleteSummary(violationUpdateRequests);
    }

    protected void addRecordToDeleteRequest() {
        try {
            AttributeValue tableHashKeyValue = tableWriter.genAttributeValueForTableKey(tableHelper.getTableHashKeyType(), getNextTableHashKey());
            String nextTableRangeKey = getNextTableRangeKey();
            AttributeValue tableRangeKeyValue = nextTableRangeKey == null ? null : tableWriter.genAttributeValueForTableKey(tableHelper.getTableRangeKeyType(),
                    nextTableRangeKey);
            violationUpdateRequests += tableWriter.addDeleteRequest(tableHashKeyValue, tableRangeKeyValue);
        } catch (IllegalArgumentException iae) {
            /**
             * When string is empty, genAttributeValueForTableKey throw
             * exception, ignore.
             */
        }
    }

    protected void sendDeleteRequests() {
        try {
        	violationUpdateRequests += tableWriter.sendDeleteRequests();
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(iae.getMessage());
        }
    }

    /**
     * returns 'true' if output file was generated, 'false' otherwise
     */
    public boolean updateFromFile(boolean useConditionalUpdate) throws Exception {
        String correctionFilePath = options.getCorrectionInputPath();
        PrintHelper.printUpdateStartInfo(correctionFilePath);
        if (options.isCorrectionInputS3Path()) {
            correctionFilePath = options.getTmpCorrectionInputPath();
        }

        loadRecordsFromCorrectionFile(correctionFilePath);
        checkUseConditionalUpdate(useConditionalUpdate);
        
        // Used to create correction output file only if an error occurs
        boolean isCorrectionOutputFileGenerated = false;
        
        try {
            while (correctionReader.moveToNextRecordIfHas()) {
                try {
                    Map<String, AttributeValue> primaryKey = genTablePrimaryKeyForRecord();
                    Map<String, AttributeValueUpdate> updateItems = genUpdateItemsForRecord();
                    Map<String, ExpectedAttributeValue> expectedItems = null;
                    if (useConditionalUpdate) {
                        expectedItems = genExpectedItemsForRecord(updateItems);
                    }
                    if(tableWriter.sendUpdateRequest(primaryKey, updateItems, expectedItems)) {
                        successfulUpdates++;
                    }
                } catch(Exception e) {
                    if(e instanceof ConditionalCheckFailedException) {
                        conditionalUpdateFailures++;
                    } else {
                        unexpectedErrors++;
                    }
                    
                    // generate output file if it does not exist
                    if(!isCorrectionOutputFileGenerated) {
                        createViolationWriter();
                        isCorrectionOutputFileGenerated = true;
                        // Add header to the output file
                        List<String> correctionOutputHeader = new ArrayList<String>(correctionReader.getHeader());
                        correctionOutputHeader.add(ViolationRecord.GSI_VALUE_UPDATE_ERROR); // Add another column for error
                        ViolationWriter.getInstance().addViolationRecord(correctionOutputHeader);
                    }
                    List<String> failedRecord = new ArrayList<String>(correctionReader.getCurrentRecord());
                    // Add error to the record
                    failedRecord.add(correctionReader.getHeader().size(), e.getMessage());
                    ViolationWriter.getInstance().addViolationRecord(failedRecord);
                }
            }
        } finally {
            // close the file
            if(isCorrectionOutputFileGenerated) {
                ViolationWriter.getInstance().flushAndCloseWriter();
            }
        }
        
        if(useConditionalUpdate) {
            PrintHelper.printCorrectionSummary(violationUpdateRequests, successfulUpdates, 
                    conditionalUpdateFailures, unexpectedErrors, options.getCorrectionOutputPath());
        } else {
            PrintHelper.printCorrectionSummary(violationUpdateRequests, successfulUpdates, 
                    unexpectedErrors, options.getCorrectionOutputPath());
        }
        
        if(conditionalUpdateFailures > 0 || unexpectedErrors > 0) {
            return true;
        }
        
        return false;
    }
    
    private boolean checkUseConditionalUpdate(boolean useConditionalUpdate) {
        if (useConditionalUpdate) {
            if (options.getGsiHashKeyName() != null && !correctionReader.ifContainsColumn(ViolationRecord.GSI_HASH_KEY)) {
                throw new IllegalArgumentException("Error: Failed to find " + ViolationRecord.GSI_HASH_KEY + " on input file, cannot start conditional update");
            }
            if (options.getGsiRangeKeyName() != null && !correctionReader.ifContainsColumn(ViolationRecord.GSI_RANGE_KEY)) {
                throw new IllegalArgumentException("Error: Failed to find " + ViolationRecord.GSI_RANGE_KEY
                        + " on input file, cannot start conditional update ");
            }
            return true;
        }
        return false;
    }

    protected void loadRecordsFromCorrectionFile(String correctionFilePath) {
        try {
            correctionReader.loadCSVFile(correctionFilePath);
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException("Error: Given correction file '" + correctionFilePath + "' not exists.");
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Error: Failed to read correction file '" + correctionFilePath + "' .");
        }
    }

    protected Map<String, AttributeValue> genTablePrimaryKeyForRecord() {
        try {
            AttributeValue tableHashKeyValue = tableWriter.genAttributeValueForTableKey(tableHelper.getTableHashKeyType(), getNextTableHashKey());
            String nextTableRangeKey = getNextTableRangeKey();
            AttributeValue tableRangeKeyValue = nextTableRangeKey == null ? null : tableWriter.genAttributeValueForTableKey(tableHelper.getTableRangeKeyType(),
                    nextTableRangeKey);
            return tableWriter.genTablePrimaryKey(tableHashKeyValue, tableRangeKeyValue);
        } catch (IllegalArgumentException iae) {
            /**
             * When string is empty, genAttributeValueForTableKey throw
             * exception, ignore.
             */
            return null;
        }
    }

    protected Map<String, AttributeValueUpdate> genUpdateItemsForRecord() {
        Map<String, AttributeValueUpdate> updateItems = null;
        AttributeValue GSIHashKeyUpdateValue = tableWriter.genAttributeValueForGSIKey(options.getGsiHashKeyType(), getNextGsiHashKeyUpdateValue());
        AttributeValue GSIRangeKeyUpdateValue = tableWriter.genAttributeValueForGSIKey(options.getGsiRangeKeyType(), getNextGsiRangeKeyUpdateValue());
        
        // Find if gsi hash key/range key has violations. This will be needed when both hash and range violations
        // are to be found but only one has a violation.
        String gsiHashKeyName = getNextGsiHashKeyViolationType() == null ? null : options.getGsiHashKeyName();
        String gsiRangeKeyName = getNextGsiRangeKeyViolationType() == null ? null : options.getGsiRangeKeyName();
        
        boolean deleteBlank = getNextDeleteBlankAttribute();
        if (deleteBlank) {
            updateItems = genUpdateItemsWithEmptyAttributeDeleted(gsiHashKeyName, GSIHashKeyUpdateValue, gsiRangeKeyName,
                    GSIRangeKeyUpdateValue);
        } else {
            updateItems = genUpdateItemsWithEmptyAttributeKept(gsiHashKeyName, GSIHashKeyUpdateValue, gsiRangeKeyName,
                    GSIRangeKeyUpdateValue);
        }
        return updateItems;
    }
    
    /**
     * Delete the attribute if update value for it is null.
     */
    public Map<String, AttributeValueUpdate> genUpdateItemsWithEmptyAttributeDeleted(String GSIHashKeyName, AttributeValue GSIHashKeyUpdateValue,
            String GSIRangeKeyName, AttributeValue GSIRangeKeyUpdateValue) {
        Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
        
        boolean updateFound = false;
        if (GSIHashKeyName != null) {
            if (GSIHashKeyUpdateValue == null) {
                updateItems.put(GSIHashKeyName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                updateFound = true;
            } else {
                updateItems.put(GSIHashKeyName, new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(GSIHashKeyUpdateValue));
                updateFound = true;
            }
        }

        if (GSIRangeKeyName != null) {
            if (GSIRangeKeyUpdateValue == null) {
                updateItems.put(GSIRangeKeyName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
                updateFound = true;
            } else {
                updateItems.put(GSIRangeKeyName, new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(GSIRangeKeyUpdateValue));
                updateFound = true;
            }
        }
        
        if(updateFound) {
            violationUpdateRequests++;
        }
        return updateItems;
    }

    /**
     * Do nothing to an attribute if update value for it is null.
     */
    public Map<String, AttributeValueUpdate> genUpdateItemsWithEmptyAttributeKept(String GSIHashKeyName, AttributeValue GSIHashKeyUpdateValue,
            String GSIRangeKeyName, AttributeValue GSIRangeKeyUpdateValue) {
        Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
        
        boolean updateFound = false;
        if (GSIHashKeyName != null && GSIHashKeyUpdateValue != null) {
            updateItems.put(GSIHashKeyName, new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(GSIHashKeyUpdateValue));
            updateFound = true;
        }

        if (GSIRangeKeyName != null && GSIRangeKeyUpdateValue != null) {
            updateItems.put(GSIRangeKeyName, new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(GSIRangeKeyUpdateValue));
            updateFound = true;
        }
        
        if(updateFound) {
            violationUpdateRequests++;
        }
        return updateItems;
    }

    /**
     * Put expected items into the map only when it exists and the action is put
     */
    protected Map<String, ExpectedAttributeValue> genExpectedItemsForRecord(Map<String, AttributeValueUpdate> updateItems) {
        Map<String, ExpectedAttributeValue> expectedItems = new HashMap<String, ExpectedAttributeValue>();
        if (null != updateItems.get(options.getGsiHashKeyName())) {
            ExpectedAttributeValue gsiHashKeyExpectedValue = tableWriter.genExpectedAttributeValue(getNextGsiHashKey());
            expectedItems.put(options.getGsiHashKeyName(), gsiHashKeyExpectedValue);
        }
        if (null != updateItems.get(options.getGsiRangeKeyName())) {
            ExpectedAttributeValue gsiRangeKeyExpectedValue = tableWriter.genExpectedAttributeValue(getNextGsiRangeKey());
            expectedItems.put(options.getGsiRangeKeyName(), gsiRangeKeyExpectedValue);
        }
        return expectedItems;
    }

    protected String getNextTableHashKey() throws IllegalArgumentException {
        String nextHashKey = correctionReader.getValueInRecordByName(ViolationRecord.TABLE_HASH_KEY);
        if (null == nextHashKey) {
            throw new IllegalArgumentException("Error: '" + ViolationRecord.TABLE_HASH_KEY + "' not found on input file");
        }
        return nextHashKey;
    }

    protected String getNextTableRangeKey() throws IllegalArgumentException {
        String nextTableRangeKey = correctionReader.getValueInRecordByName(ViolationRecord.TABLE_RANGE_KEY);
        if (null != tableHelper.getTableRangeKeyName() && null == nextTableRangeKey) {
            throw new IllegalArgumentException("Error: Given table has range key, but '" + ViolationRecord.TABLE_RANGE_KEY + "' not found on input file.");
        }
        if (null == tableHelper.getTableRangeKeyName() && null != nextTableRangeKey) {
            throw new IllegalArgumentException("Error: Given table does not have range key, but '" + ViolationRecord.TABLE_RANGE_KEY
                    + "' found on input file. ");
        }
        return nextTableRangeKey;
    }

    protected String getNextGsiHashKey() {
        String nextGsiHashkeyValue = correctionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY);
        if (null != options.getGsiHashKeyName() && null == nextGsiHashkeyValue) {
            throw new IllegalArgumentException("Error: '" + ViolationRecord.GSI_HASH_KEY
                    + "' not found on input file which is required for conditional update.");
        }
        if (null == options.getGsiHashKeyName() && null != nextGsiHashkeyValue) {
            throw new IllegalArgumentException("Error: " + Options.GSI_HASH_KEY_NAME + " not set on property file but '" + ViolationRecord.GSI_HASH_KEY
                    + "' column found on input file.");
        }
        return nextGsiHashkeyValue;
    }

    protected String getNextGsiRangeKey() {
        String nextGsiRangeKeyValue = correctionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY);
        if (null != options.getGsiRangeKeyName() && null == nextGsiRangeKeyValue) {
            throw new IllegalArgumentException("Error: '" + ViolationRecord.GSI_RANGE_KEY
                    + "' not found on input file which is required for conditional update.");
        }
        if (null == options.getGsiRangeKeyName() && null != nextGsiRangeKeyValue) {
            throw new IllegalArgumentException("Error: " + Options.GSI_RANGE_KEY_NAME + " not set on property file but '" + ViolationRecord.GSI_RANGE_KEY
                    + "' column found on input file.");
        }
        return nextGsiRangeKeyValue;
    }

    protected String getNextGsiHashKeyUpdateValue() throws IllegalArgumentException {
        String nextGsiHashKeyUpdateValues = correctionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY_UPDATE_VALUE);
        // It is okay to ignore a random value mentioned even if GsiHashKeyName in options is null
        // It is also okay to ignore the places where GSIHashKeyUpdateValue is not specified as the user might want to keep that value
        return nextGsiHashKeyUpdateValues;
    }
    
    protected String getNextGsiHashKeyViolationType() {
        String nextGsiHashKeyViolationType = correctionReader.getValueInRecordByName(ViolationRecord.GSI_HASH_KEY_VIOLATION_TYPE);
        return nextGsiHashKeyViolationType;
    }
    
    protected String getNextGsiRangeKeyViolationType() {
        String nextGsiRangeKeyViolationType = correctionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY_VIOLATION_TYPE);
        return nextGsiRangeKeyViolationType;
    }

    protected String getNextGsiRangeKeyUpdateValue() throws IllegalArgumentException {
        String nextGsiRangeKeyUpdateValues = correctionReader.getValueInRecordByName(ViolationRecord.GSI_RANGE_KEY_UPDATE_VALUE);
        // It is okay to ignore a random value mentioned even if GsiRangeKeyName in options is null
        // It is also okay to ignore the places where GSIRangeKeyUpdateValue is not specified as the user might want to keep that value
        return nextGsiRangeKeyUpdateValues;
    }

    protected boolean getNextDeleteBlankAttribute() throws IllegalArgumentException {
        String deleteBlankAttributeValues = correctionReader.getValueInRecordByName(ViolationRecord.GSI_CORRECTION_DELETE_BLANK);
        if (deleteBlankAttributeValues == null) {
            return false;
        }
        
        if (deleteBlankAttributeValues.equalsIgnoreCase(ViolationRecord.GSI_CORRECTION_DELETE_BLANK_YES)) {
            return true;
        } else if (deleteBlankAttributeValues.equalsIgnoreCase(ViolationRecord.GSI_CORRECTION_DELETE_BLANK_NO) || deleteBlankAttributeValues.equals("")) {
            return false;
        } else {
            throw new IllegalArgumentException("Error: " + ViolationRecord.GSI_CORRECTION_DELETE_BLANK + " value: '" + deleteBlankAttributeValues
                    + "' invalid.");
        }
    }

    public long getViolationUpdateRequests() {
        return violationUpdateRequests;
    }

    public long getSuccessfulUpdates() {
        return successfulUpdates;
    }

    public Options getOptions() {
        return options;
    }

    public TableHelper getTableHelper() {
        return tableHelper;
    }

    public CorrectionReader getCorrectionReader() {
        return correctionReader;
    }

    public TableWriter getTableWriter() {
        return tableWriter;
    }

    public long getConditionalUpdateFailures() {
        return conditionalUpdateFailures;
    }

    public long getUnexpectedErrors() {
        return unexpectedErrors;
    }
    
}
