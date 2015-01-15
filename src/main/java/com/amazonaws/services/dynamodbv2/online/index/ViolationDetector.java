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

import java.io.File;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * Entrance for violation detection and correction.
 * 
 */
public class ViolationDetector {
    private Options options;
    private OptionLoader optionLoader;
    private AWSConnection awsConnection;
    private TableHelper tableHelper;
    private TableReader tableReader;
    private Correction correction;
    
    private static final Logger logger = Logger.getLogger(ViolationDetector.class);
    
    // Used for running tests on DDB Local.
    private boolean runOnDDBLocal = false;
    
    // Command line usage
    private static final String TOOL_USAGE = "\nDetection:  java -jar ViolationDetector.jar -p <config-file-path> -t <keep/delete>\n" +
            "Correction: java -jar ViolationDetector.jar -p <config-file-path> -c <update/delete>";
    private static final int TOOL_USAGE_WIDTH = 150;

    /**
     * Constructor for unit test purpose only.
     */
    public ViolationDetector(Options options, OptionLoader optionLoader, AWSConnection awsConnection, TableHelper tableHelper, TableReader tableReader,
            Correction correction, boolean runOnDDBLocal) {
        this.options = options;
        this.optionLoader = optionLoader;
        this.awsConnection = awsConnection;
        this.tableHelper = tableHelper;
        this.tableReader = tableReader;
        this.correction = correction;
        this.runOnDDBLocal = runOnDDBLocal;
    }

    public ViolationDetector() {
    }

    private void setConfigFile(String configFile) {
        try {
            optionLoader = new OptionLoader(configFile);
        } catch (IllegalArgumentException iae) {
            logger.error("Exception!", iae);
            iae.printStackTrace();
            System.exit(1);
        }
    }

    public void initDetection() {
        try {
            optionLoader.loadDetectionOptions();
            options = optionLoader.getOptions();
            awsConnection = new AWSConnection(options.getCredentialsFilePath());
        } catch (Exception e) {
            logger.error("Exception!", e);
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Used only for testing
     * Output array sequence:
     * [0] - itemsScanned
     * [1] - violations found
     * [2] - violations deleted
     */
    public int[] getViolationDectectionOutput() {
        int[] detectionOutputValues = {(int)tableReader.getItemsScanned(), 
                                    (int)tableReader.getViolationsFound(), 
                                    (int)tableReader.getViolationsDeleted()};
        return detectionOutputValues;
    }

    public void violationDetection(boolean delete) {
        try {
            AmazonDynamoDBClient dynamoDBClient = awsConnection.getDynamoDBClient(options.getDynamoDBRegion(), runOnDDBLocal);
            tableHelper = new TableHelper(dynamoDBClient, options.getTableName());
            tableReader = new TableReader(options, dynamoDBClient, tableHelper, runOnDDBLocal);
            validateKeyNames();
            tableReader.scanTable(delete);
        } catch (Exception e) {
            logger.error("Exception!", e);
            e.printStackTrace();
            System.exit(1);
        }

        if (options.isDetectionOutputS3Path()) {
            putOutputFileToS3(options.getDetectionOutputPath(), options.getTmpDetectionOutputPath());
        }
    }

    /**
     * Ensure that gsiHashKeyName and gsiRangeKeyName is not equal to table hash key name or table range key name
     */
    private void validateKeyNames() {
        // gsiHashKeyName should not be equal to table hash or table range
        if(options.getGsiHashKeyName() != null) {
            if(tableHelper.getTableHashKeyName().equals(options.getGsiHashKeyName())) {
                String errorStr = "Error: " + Options.GSI_HASH_KEY_NAME + " cannot be equal to table hash key name";
                logger.error(errorStr);
                System.exit(1);
            }
            
            if(tableHelper.getTableRangeKeyName() != null && tableHelper.getTableRangeKeyName().equals(options.getGsiHashKeyName())) {
                String errorStr = "Error: " + Options.GSI_HASH_KEY_NAME + " cannot be equal to table range key name";
                logger.error(errorStr);
                System.exit(1);
            }
        }
        
        // gsiRangeKeyName should not be equal to table hash or table range
        if(options.getGsiRangeKeyName() != null) {
            if(tableHelper.getTableHashKeyName().equals(options.getGsiRangeKeyName())) {
                String errorStr = "Error: " + Options.GSI_RANGE_KEY_NAME + " cannot be equal to table hash key name";
                logger.error(errorStr);
                System.exit(1);
            }
            if(tableHelper.getTableRangeKeyName() != null && tableHelper.getTableRangeKeyName().equals(options.getGsiRangeKeyName())) {
                String errorStr = "Error: " + Options.GSI_RANGE_KEY_NAME + " cannot be equal to table range key name";
                logger.error(errorStr);
                System.exit(1);
            }
        }
    }

    protected void putOutputFileToS3(String s3Path, String tmpPath) {
        try {
            AmazonS3Client s3Client = awsConnection.getS3Client();
            File tmpOutputFile = new File(tmpPath);
            s3Client.putObject(options.getS3PathBucketName(s3Path), options.getS3PathKey(s3Path), tmpOutputFile);
        } catch (Exception e) {
            String errorStr = "Error: " + e.getMessage() + ", failed to put output file into S3 path " + s3Path
                    + ", temporary output file can be found here: " + tmpPath;
            logger.error(errorStr);
            System.exit(1);
        }
    }

    public void initCorrection() {
        try {
            optionLoader.loadCorrectionOptions();
            options = optionLoader.getOptions();
            awsConnection = new AWSConnection(options.getCredentialsFilePath());
        } catch (Exception e) {
            logger.error("Exception!", e);
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Used only for testing
     * Output array sequence:
     * [0] - items successfully updated
     * [1] - violation update requests
     * [2] - conditionalUpdateFailures
     * [3] - unexpectedErrors
     */
    public int[] getViolationCorrectionOutput() {
        int[] detectionOutputValues = {(int)correction.getSuccessfulUpdates(), 
                                    (int)correction.getViolationUpdateRequests(), 
                                    (int)correction.getConditionalUpdateFailures(),
                                    (int)correction.getUnexpectedErrors()};
        return detectionOutputValues;
    }

    public void violationCorrection(boolean delete, boolean useConditionalUpdate) {
        /** Get file from S3 to temporary correction file before processing */
        if (options.isCorrectionInputS3Path()) {
            downloadInputFileToLocal();
        }

        try {
            AmazonDynamoDBClient dynamoDBClient = awsConnection.getDynamoDBClient(options.getDynamoDBRegion(), runOnDDBLocal);
            tableHelper = new TableHelper(dynamoDBClient, options.getTableName());
            validateKeyNames();
            correction = new Correction(options, tableHelper, dynamoDBClient, runOnDDBLocal);
            if (delete) {
                correction.deleteFromFile();
            } else {
                boolean violationOutputGenerated = correction.updateFromFile(useConditionalUpdate);
                if(violationOutputGenerated && options.isCorrectionOutputS3Path()) {
                    putOutputFileToS3(options.getCorrectionOutputPath(), options.getTmpCorrectionOutputPath());
                }
            }
        } catch (Exception e) {
            logger.error("Exception!", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected void downloadInputFileToLocal() {
        try {
            AmazonS3Client s3Client = awsConnection.getS3Client();
            s3Client.getObject(new GetObjectRequest(options.getS3PathBucketName(options.getCorrectionInputPath()), 
                    options.getS3PathKey(options.getCorrectionInputPath())),
                    new File(options.getTmpCorrectionInputPath()));
        } catch (AmazonServiceException ase) {
            String errorStr = "Error: Failed to download given file from S3 path: " + options.getCorrectionInputPath() + " , please check your path.";
            logger.error(errorStr);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Exception!", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void confirmDelete() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Are you sure to delete all violations on the table? y/n ");
        String op = scanner.nextLine().trim();
        if (op.equalsIgnoreCase("n")) {
            logger.info("User decided to not delete. Exiting...");
            scanner.close();
            System.exit(0);
        } else if (op.equalsIgnoreCase("y")) {
            logger.info("Confirmed deletion. Will delete all violations in the table...");
            scanner.close();
        } else {
            logger.error("Invalid entry by the user. Will exit.");
            System.out.println("Please type in y/n, exiting...");
            scanner.close();
            System.exit(0);
        }
    }
    
    private static boolean getUseConditionalUpdateOptionFromConsole() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Do you want to use conditional update? y/n ");
        String op = scanner.nextLine().trim();
        if (op.equalsIgnoreCase("n")) {
            logger.info("User decided to NOT use conditional update. Will use normal update.");
            scanner.close();
            return false;
        } else if (op.equalsIgnoreCase("y")) {
            logger.info("User decided to use conditional update.");
            scanner.close();
            return true;
        } else {
            logger.error("Invalid entry by the user. Will exit.");
            System.out.println("Please type in y/n, exiting...");
            scanner.close();
            System.exit(0);
        }
        return false;
    }

    public static void main(String[] args) {
        CommandLine commandLine;
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();

        Option optionHelp = new Option("h", "help", false, "Help and usage information");

        OptionBuilder.withArgName("keep/delete");
        OptionBuilder.withLongOpt("detect");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Detect violations on given table. " + "\nwith 'keep', violations will be kept and recorded;"
                + "\nwith 'delete', violations will be deleted and recorded.");
        Option optionDetection = OptionBuilder.create("t");

        OptionBuilder.withArgName("update/delete");
        OptionBuilder.withLongOpt("correct");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Correct violations based on records on correction input file."
                + "\nwith 'delete', records on input file will be deleted from the table;"
                + "\nwith 'update', records on input file will be updated to the table.");
        Option optionCorrection = OptionBuilder.create("c");

        OptionBuilder.withArgName("configFilePath");
        OptionBuilder.withLongOpt("configFilePath");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Path of the config file. \nThis option is required for both detection and correction.");
        Option optionConfigFilePath = OptionBuilder.create("p");

        options.addOption(optionConfigFilePath);
        options.addOption(optionDetection);
        options.addOption(optionCorrection);
        options.addOption(optionHelp);

        try {
            ViolationDetector detector = new ViolationDetector();
            commandLine = parser.parse(options, args);

            /** Violation detection */
            if (commandLine.hasOption("t")) {
                if(!commandLine.hasOption("p")) {
                    logger.error("Config file path not provided. Exiting...");
                    formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
                    System.exit(1);
                }
                String configFilePath = commandLine.getOptionValue("p");
                detector.setConfigFile(configFilePath);
                
                String detectOption = commandLine.getOptionValue("t");
                if (detectOption.compareTo("delete") == 0) {
                    confirmDelete();
                    detector.initDetection();
                    detector.violationDetection(true);
                } else if (detectOption.compareTo("keep") == 0) {
                    detector.initDetection();
                    detector.violationDetection(false);
                } else {
                    String errMessage = "Invalid options " + detectOption + " for 't/detect'";
                    logger.error(errMessage + ". Exiting...");
                    formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
                    System.exit(1);
                }
                return;
            }

            /** Violation correction */
            if (commandLine.hasOption("c")) {
                if(!commandLine.hasOption("p")) {
                    logger.error("Config file path not provided. Exiting...");
                    formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
                    System.exit(1);
                }
                String configFilePath = commandLine.getOptionValue("p");
                detector.setConfigFile(configFilePath);
                
                String correctOption = commandLine.getOptionValue("c");
                if (correctOption.compareTo("delete") == 0) {
                    confirmDelete();
                    detector.initCorrection();
                    detector.violationCorrection(true, false /* useConditionalUpdate */);
                } else if (correctOption.compareTo("update") == 0) {
                    detector.initCorrection();
                    boolean useConditionalUpdate = getUseConditionalUpdateOptionFromConsole();
                    detector.violationCorrection(false, useConditionalUpdate);
                } else {
                    String errMessage = "Invalid options " + correctOption + " for 'c/correct'";
                    logger.error(errMessage + ". Exiting...");
                    formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
                    System.exit(1);
                }
                return;
            }

            /** Help information */
            if (commandLine.hasOption("h")) {
                formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
                return;
            }

            /** Error: print usage and exit */
            String errMessage = "Invalid options, check usage";
            logger.error(errMessage + ". Exiting...");
            formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
            System.exit(1);

        } catch (IllegalArgumentException iae) {
            logger.error("Exception!", iae);
            System.exit(1);
        } catch (ParseException e) {
            logger.error("Exception!", e);
            formatter.printHelp(TOOL_USAGE_WIDTH, TOOL_USAGE, null /*header*/, options, null /*footer*/);
            System.exit(1);
        }
    }
}
