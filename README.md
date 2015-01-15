## Overview
For an overview of of the DynamoDB Online Index Violation Detector Tool, please refer to the [AWS DynamoDB Documentation](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.ViolationDetection.html).

## Minimum Requirements
- Java 1.7+
- Maven

## Getting Started
To download the code and to build and create a runnable jar, use the following commands:
```
git clone https://github.com/awslabs/dynamodb-online-index-violation-detector.git
cd dynamodb-online-index-violation-detector
mvn package
```
This will generate a jar named ViolationDetector.java in the dynamodb-online-index-violation-detector directory.

## Usage
You can run the generated jar named ViolationDetector.java to start detecting and correcting violations on any DynamoDB table.
```
java -jar ViolationDetector.java [options]
```
### Available options:
- -p,--configFilePath \<configFilePath\>
  - Path of the config file. This option is required for both detection and correction. Refer to the sample [config.properties](https://github.com/awslabs/dynamodb-online-index-violation-detector/tree/master/config/config.properties) file.
- -t,--detect \<keep/delete\>
  - Detect violations on given table. With 'keep', violations will be kept and recorded. With 'delete', violations will be deleted and recorded.
- -c,--correct \<update/delete\>
  - Correct violations based on records on correction input file. With 'delete', records on input file will be deleted from the table. With 'update', records on input file will be updated to the table.
- -h,--help
  - Help and usage information

For detailed instructions, refer to the [AWS DynamoDB Documentation](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.ViolationDetection.html).

## Running Integration Tests
In order to run integration tests, you will have to start DynamoDB Local server on port 8000. For downloading and starting DynamoDB Local, refer to the instructions given in the [AWS DynamoDB Local Documentation](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html).
Once you have the server running, run the following command to start integration tests:
```
mvn integration-test
```
