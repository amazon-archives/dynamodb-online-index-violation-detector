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

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

/**
 * Check if given option value is valid.
 * 
 */
public class OptionChecker {
    private final String S3_PATH_PREFIX = "s3://";

    public boolean isValidKeyType(String keyType) {
        if (!ScalarAttributeType.B.name().equals(keyType) && !ScalarAttributeType.S.name().equals(keyType) && !ScalarAttributeType.N.name().equals(keyType)) {
            return false;
        }
        return true;
    }

    public boolean isS3Path(String path) {
        return path.startsWith(S3_PATH_PREFIX) ? true : false;
    }

    public boolean isNumberInRange(int number, int lowerBound, int upperBound) {
        if (number > upperBound || number < lowerBound) {
            return false;
        }
        return true;
    }
}
