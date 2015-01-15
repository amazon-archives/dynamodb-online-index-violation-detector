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

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.online.index.OptionChecker;

/**
 * 
 * Unit tests for OptionChecker.
 * 
 */
public class OptionCheckerTest {
    RandomDataGenerator randDataGenerator = new RandomDataGenerator();
    OptionChecker optionChecker = new OptionChecker();

    @Test
    public void testIsValidKeyTypeWithValidKeyType() {
        assertTrue(optionChecker.isValidKeyType("S"));
        assertTrue(optionChecker.isValidKeyType("N"));
        assertTrue(optionChecker.isValidKeyType("B"));
    }
    
    @Test
    public void testIsValidKeyTypeWithInvalidKeyType(){
        assertFalse(optionChecker.isValidKeyType("NS"));
    }

    @Test
    public void testIsS3PathWithValidS3Path() {
        assertTrue(optionChecker.isS3Path("s3://fads"));
    }

    @Test
    public void testIsS3PathWithInvalidS3Path(){
        assertFalse(optionChecker.isS3Path("fadsfasd"));
    }

    @Test
    public void testCheckNumberRangeWithInRange() {
        int lowerBound = Math.abs(randDataGenerator.nextRandomInt(1000)) + 1;
        int number = 2 * lowerBound;
        int higherBound = 3 * lowerBound;
        assertTrue(optionChecker.isNumberInRange(number, lowerBound, higherBound));
    }
    
    @Test
    public void testCheckNumberRangeBeyondRange(){
        int lowerBound = Math.abs(randDataGenerator.nextRandomInt(1000)) + 1;
        int number = 3 * lowerBound;
        int higherBound = 2 * lowerBound;
        assertFalse(optionChecker.isNumberInRange(number, lowerBound, higherBound));
    }
}
