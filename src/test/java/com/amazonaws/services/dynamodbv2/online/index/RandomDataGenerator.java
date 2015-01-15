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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 
 * Generator random data.
 * 
 */
public class RandomDataGenerator {
    private static char[] symbols;
    private final Random random = new Random();

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch < '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        symbols = tmp.toString().toCharArray();
    }

    /**
     * 
     * Generate next random string
     * 
     */
    public String nextRadomString(int length) {
        char[] buf = new char[length];
        for (int index = 0; index < buf.length; ++index)
            buf[index] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }

    /**
     * 
     * Generate next random string array
     * 
     */
    public List<String> nextRandomStringArray(int stringLength) {
        int arrayLength = random.nextInt(10) + 1;
        return this.nextRandomStringArray(stringLength, arrayLength);
    }

    public List<String> nextRandomStringArray(int stringLength, int arrayLength) {
        List<String> array = new ArrayList<String>();
        for (int i = 0; i < arrayLength; i++) {
            array.add(this.nextRadomString(stringLength));
        }
        return array;
    }

    /**
     * 
     * Return random array of integer
     * 
     */
    public List<String> nextRandomIntArray() {
        List<String> array = new ArrayList<String>();
        int arrayLength = random.nextInt(10) + 1;
        for (int i = 0; i < arrayLength; i++) {
            array.add(Integer.toString(this.nextRandomInt()));
        }
        return array;
    }

    /**
     * 
     * Return next random binary array
     * 
     */
    public List<ByteBuffer> nextRandomBinaryArray(int binaryLength) {
        List<ByteBuffer> array = new ArrayList<ByteBuffer>();
        int arrayLength = random.nextInt(10) + 1;
        for (int i = 0; i < arrayLength; i++) {
            array.add(this.nextRandomBinary(binaryLength));
        }
        return array;
    }

    /**
     * 
     * Generate next binary buffer
     * 
     */
    public ByteBuffer nextRandomBinary(int length) {
        char[] buf = new char[length];
        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        for (int index = 0; index < buf.length; ++index) {
            byte tmp = (byte) symbols[random.nextInt(symbols.length)];
            byteBuffer.put(tmp);
        }
        /** Should reset the position */
        byteBuffer.position(0);
        return byteBuffer;
    }

    /**
     * 
     * Generate next random integer
     * 
     */
    public int nextRandomInt() {
        return random.nextInt();
    }

    /**
     * 
     * Generate next random integer within limit
     * 
     */
    public int nextRandomInt(int limit) {
        return random.nextInt(limit);
    }
}
