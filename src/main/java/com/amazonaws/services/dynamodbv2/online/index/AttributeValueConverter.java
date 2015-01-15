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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parse attribute values of DynamoDB.
 */
public class AttributeValueConverter {
    private static final Charset charset = Charset.forName("UTF-8");
    private static CharsetDecoder decoder = charset.newDecoder();
    private static CharsetEncoder encoder = charset.newEncoder();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static AttributeValue parseFromWithAttributeTypeString(String value) throws IllegalArgumentException {
        try {
            Map<String, String> valueMap = mapper.readValue(value, new TypeReference<Map<String, String>>(){});
            AttributeValue attributeValue = new AttributeValue();
            if (null != valueMap.get("N")) {
                attributeValue.withN(valueMap.get("N"));
            } else if (null != valueMap.get("S")) {
                attributeValue.withS(valueMap.get("S"));
            } else if (null != valueMap.get("B")) {
                attributeValue.withB(encoder.encode(CharBuffer.wrap(valueMap.get("B"))));
            } else if (null != valueMap.get("NS")) {
                List<String> numberSet = mapper.readValue(valueMap.get("NS"), new TypeReference<List<String>>(){});
                attributeValue.withNS(numberSet);
            } else if (null != valueMap.get("SS")) {
                List<String> stringSet = mapper.readValue(valueMap.get("SS"), new TypeReference<List<String>>(){});
                attributeValue.withSS(stringSet);
            } else if (null != valueMap.get("BS")) {
                List<String> binaryStringSet = mapper.readValue(valueMap.get("BS"), new TypeReference<List<String>>(){});
                List<ByteBuffer> bianrySet = new ArrayList<ByteBuffer>();
                for (String bss : binaryStringSet) {
                    bianrySet.add(encoder.encode(CharBuffer.wrap(bss)));
                }
                attributeValue.withBS(bianrySet);
            } else {
                throw new IllegalArgumentException("Error: Invalid Attribute Value Type. ");
            }
            return attributeValue;
        } catch (CharacterCodingException cce) {
            throw new IllegalArgumentException("Error: Failed to encode binary into string.");
        } catch (IOException e) {
            throw new IllegalArgumentException("Error: Failed to parse set from string.");
        }
    }
    
    public static AttributeValue parseFromBlankString(String attribtueType, String value) throws IllegalArgumentException {
        try {
            AttributeValue attributeValue = new AttributeValue();
            if (attribtueType.equals("N")) {
                attributeValue.withN(value);
            } else if (attribtueType.equals("S")) {
                attributeValue.withS(value);
            } else if (attribtueType.equals("B")) {
                attributeValue.withB(encoder.encode(CharBuffer.wrap(value)));
            } else if (attribtueType.equals("NS")) {
                List<String> numberSet = mapper.readValue(value, new TypeReference<List<String>>(){});
                attributeValue.withNS(numberSet);
            } else if (attribtueType.equals("SS")) {
                List<String> stringSet = mapper.readValue(value, new TypeReference<List<String>>(){});
                attributeValue.withSS(stringSet);
            } else if (attribtueType.equals("BS")) {
                List<String> binaryStringSet = mapper.readValue(value, new TypeReference<List<String>>(){});
                List<ByteBuffer> bianrySet = new ArrayList<ByteBuffer>();
                for (String bss : binaryStringSet) {
                    bianrySet.add(encoder.encode(CharBuffer.wrap(bss)));
                }
                attributeValue.withBS(bianrySet);
            } else {
                throw new IllegalArgumentException("Error: Invalid Attribute Value Type. ");
            }
            return attributeValue;
        } catch (CharacterCodingException cce) {
            throw new IllegalArgumentException("Error: Failed to encode binary into string.");
        } catch (IOException e) {
            throw new IllegalArgumentException("Error: Failed to parse set from string.");
        }
    }
    public static String toStringWithAttributeType(AttributeValue attributeValue) throws IllegalArgumentException {
        try {
            Map<String, String> valueMap = new HashMap<String, String>();
            if (null != attributeValue.getS()) {
                valueMap.put("S",attributeValue.getS());
            }
            if (null != attributeValue.getN()) {
                valueMap.put("N", attributeValue.getN());
            }
            if (null != attributeValue.getB()) {
                valueMap.put("B", decoder.decode(attributeValue.getB()).toString());
            }
            if (null != attributeValue.getNS()) {
                valueMap.put("NS", mapper.writeValueAsString(attributeValue.getNS()));
            }
            if (null != attributeValue.getSS()) {
                valueMap.put("SS", mapper.writeValueAsString(attributeValue.getSS()));
            }
            if (null != attributeValue.getBS()) {
                List<String> binaryList = new ArrayList<String>();
                for (ByteBuffer bb : attributeValue.getBS()) {
                    binaryList.add(decoder.decode(bb).toString());
                }
                valueMap.put("BS", mapper.writeValueAsString(binaryList));
            }
            return mapper.writeValueAsString(valueMap);
        } catch (CharacterCodingException cce) {
            throw new IllegalArgumentException("Error: Failed to decode binary from string.");
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error: Failed to convert set into string.");
        }
    }
    
    public static String toBlankString(AttributeValue attributeValue) throws IllegalArgumentException {
        try {
            String value = null;
            if (null != attributeValue.getS()) {
                value = attributeValue.getS();
            }
            if (null != attributeValue.getN()) {
                value = attributeValue.getN();
            }
            if (null != attributeValue.getB()) {
                value = decoder.decode(attributeValue.getB()).toString();
            }
            if (null != attributeValue.getNS()) {
                value = mapper.writeValueAsString(attributeValue.getNS());
            }
            if (null != attributeValue.getSS()) {
                value = mapper.writeValueAsString(attributeValue.getSS());
            }
            if (null != attributeValue.getBS()) {
                List<String> binaryList = new ArrayList<String>();
                for (ByteBuffer bb : attributeValue.getBS()) {
                    binaryList.add(decoder.decode(bb).toString());
                }
                value = mapper.writeValueAsString(binaryList);
            }
            return value;
        } catch (CharacterCodingException cce) {
            throw new IllegalArgumentException("Error: Failed to decode binary from string.");
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error: Failed to convert set into string.");
        }
    }

}
