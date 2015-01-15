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
import java.nio.charset.Charset;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

/**
 * Check item or attribute violations.
 * 
 */
public class ViolationChecker {

    /** Violation Types */
    public static final String SIZE_VIOLATION = "Size Violation";
    public static final String TYPE_VIOLATION = "Type Violation";

    /** Maximum Values in Bytes */
    public static final int MAX_HASH_KEY_SIZE = 2048;
    public static final int MAX_RANGE_KEY_SIZE = 1024;

    /** Set Type */
    public static final String SS = "SS";
    public static final String NS = "NS";
    public static final String BS = "BS";

    /** Character set for encoding */
    public final static Charset UTF8 = Charset.forName("UTF-8");

    private boolean recordViolation = true;
    private boolean recordGsiValueInViolationRecord = false;
    private boolean tableHasRangeKey = false;
    private boolean checkGSIHashKey = false;
    private boolean checkGSIRangeKey = false;
    private String tableHashkeyName;
    private String tableRangeKeyName;
    private String GSIHashKeyName;
    private String GSIHashKeyType;
    private String GSIRangeKeyName;
    private String GSIRangeKeyType;
    private ViolationRecord violationRecord;
    private boolean isHashKeyViolation = false;
    private boolean isRangeKeyViolation = false;

    /**
     * Constructor for unit tests
     */
    protected ViolationChecker(String outputFilePath, String tableHashKeyName, String tableRangeKeyName, String GSIHashKeyName, String GSIHashKeyType,
            String GSIRangeKeyName, String GSIRangeKeyType, boolean recordViolation, boolean recordGsiValueInViolationRecord, ViolationRecord violationRecord,
            ViolationWriter violationWriter) {
        this.tableHashkeyName = tableHashKeyName;
        this.tableRangeKeyName = tableRangeKeyName;
        if (tableRangeKeyName != null) {
            tableHasRangeKey = true;
        }

        if (GSIHashKeyName != null) {
            this.checkGSIHashKey = true;
            this.GSIHashKeyName = GSIHashKeyName;
            this.GSIHashKeyType = GSIHashKeyType;
        }

        if (GSIRangeKeyName != null) {
            this.checkGSIRangeKey = true;
            this.GSIRangeKeyName = GSIRangeKeyName;
            this.GSIRangeKeyType = GSIRangeKeyType;
        }

        this.recordViolation = recordViolation;
        this.recordGsiValueInViolationRecord = recordGsiValueInViolationRecord;
        this.violationRecord = violationRecord;
    }

    public ViolationChecker(Options options, TableHelper tableHelper) {
        this.tableHashkeyName = tableHelper.getTableHashKeyName();
        this.tableRangeKeyName = tableHelper.getTableRangeKeyName();
        if (tableRangeKeyName != null) {
            tableHasRangeKey = true;
        }

        this.GSIHashKeyName = options.getGsiHashKeyName();
        if (GSIHashKeyName != null) {
            this.checkGSIHashKey = true;
            this.GSIHashKeyName = options.getGsiHashKeyName();
            this.GSIHashKeyType = options.getGsiHashKeyType();
        }

        this.GSIRangeKeyName = options.getGsiRangeKeyName();
        if (GSIRangeKeyName != null) {
            this.checkGSIRangeKey = true;
            this.GSIRangeKeyName = options.getGsiRangeKeyName();
            this.GSIRangeKeyType = options.getGsiRangeKeyType();
        }

        this.recordViolation = options.recordDetails();
        this.recordGsiValueInViolationRecord = options.recordGsiValueInViolationRecord();
        if (recordViolation) {
            violationRecord = new ViolationRecord(tableHasRangeKey, checkGSIHashKey, checkGSIRangeKey, recordGsiValueInViolationRecord);
        }
    }

    public ViolationRecord getViolationRecordHead() throws IOException {
        return violationRecord.getViolationRecordHead();
    }

    public ViolationRecord checkItemViolationAndGetRecord(Map<String, AttributeValue> item) {
        isHashKeyViolation = false;
        isRangeKeyViolation = false;

        if (recordViolation) {
            violationRecord.clear();
        }

        if (checkGSIHashKey) {
            AttributeValue GSIHashKeyValue = item.get(GSIHashKeyName);
            /** If that value does not exist, ignore */
            if (GSIHashKeyValue != null) {
                isHashKeyViolation = checkAttributeViolation(GSIHashKeyValue, GSIHashKeyType, KeyType.HASH);
            }
        }

        if (checkGSIRangeKey) {
            AttributeValue GSIRangeKeyValue = item.get(GSIRangeKeyName);
            if (GSIRangeKeyValue != null) {
                isRangeKeyViolation = checkAttributeViolation(GSIRangeKeyValue, GSIRangeKeyType, KeyType.RANGE);
            }
        }

        if (recordViolation && (isHashKeyViolation || isRangeKeyViolation)) {
            AttributeValue tableHashKeyValue = item.get(tableHashkeyName);
            AttributeValue tableRangeKeyValue = tableHasRangeKey ? item.get(tableRangeKeyName) : null;
            recordItemTablePrimaryKey(tableHashKeyValue, tableRangeKeyValue);
        }

        if (isHashKeyViolation || isRangeKeyViolation) {
            if(recordViolation) {
                return violationRecord;
            } else {
                // return any non-null violation record because null would mean that there is no violation. 
                // Since we are not recording violations, this value will not be used.
                return new ViolationRecord(tableHasRangeKey, checkGSIHashKey, checkGSIRangeKey, recordGsiValueInViolationRecord);
            }
        }
        return null;
    }

    protected boolean checkAttributeViolation(AttributeValue keyValue, String expectedDatatype, KeyType keyType) {
        int maxKeySize = keyType.name().equals(KeyType.HASH.name()) ? MAX_HASH_KEY_SIZE : MAX_RANGE_KEY_SIZE;
        if (keyValue.getS() != null) {
            if (!ScalarAttributeType.S.name().equals(expectedDatatype)) {
                if (recordViolation) {
                    recordTypeViolation(keyValue, keyType, expectedDatatype, ScalarAttributeType.S.name());
                }
                return true;
            } else {
                int size = keyValue.getS().getBytes(UTF8).length;
                if (size > maxKeySize) {
                    if (recordViolation) {
                        recordSizeViolation(keyValue, size, keyType);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        } else if (keyValue.getN() != null) {
            if (!ScalarAttributeType.N.name().equals(expectedDatatype)) {
                if (recordViolation) {
                    recordTypeViolation(keyValue, keyType, expectedDatatype, ScalarAttributeType.N.name());
                }
                return true;
            } else {
                // There can be no size violation for Number datatype
                return false;
            }
        } else if (keyValue.getB() != null) {
            if (!ScalarAttributeType.B.name().equals(expectedDatatype)) {
                if (recordViolation) {
                    recordTypeViolation(keyValue, keyType, expectedDatatype, ScalarAttributeType.B.name());
                }
                return true;
            } else {
                int size = keyValue.getB().array().length;
                if (size > maxKeySize) {
                    if (recordViolation) {
                        recordSizeViolation(keyValue, size, keyType);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        } else if (keyValue.getSS() != null) {
            if (recordViolation) {
                recordTypeViolation(keyValue, keyType, expectedDatatype, SS);
            }
            return true;
        } else if (keyValue.getNS() != null) {
            if (recordViolation) {
                recordTypeViolation(keyValue, keyType, expectedDatatype, NS);
            }
            return true;
        } else if (keyValue.getBS() != null) {
            if (recordViolation) {
                recordTypeViolation(keyValue, keyType, expectedDatatype, BS);
            }
            return true;
        }
        return false;
    }

    /** For GSI Violation value, should store them with their attribute type */
    protected void recordSizeViolation(AttributeValue keyValue, int size, KeyType keyType) {
        if (keyType == KeyType.HASH) {
            if (recordGsiValueInViolationRecord) {
                violationRecord.setGSIHashKey(AttributeValueConverter.toStringWithAttributeType(keyValue));
            }
            violationRecord.setGSIHashKeyViolationType(SIZE_VIOLATION);
            violationRecord.setGSIHashKeyViolationDesc("Max Bytes Allowed: " + MAX_HASH_KEY_SIZE + " Found: " + size);
        } else if (keyType == KeyType.RANGE) {
            if (recordGsiValueInViolationRecord) {
                violationRecord.setGSIRangeKey(AttributeValueConverter.toStringWithAttributeType(keyValue));
            }
            violationRecord.setGSIRangeKeyViolationType(SIZE_VIOLATION);
            violationRecord.setGSIRangeKeyViolationDesc("Max Bytes Allowed: " + MAX_RANGE_KEY_SIZE + " Found: " + size);
        }
    }

    protected void recordTypeViolation(AttributeValue keyValue, KeyType keyType, String expectedDatatype, String foundDatatype) {
        if (keyType == KeyType.HASH) {
            if (recordGsiValueInViolationRecord) {
                violationRecord.setGSIHashKey(AttributeValueConverter.toStringWithAttributeType(keyValue));
            }
            violationRecord.setGSIHashKeyViolationType(TYPE_VIOLATION);
            violationRecord.setGSIHashKeyViolationDesc("Expected: " + expectedDatatype + " Found: " + foundDatatype);
        } else {
            if (recordGsiValueInViolationRecord) {
                violationRecord.setGSIRangeKey(AttributeValueConverter.toStringWithAttributeType(keyValue));
            }
            violationRecord.setGSIRangeKeyViolationType(TYPE_VIOLATION);
            violationRecord.setGSIRangeKeyViolationDesc("Expected: " + expectedDatatype + " Found: " + foundDatatype);
        }
    }

    public void recordItemTablePrimaryKey(AttributeValue itemHashKey, AttributeValue itemRangeKey) {
        recordItemTableHashKey(itemHashKey);
        recordItemTableRangeKey(itemRangeKey);
    }

    /** For table hash key or range key, store them as blank strings */
    protected void recordItemTableHashKey(AttributeValue itemHashKey) {
        if (itemHashKey.getS() == null && itemHashKey.getN() == null && itemHashKey.getB() == null) {
            throw new IllegalArgumentException("Error: Invalid hash key, should contains S, N or B only.");
        }
        violationRecord.setTableHashKey(AttributeValueConverter.toBlankString(itemHashKey));
    }

    protected void recordItemTableRangeKey(AttributeValue itemRangeKey) {
        if (tableHasRangeKey == false) {
            if (itemRangeKey != null) {
                throw new IllegalArgumentException("Error: Table does not have range key but was provided.");
            }
            return;
        }
        if (itemRangeKey.getS() == null && itemRangeKey.getN() == null && itemRangeKey.getB() == null) {
            throw new IllegalArgumentException("Error: Invalid range key, should contains S, N or B only.");
        }
        violationRecord.setTableRangeKey(AttributeValueConverter.toBlankString(itemRangeKey));
    }
}
