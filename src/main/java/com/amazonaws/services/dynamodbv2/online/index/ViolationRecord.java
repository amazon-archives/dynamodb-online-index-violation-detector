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

import java.util.ArrayList;
import java.util.List;

/**
 * Define the format of violation record.
 * 
 */
public class ViolationRecord {

    /** Head for output records */
    public static final String TABLE_HASH_KEY = "Table Hash Key";
    public static final String TABLE_RANGE_KEY = "Table Range Key";
    public static final String GSI_HASH_KEY = "GSI Hash Key Value";
    public static final String GSI_HASH_KEY_VIOLATION_TYPE = "GSI Hash Key Violation Type";
    public static final String GSI_HASH_KEY_VIOLATION_DESC = "GSI Hash Key Violation Description";
    public static final String GSI_HASH_KEY_UPDATE_VALUE = "GSI Hash Key Update Value(FOR USER)";
    public static final String GSI_RANGE_KEY = "GSI Range Key Value";
    public static final String GSI_RANGE_KEY_VIOLATION_TYPE = "GSI Range Key Violation Type";
    public static final String GSI_RANGE_KEY_VIOLATION_DESC = "GSI Range Key Violation Description";
    public static final String GSI_RANGE_KEY_UPDATE_VALUE = "GSI Range Key Update Value(FOR USER)";
    public static final String GSI_CORRECTION_DELETE_BLANK = "Delete Blank Attributes When Updating?(Y/N)";
    public static final String GSI_CORRECTION_DELETE_BLANK_YES = "Y";
    public static final String GSI_CORRECTION_DELETE_BLANK_NO = "N";
    
    // This option is used only for violation correction output
    public static final String GSI_VALUE_UPDATE_ERROR = "Error While Updating Value";

    private String tableHashKey = null;
    private String tableRangeKey = null;
    private String GSIHashKeyViolationType = null;
    private String GSIHashKey = null;
    private String GSIHashKeyViolationDesc = null;
    private String GSIHashKeyUpdateValue = "";
    private String GSIRangeKeyViolationType = null;
    private String GSIRangeKey = null;
    private String GSIRangeKeyViolationDesc = null;
    private String GSIRangekeyUpdateValue = "";
    private String GSIDeleteBlankOnUpdate = "";

    /** Error messages */
    private final String ERROR_CHECK_NO_GSI_KEY = "Error: Should check one GSI Key at least.";
    private final String ERROR_SET_TABLE_RANGE_KEY = "Error: tableHasRangeKey false, cannot set table range key";
    private final String ERROR_SET_GSI_HASH_KEY = "Error: checkGSIHashKey false, cannot set GSI hash key";
    private final String ERROR_SET_GSI_HASH_KEY_VIO_TYPE = "Error: checkGSIHashKey false, cannot set GSI hash key violation type";
    private final String ERROR_SET_GSI_HASH_KEY_VIO_DESC = "Error: checkGSIHashKey false, cannot set GSI hash key violation description";
    private final String ERROR_SET_GSI_HASH_KEY_UPDATE = "Error: checkGSIHashKey false, cannot set GSI hash key update value";
    private final String ERROR_SET_GSI_RANGE_KEY = "Error: checkGSIRangeKey false, cannot set GSI range key";
    private final String ERROR_MSG_SET_GSI_RANGE_KEY_VIO_TYPE = "Error: checkGSIRangeKey false, cannot set GSI range key violation type";
    private final String ERROR_MSG_SET_GSI_RANGE_KEY_VIO_DESC = "Error: checkGSIRangeKey false, cannot set GSI range violation description";
    private final String ERROR_MSG_GSI_RANGE_KEY_UPDATE = "Error: checkGSIRangeKey false, cannot set GSI range key update value";

    private boolean tableHasRangeKey = false;
    private boolean checkGSIHashKey = false;
    private boolean checkGSIRangeKey = false;
    private boolean recordGsiValueInViolationRecord = false;
    private boolean GSIHashKeyViolationRecorded = false;
    private boolean GSIRangeKeyViolationRecorded = false;

    /**
     * User can choose to check GSI hash key or GSI range key independently by
     * enabling or disabling the two options.
     */
    public ViolationRecord(boolean tableHasRangeKey, boolean checkGSIHashKey, boolean checkGSIRangeKey, boolean recordGsiValueInViolationRecord) {
        if (checkGSIHashKey == false && checkGSIRangeKey == false) {
            throw new IllegalArgumentException(ERROR_CHECK_NO_GSI_KEY);
        }

        this.tableHasRangeKey = tableHasRangeKey;
        this.checkGSIHashKey = checkGSIHashKey;
        this.checkGSIRangeKey = checkGSIRangeKey;
        this.recordGsiValueInViolationRecord = recordGsiValueInViolationRecord;
    }

    public ViolationRecord getViolationRecordHead() {
        ViolationRecord violationRecordHead = new ViolationRecord(tableHasRangeKey, checkGSIHashKey, checkGSIRangeKey, recordGsiValueInViolationRecord);
        violationRecordHead.setTableHashKey(TABLE_HASH_KEY);

        if (tableHasRangeKey) {
            violationRecordHead.setTableRangeKey(TABLE_RANGE_KEY);
        }

        if (checkGSIHashKey) {
            violationRecordHead.setGSIHashKey(GSI_HASH_KEY);
            violationRecordHead.setGSIHashKeyViolationType(GSI_HASH_KEY_VIOLATION_TYPE);
            violationRecordHead.setGSIHashKeyViolationDesc(GSI_HASH_KEY_VIOLATION_DESC);
            violationRecordHead.setGSIHashKeyUpdateValue(GSI_HASH_KEY_UPDATE_VALUE);
        }

        if (checkGSIRangeKey) {
            violationRecordHead.setGSIRangeKey(GSI_RANGE_KEY);
            violationRecordHead.setGSIRangeKeyViolationType(GSI_RANGE_KEY_VIOLATION_TYPE);
            violationRecordHead.setGSIRangeKeyViolationDesc(GSI_RANGE_KEY_VIOLATION_DESC);
            violationRecordHead.setGSIRangeKeyUpdateValue(GSI_RANGE_KEY_UPDATE_VALUE);
        }

        violationRecordHead.setGSIDeleteBlankOnUpdate(GSI_CORRECTION_DELETE_BLANK);
        return violationRecordHead;
    }

    /**
     * Convert the record into string list. If key checked, but violation not
     * found, will store empty string to keep the format consistent.
     */
    public List<String> toStringList() {
        List<String> record = new ArrayList<String>();
        record.add(tableHashKey);
        if (tableHasRangeKey) {
            record.add(tableRangeKey);
        }

        if (checkGSIHashKey) {
            if (GSIHashKeyViolationRecorded) {
                if (recordGsiValueInViolationRecord) {
                    record.add(GSIHashKey);
                }
                record.add(GSIHashKeyViolationType);
                record.add(GSIHashKeyViolationDesc);
                record.add(GSIHashKeyUpdateValue);
            } else {
                if (recordGsiValueInViolationRecord) {
                    record.add("");
                }
                record.add("");
                record.add("");
                record.add("");
            }
        }

        if (checkGSIRangeKey) {
            if (GSIRangeKeyViolationRecorded) {
                if (recordGsiValueInViolationRecord) {
                    record.add(GSIRangeKey);
                }
                record.add(GSIRangeKeyViolationType);
                record.add(GSIRangeKeyViolationDesc);
                record.add(GSIRangekeyUpdateValue);
            } else {
                if (recordGsiValueInViolationRecord) {
                    record.add("");
                }
                record.add("");
                record.add("");
                record.add("");
            }
        }

        record.add(GSIDeleteBlankOnUpdate);
        return record;
    }

    public void clear() {
        GSIHashKeyViolationRecorded = false;
        GSIRangeKeyViolationRecorded = false;
    }

    public void setTableHashKey(String tableHashKey) {
        this.tableHashKey = tableHashKey;
    }

    public void setTableRangeKey(String tableRangeKey) {
        if (!tableHasRangeKey) {
            throw new IllegalArgumentException(ERROR_SET_TABLE_RANGE_KEY);
        }
        this.tableRangeKey = tableRangeKey;
    }

    public void setGSIHashKey(String GSIHashKey) {
        if (!checkGSIHashKey) {
            throw new IllegalArgumentException(ERROR_SET_GSI_HASH_KEY);
        }
        this.GSIHashKey = GSIHashKey;
        GSIHashKeyViolationRecorded = true;
    }

    public void setGSIHashKeyViolationType(String GSIHashKeyViolationType) {
        if (!checkGSIHashKey) {
            throw new IllegalArgumentException(ERROR_SET_GSI_HASH_KEY_VIO_TYPE);
        }
        this.GSIHashKeyViolationType = GSIHashKeyViolationType;
        GSIHashKeyViolationRecorded = true;
    }

    public void setGSIHashKeyViolationDesc(String GSIHashKeyViolationDesc) {
        if (!checkGSIHashKey) {
            throw new IllegalArgumentException(ERROR_SET_GSI_HASH_KEY_VIO_DESC);
        }
        this.GSIHashKeyViolationDesc = GSIHashKeyViolationDesc;
    }

    public void setGSIHashKeyUpdateValue(String GSIHashKeyUpdateValue) {
        if (!checkGSIHashKey) {
            throw new IllegalArgumentException(ERROR_SET_GSI_HASH_KEY_UPDATE);
        }
        this.GSIHashKeyUpdateValue = GSIHashKeyUpdateValue;
    }

    public void setGSIRangeKey(String GSIRangeKey) {
        if (!checkGSIRangeKey) {
            throw new IllegalArgumentException(ERROR_SET_GSI_RANGE_KEY);
        }
        this.GSIRangeKey = GSIRangeKey;
        GSIRangeKeyViolationRecorded = true;
    }

    public void setGSIRangeKeyViolationType(String GSIRangeKeyViolationType) {
        if (!checkGSIRangeKey) {
            throw new IllegalArgumentException(ERROR_MSG_SET_GSI_RANGE_KEY_VIO_TYPE);
        }
        this.GSIRangeKeyViolationType = GSIRangeKeyViolationType;
        GSIRangeKeyViolationRecorded = true;
    }

    public void setGSIRangeKeyViolationDesc(String GSIRangeKeyViolationDesc) {
        if (!checkGSIRangeKey) {
            throw new IllegalArgumentException(ERROR_MSG_SET_GSI_RANGE_KEY_VIO_DESC);
        }
        this.GSIRangeKeyViolationDesc = GSIRangeKeyViolationDesc;
    }

    public void setGSIRangeKeyUpdateValue(String GSIRangekeyUpdateValue) {
        if (!checkGSIRangeKey) {
            throw new IllegalArgumentException(ERROR_MSG_GSI_RANGE_KEY_UPDATE);
        }
        this.GSIRangekeyUpdateValue = GSIRangekeyUpdateValue;
    }

    public void setGSIDeleteBlankOnUpdate(String GSIDeleteBlankOnUpdate) {
        this.GSIDeleteBlankOnUpdate = GSIDeleteBlankOnUpdate;
    }
}
