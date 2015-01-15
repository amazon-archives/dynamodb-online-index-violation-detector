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

import java.util.List;

import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Rate Limiter for read/write DynamoDB table.
 * 
 */
public class TableRWRateLimiter {
    private double readWriteIOPSCapacityUnits;
    private double readWriteIOPSPercent;
    private double accumulatedReadWritePermits;
    private int numOfTasks;
    /**
     * Rate limiter is created with a permits per second rate. Each time the
     * limiter acquire the permitsToConsume, it will adjust the time to block
     * until the next permits is ready to release, so that the overall
     * rate it maintained. For more details, see:
     * http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/util/concurrent/RateLimiter.html
     */
    private RateLimiter rateLimiter;

    public TableRWRateLimiter(double readWriteIOPSCapacityUnits, double readWriteIOPSPercent, int numOfTasks) {
        this.readWriteIOPSCapacityUnits = readWriteIOPSCapacityUnits;
        this.readWriteIOPSPercent = readWriteIOPSPercent;
        this.numOfTasks = numOfTasks;
        this.accumulatedReadWritePermits = 0;
        double rateLimit = getRateLimit();
        rateLimiter = RateLimiter.create(rateLimit);
    }

    private double getRateLimit() {
        double rateLimit = readWriteIOPSCapacityUnits * readWriteIOPSPercent / 100;
        double rateLimitPerTask = rateLimit / numOfTasks;
        if (rateLimitPerTask <= 0) {
            throw new IllegalArgumentException("Error: readWriteIOPSCapacityUnits: " + readWriteIOPSCapacityUnits + " or readWriteIOPSPercent: "
                    + readWriteIOPSPercent + "too low, can not start read or write table.");
        }
        return rateLimitPerTask;
    }

    public void adjustRateWithConsumedCapacity(List<ConsumedCapacity> bathWriteConsumedCapacity) {
        for (ConsumedCapacity consumedCapacity : bathWriteConsumedCapacity) {
            adjustRateWithConsumedCapacity(consumedCapacity);
        }
    }
    
    public void adjustRateWithConsumedCapacity(ConsumedCapacity consumedCapacity) {
        accumulatedReadWritePermits += consumedCapacity.getCapacityUnits();
        if (accumulatedReadWritePermits > 1.0) {
            int intValueOfPermits = Double.valueOf(accumulatedReadWritePermits).intValue();
            rateLimiter.acquire(intValueOfPermits);
            accumulatedReadWritePermits -= (double) intValueOfPermits;
        }
    }

}
