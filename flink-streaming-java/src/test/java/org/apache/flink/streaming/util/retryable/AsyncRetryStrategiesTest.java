/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.retryable;

import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/** Tests for the {@link AsyncRetryStrategies}. */
public class AsyncRetryStrategiesTest extends TestLogger {

    @Test
    public void testFixedDelayRetryStrategy() {
        AsyncRetryStrategy<Void> fixedDelayRetryStrategy =
                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<Void>(10, 100).build();

        Assert.assertTrue(fixedDelayRetryStrategy.canRetry(10));
        Assert.assertFalse(fixedDelayRetryStrategy.canRetry(11));
        Assert.assertEquals(100, fixedDelayRetryStrategy.getBackoffTimeMillis(0));
    }

    @Test
    public void testExponentialBackoffDelayRetryStrategy() {
        AsyncRetryStrategy<Void> exponentialBackoffDelayRetryStrategy =
                new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Void>(
                                10, 100, 2000, 2)
                        .build();

        Assert.assertEquals(100, exponentialBackoffDelayRetryStrategy.getBackoffTimeMillis(0));
        Assert.assertEquals(100, exponentialBackoffDelayRetryStrategy.getBackoffTimeMillis(1));
        Assert.assertEquals(2000, exponentialBackoffDelayRetryStrategy.getBackoffTimeMillis(6));
    }

    @Test
    public void testFixedIncrementalDelayRetryStrategy() {
        AsyncRetryStrategy<Void> fixedIncrementalDelayRetryStrategy =
                new AsyncRetryStrategies.FixedIncrementalDelayRetryStrategyBuilder<Void>(
                                10, 100, 500, 100)
                        .build();

        Assert.assertEquals(100, fixedIncrementalDelayRetryStrategy.getBackoffTimeMillis(0));
        Assert.assertEquals(100, fixedIncrementalDelayRetryStrategy.getBackoffTimeMillis(1));
        Assert.assertEquals(500, fixedIncrementalDelayRetryStrategy.getBackoffTimeMillis(6));
    }
}
