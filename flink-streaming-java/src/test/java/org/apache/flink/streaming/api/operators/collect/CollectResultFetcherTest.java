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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.operators.collect.utils.SimpleCountCoordinationRequestHandler;
import org.apache.flink.streaming.api.operators.collect.utils.TestJobClient;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Tests for {@link CollectResultIterator}. */
public class CollectResultFetcherTest extends TestLogger {
    private final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;
    private static final OperatorID TEST_OPERATOR_ID = new OperatorID();

    private static final JobID TEST_JOB_ID = new JobID();
    private static final String ACCUMULATOR_NAME = "accumulatorName";

    @Test
    public void testFetcherWithFixedIncrementalDelayRetryStrategy() throws Exception {
        List<Integer> data = new ArrayList<>();
        SimpleCountCoordinationRequestHandler handler = new SimpleCountCoordinationRequestHandler();
        Tuple2<CollectResultFetcher<Integer>, JobClient> tuple2 =
                createFetcherAndJobClient(
                        new UncheckpointedCollectResultBuffer<>(serializer, true),
                        handler,
                        new AsyncRetryStrategies.FixedIncrementalDelayRetryStrategyBuilder<Void>(
                                        Integer.MAX_VALUE, 100, 1000, 100)
                                .build());

        CollectResultFetcher<Integer> resultFetcher = tuple2.f0;

        Thread t =
                new Thread(
                        () -> {
                            try {
                                resultFetcher.next();
                            } catch (Exception ignored) {

                            }
                        });

        t.start();
        TimeUnit.MILLISECONDS.sleep(1100);
        handler.close();
        Assert.assertEquals(5, handler.getRequestCount());
    }

    @Test
    public void testFetcherWithExponentialBackoffDelayRetryStrategy() throws Exception {
        List<Integer> data = new ArrayList<>();
        SimpleCountCoordinationRequestHandler handler = new SimpleCountCoordinationRequestHandler();
        Tuple2<CollectResultFetcher<Integer>, JobClient> tuple2 =
                createFetcherAndJobClient(
                        new UncheckpointedCollectResultBuffer<>(serializer, true),
                        handler,
                        new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Void>(
                                        Integer.MAX_VALUE, 100, 1000, 2)
                                .build());

        CollectResultFetcher<Integer> resultFetcher = tuple2.f0;

        Thread t =
                new Thread(
                        () -> {
                            try {
                                resultFetcher.next();
                            } catch (Exception ignored) {

                            }
                        });

        t.start();
        TimeUnit.MILLISECONDS.sleep(1100);
        handler.close();
        Assert.assertEquals(4, handler.getRequestCount());
    }

    @Test
    public void testFetcherWithFixedDelayRetryStrategy() throws Exception {
        List<Integer> data = new ArrayList<>();
        SimpleCountCoordinationRequestHandler handler = new SimpleCountCoordinationRequestHandler();
        Tuple2<CollectResultFetcher<Integer>, JobClient> tuple2 =
                createFetcherAndJobClient(
                        new UncheckpointedCollectResultBuffer<>(serializer, true),
                        handler,
                        new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<Void>(
                                        Integer.MAX_VALUE, 100)
                                .build());

        CollectResultFetcher<Integer> resultFetcher = tuple2.f0;

        Thread t =
                new Thread(
                        () -> {
                            try {
                                resultFetcher.next();
                            } catch (Exception ignored) {

                            }
                        });

        t.start();
        TimeUnit.MILLISECONDS.sleep(1050);
        handler.close();
        Assert.assertEquals(11, handler.getRequestCount());
    }

    @Test
    public void testFixedDelayRetryStrategy() throws Exception {}

    private Tuple2<CollectResultFetcher<Integer>, JobClient> createFetcherAndJobClient(
            AbstractCollectResultBuffer<Integer> buffer,
            SimpleCountCoordinationRequestHandler handler,
            AsyncRetryStrategy asyncRetryStrategy) {
        CollectResultFetcher<Integer> resultFetcher =
                new CollectResultFetcher<Integer>(
                        buffer,
                        CompletableFuture.completedFuture(TEST_OPERATOR_ID),
                        ACCUMULATOR_NAME,
                        asyncRetryStrategy,
                        AkkaOptions.ASK_TIMEOUT_DURATION.defaultValue().toMillis());

        TestJobClient.JobInfoProvider infoProvider =
                new TestJobClient.JobInfoProvider() {

                    @Override
                    public boolean isJobFinished() {
                        return handler.isClosed();
                    }

                    @Override
                    public Map<String, OptionalFailure<Object>> getAccumulatorResults() {
                        return new HashMap<>();
                    }
                };

        TestJobClient jobClient =
                new TestJobClient(TEST_JOB_ID, TEST_OPERATOR_ID, handler, infoProvider);
        resultFetcher.setJobClient(jobClient);

        return Tuple2.of(resultFetcher, jobClient);
    }
}
