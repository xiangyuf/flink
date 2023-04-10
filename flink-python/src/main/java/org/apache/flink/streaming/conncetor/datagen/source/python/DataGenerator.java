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

package org.apache.flink.streaming.conncetor.datagen.source.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * A data source that produces sequence integer type data in parallel, which is used by flink
 * python. Refer to {@link org.apache.flink.connector.datagen.source.DataGeneratorSource}.
 */
public class DataGenerator extends DataGeneratorSource<Long> {

    /**
     * Instantiates a new {@code DataGenerator}.
     *
     * @param count The number of generated data points.
     * @param rateLimiterStrategy The strategy for rate limiting.
     */
    public DataGenerator(long count, RateLimiterStrategy rateLimiterStrategy) {
        this(v -> v, count, rateLimiterStrategy, Types.LONG);
    }

    private DataGenerator(
            GeneratorFunction<Long, Long> generatorFunction,
            long count,
            RateLimiterStrategy rateLimiterStrategy,
            TypeInformation<Long> typeInfo) {
        super(generatorFunction, count, rateLimiterStrategy, typeInfo);
    }
}
