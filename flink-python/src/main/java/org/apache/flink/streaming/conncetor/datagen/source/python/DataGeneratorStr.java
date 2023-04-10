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
import org.apache.flink.util.StringUtils;

import java.util.Random;

/**
 * A data source that produces random String type data in parallel, which is used by flink python.
 * Refer to {@link org.apache.flink.connector.datagen.source.DataGeneratorSource}.
 */
public class DataGeneratorStr extends DataGeneratorSource<String> {

    /**
     * Instantiates a new {@code DataGeneratorStr}.
     *
     * @param count The number of generated data points.
     * @param length The length of each generated string data.
     * @param rateLimiterStrategy The strategy for rate limiting.
     */
    public DataGeneratorStr(long count, int length, RateLimiterStrategy rateLimiterStrategy) {
        this(new StringGeneratorFunction(length), count, rateLimiterStrategy, Types.STRING);
    }

    private DataGeneratorStr(
            GeneratorFunction<Long, String> generatorFunction,
            long count,
            RateLimiterStrategy rateLimiterStrategy,
            TypeInformation<String> typeInfo) {
        super(generatorFunction, count, rateLimiterStrategy, typeInfo);
    }

    /** A GeneratorFunction that is used to generate random string data. */
    public static class StringGeneratorFunction implements GeneratorFunction<Long, String> {

        private Random rd;
        private int length;

        public StringGeneratorFunction(int length) {
            this.length = length;
        }

        @Override
        public String map(Long value) throws Exception {
            if (rd == null) {
                rd = new Random(System.currentTimeMillis() - value);
            }
            return StringUtils.generateRandomAlphanumericString(rd, length);
        }
    }
}
