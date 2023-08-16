/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connector.datagen.source.python;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.conncetor.datagen.source.python.DataGenerator;
import org.apache.flink.streaming.conncetor.datagen.source.python.DataGeneratorStr;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Test DataGenerator usage. */
@Disabled
public class DataGeneratorTest {

    @Test
    void testDataGenerator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> datastream =
                env.fromSource(
                        new DataGenerator(Long.MAX_VALUE, RateLimiterStrategy.perSecond(10)),
                        WatermarkStrategy.noWatermarks(),
                        "source");
        datastream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testGeneratorStr() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> datastream =
                env.fromSource(
                        new DataGeneratorStr(Long.MAX_VALUE, 2, RateLimiterStrategy.perSecond(10)),
                        WatermarkStrategy.noWatermarks(),
                        "source");
        datastream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
