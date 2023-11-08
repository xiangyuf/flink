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

package org.apache.flink.connector.preview;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;

/**
 * Preview connector used to preview sink data for validate SQL logic, now used for sql debug
 * internal.
 */
public class PreviewTableSinkFactory implements DynamicTableSinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PreviewTableSinkFactory.class);

    public static final String IDENTIFIER = "preview";

    public static final ConfigOption<Boolean> CHANGELOG_MODE_ENABLE =
            ConfigOptions.key("changelog-mode.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable changelog mode, can be controlled to generate and obtain this mode data.");

    public static final ConfigOption<Integer> CHANGE_RESULT_ROWS_MAX =
            ConfigOptions.key("changelog-mode.result-rows.max")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "The number of rows to cache 	when in the changelog result. "
                                    + "If the number of rows exceeds the specified value, it retries the row in the FIFO style.");

    public static final ConfigOption<Boolean> TABLE_MODE_ENABLE =
            ConfigOptions.key("table-mode.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable table mode, can be controlled to generate and obtain this mode data.");

    public static final ConfigOption<Integer> TABLE_RESULT_ROWS_MAX =
            ConfigOptions.key("table-mode.result-rows.max")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "The number of rows to cache when in the table result. "
                                    + "If the number of rows exceeds the specified value, it retries the row in the FIFO style.");

    public static final ConfigOption<Boolean> INTERNAL_TEST_ENABLE =
            ConfigOptions.key("internal-test.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If internal test enable, the result will send to system out stream for test.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        PreviewTableOptions.Builder previewBuilder = PreviewTableOptions.builder();
        ReadableConfig readableConfig = helper.getOptions();
        previewBuilder.setChangelogModeEnable(readableConfig.get(CHANGELOG_MODE_ENABLE));
        previewBuilder.setChangelogRowsMax(readableConfig.get(CHANGE_RESULT_ROWS_MAX));
        previewBuilder.setTableModeEnable(readableConfig.get(TABLE_MODE_ENABLE));
        previewBuilder.setTableRowsMax(readableConfig.get(TABLE_RESULT_ROWS_MAX));
        previewBuilder.setInternalTestEnable(readableConfig.get(INTERNAL_TEST_ENABLE));

        FlinkConnectorRateLimiter rateLimiter = null;
        Optional<Long> rateLimitNumOptional = readableConfig.getOptional(RATE_LIMIT_NUM);
        if (rateLimitNumOptional.isPresent()) {
            rateLimiter = new GuavaFlinkConnectorRateLimiter();
            rateLimiter.setRate(rateLimitNumOptional.get());
        }
        previewBuilder.setRateLimiter(rateLimiter);

        TableSchema tableSchema = context.getCatalogTable().getSchema();

        return new PreviewTableSink(tableSchema, previewBuilder.build(), format);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(CHANGELOG_MODE_ENABLE);
        optionalOptions.add(CHANGE_RESULT_ROWS_MAX);
        optionalOptions.add(TABLE_MODE_ENABLE);
        optionalOptions.add(TABLE_RESULT_ROWS_MAX);
        optionalOptions.add(RATE_LIMIT_NUM);
        return optionalOptions;
    }
}
