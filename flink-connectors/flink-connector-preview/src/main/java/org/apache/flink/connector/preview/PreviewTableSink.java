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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.Objects;

/** Preview Table Sink. */
public class PreviewTableSink implements DynamicTableSink {
    private final TableSchema tableSchema;
    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final PreviewTableOptions previewTableOptions;

    public PreviewTableSink(
            TableSchema tableSchema,
            PreviewTableOptions previewTableOptions,
            EncodingFormat<SerializationSchema<RowData>> format) {
        this.tableSchema = tableSchema;
        this.previewTableOptions = previewTableOptions;
        this.format = format;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                this.format.createRuntimeEncoder(context, tableSchema.toRowDataType());
        final TypeInformation<RowData> rowDataTypeInformation =
                context.createTypeInformation(tableSchema.toRowDataType());
        // Preview parallelism should always be 1.
        return SinkFunctionProvider.of(
                new PreviewTableSinkFunction(
                        previewTableOptions, serializationSchema, rowDataTypeInformation),
                1);
    }

    @Override
    public DynamicTableSink copy() {
        return new PreviewTableSink(tableSchema, previewTableOptions, format);
    }

    @Override
    public String asSummaryString() {
        return "Preview";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PreviewTableSink)) {
            return false;
        }
        PreviewTableSink that = (PreviewTableSink) o;
        return Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(format, that.format)
                && Objects.equals(previewTableOptions, that.previewTableOptions);
    }
}
