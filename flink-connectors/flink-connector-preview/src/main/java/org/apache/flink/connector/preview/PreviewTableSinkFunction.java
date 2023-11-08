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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataRequest;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataResponse;
import org.apache.flink.streaming.api.functions.sink.PreviewSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** Preview table sink function. */
public class PreviewTableSinkFunction extends PreviewSinkFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(PreviewTableSinkFunction.class);
    private static final long serialVersionUID = 1L;

    private final PreviewTableOptions previewTableOptions;
    private final SerializationSchema<RowData> serializationSchema;
    private final TypeInformation<RowData> rowDataTypeInformation;
    private final FlinkConnectorRateLimiter rateLimiter;

    private transient ResultDataManager<RowData> tableResult;
    private transient ResultDataManager<String> changeLogResult;
    private transient TypeSerializer<RowData> typeSerializer;

    public PreviewTableSinkFunction(
            PreviewTableOptions previewTableOptions,
            SerializationSchema<RowData> serializationSchema,
            TypeInformation<RowData> rowDataTypeInformation) {
        this.previewTableOptions = previewTableOptions;
        this.serializationSchema = serializationSchema;
        this.rowDataTypeInformation = rowDataTypeInformation;
        this.rateLimiter = previewTableOptions.getRateLimiter();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        serializationSchema.open(
                new SerializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return new UnregisteredMetricsGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return SimpleUserCodeClassLoader.create(
                                PreviewTableSinkFunction.class.getClassLoader());
                    }
                });
        initResultSizeSpace(MemorySize.parse(parameters.get(AkkaOptions.FRAMESIZE)));
        typeSerializer =
                rowDataTypeInformation.createSerializer(getRuntimeContext().getExecutionConfig());
        if (rateLimiter != null) {
            rateLimiter.open(getRuntimeContext());
        }
    }

    private void initResultSizeSpace(MemorySize akkaFrameSize) {
        /*
         * left 10% buffer.
         */
        double maxByteSize = akkaFrameSize.getBytes() * 0.9;
        long maxResultByteSize;
        if (previewTableOptions.isChangelogModeEnable()
                && previewTableOptions.isTableModeEnable()) {
            /*
             * tableResult and changeLog will take each half the space.
             */
            maxResultByteSize = new Double(maxByteSize / 2).longValue();
        } else {
            maxResultByteSize = new Double(maxByteSize).longValue();
        }
        LOG.info("the preview result byteSize max is: {}", maxResultByteSize);
        changeLogResult =
                new ResultDataManager<>(
                        previewTableOptions.getChangelogRowsMax(), maxResultByteSize, null);
        tableResult =
                new ResultDataManager<>(
                        previewTableOptions.getTableRowsMax(),
                        maxResultByteSize,
                        serializationSchema);
    }

    @Override
    public PreviewDataResponse getPreviewData(PreviewDataRequest previewDataRequest) {
        synchronized (this) {
            PreviewDataResponse previewDataResponse = new PreviewDataResponse();
            if (previewTableOptions.isChangelogModeEnable()) {
                previewDataResponse.setChangeLogResult(changeLogResult.getResultList());
            }
            if (previewTableOptions.isTableModeEnable()) {
                previewDataResponse.setTableResult(getResultByRow(tableResult.getResultList()));
            }
            LOG.info(
                    "previewFunction return data, changeLogResult size: {}, tableResult size: {}",
                    changeLogResult.getResultList().size(),
                    tableResult.getResultList().size());
            return previewDataResponse;
        }
    }

    private List<String> getResultByRow(Collection<RowData> rowDataCollection) {
        List<String> result = new ArrayList<>(rowDataCollection.size());
        for (RowData rowData : rowDataCollection) {
            result.add(new String(serializationSchema.serialize(rowData)));
        }
        return result;
    }

    private void processChangeLogResult(RowData rowData) {
        final String data =
                rowData.getRowKind().shortString()
                        + new String(serializationSchema.serialize(rowData));
        // serialize first for performance and table result will update rowKind insert
        changeLogResult.addData(data);
    }

    private void processTableResult(RowData rowData) {
        boolean isInsertOp =
                rowData.getRowKind() == RowKind.INSERT
                        || rowData.getRowKind() == RowKind.UPDATE_AFTER;

        // Always set the RowKind to INSERT, so that we can compare rows correctly (RowKind will
        // be ignored),
        rowData =
                getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()
                        ? typeSerializer.copy(rowData)
                        : rowData;
        rowData.setRowKind(RowKind.INSERT);

        // insert
        if (isInsertOp) {
            tableResult.addData(rowData);
        }
        // delete
        else {
            // TODO update performance for avoid all traverse
            // delete first duplicate rowData
            for (int i = tableResult.getResultList().size() - 1; i >= 0; i--) {
                if (tableResult.getResultList().get(i).equals(rowData)) {
                    final RowData remove = tableResult.getResultList().remove(i);
                    tableResult.releaseSpace(remove);
                    break;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (previewTableOptions.isInternalTestEnable()) {
            PreviewDataResponse response = getPreviewData(new PreviewDataRequest());
            if (previewTableOptions.isChangelogModeEnable()) {
                System.out.println(
                        response.getChangeLogResult().stream().collect(Collectors.joining("\n")));
            }
            if (previewTableOptions.isChangelogModeEnable()
                    && previewTableOptions.isTableModeEnable()) {
                System.out.println("|");
            }
            if (previewTableOptions.isTableModeEnable()) {
                System.out.println(
                        response.getTableResult().stream().collect(Collectors.joining("\n")));
            }
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (rateLimiter != null) {
            rateLimiter.acquire(1);
        }
        synchronized (this) {
            if (previewTableOptions.isChangelogModeEnable()) {
                processChangeLogResult(value);
            }
            if (previewTableOptions.isTableModeEnable()) {
                processTableResult(value);
            }
        }
    }

    private static class ResultDataManager<T> {
        private final transient LinkedList<T> resultList;
        /** max number of result list. */
        private final transient int maxResultCount;

        private final transient long maxResultByteSize;
        private final transient SerializationSchema<T> serializationSchema;

        private transient long currentResultByteSize;

        public ResultDataManager(
                int maxResultCount,
                long maxResultByteSize,
                SerializationSchema<T> serializationSchema) {
            this.resultList = new LinkedList<>();
            this.maxResultCount = maxResultCount;
            this.currentResultByteSize = 0L;
            this.maxResultByteSize = maxResultByteSize;
            this.serializationSchema = serializationSchema;
        }

        public void addData(T data) {
            final int byteSize = getDataByteSize(data);
            while (!applyResultSizeSpace(byteSize)) {
                releaseSpace(resultList.removeFirst());
            }
            resultList.add(data);
        }

        /**
         * Apply for space.
         *
         * @return if it's able to add new data. false mean we need to remove old data.
         */
        private boolean applyResultSizeSpace(int byteSize) {
            if (resultList.size() >= this.maxResultCount) {
                return false;
            }
            if (byteSize >= this.maxResultByteSize) {
                final String msg =
                        String.format(
                                "The size of a single record exceeds the maximum limit. "
                                        + "Record size: %s, maxResultSize: %s",
                                byteSize, this.maxResultByteSize);
                throw new FlinkRuntimeException(msg);
            }
            if (this.currentResultByteSize + byteSize >= this.maxResultByteSize) {
                LOG.debug(
                        "space is full. totalBytes: {}, content length: {}",
                        this.currentResultByteSize,
                        byteSize);
                return false;
            }
            this.currentResultByteSize += byteSize;
            return true;
        }

        /** release space. */
        private void releaseSpace(T data) {
            this.currentResultByteSize -= getDataByteSize(data);
        }

        /** convert data to string and get byte size. */
        private int getDataByteSize(T data) {
            String content;
            if (String.class.equals(data.getClass())) {
                content = (String) data;
            } else {
                if (serializationSchema != null) {
                    content = new String(serializationSchema.serialize(data));
                } else {
                    throw new FlinkRuntimeException("serializationSchema is null");
                }
            }
            return content.getBytes(StandardCharsets.UTF_8).length;
        }

        public LinkedList<T> getResultList() {
            return resultList;
        }
    }
}
