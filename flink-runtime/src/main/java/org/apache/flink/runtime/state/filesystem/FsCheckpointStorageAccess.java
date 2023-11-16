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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.GrafanaGauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeImpl;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.TagGaugeStoreImpl;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.NotDuplicatingCheckpointStateToolset;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of durable checkpoint storage to file systems. */
public class FsCheckpointStorageAccess extends AbstractFsCheckpointStorageAccess {
    private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStorageAccess.class);

    private final FileSystem fileSystem;

    private final Path checkpointsDirectory;

    private final Path sharedStateDirectory;

    private final Path taskOwnedStateDirectory;

    private final int fileSizeThreshold;

    private final int writeBufferSize;

    private boolean baseLocationsInitialized = false;

    private final CheckpointWriteFileStatistic metricReference;

    private final CheckpointWriteFileStatistic currentPeriodStatistic;

    public FsCheckpointStorageAccess(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        this(
                checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                jobId,
                null,
                null,
                fileSizeThreshold,
                writeBufferSize);
    }

    public FsCheckpointStorageAccess(
            FileSystem fs,
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            @Nullable String jobName,
            @Nullable String checkpointsNamespace,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        super(jobId, defaultSavepointDirectory);

        checkArgument(fileSizeThreshold >= 0);
        checkArgument(writeBufferSize >= 0);

        this.fileSystem = checkNotNull(fs);
        if (jobName != null) {
            this.checkpointsDirectory =
                    getCheckpointDirectoryForJob(
                            checkpointBaseDirectory, jobName, checkpointsNamespace);
        } else {
            this.checkpointsDirectory =
                    getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId);
        }

        this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
        this.taskOwnedStateDirectory =
                new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
        this.fileSizeThreshold = fileSizeThreshold;
        this.writeBufferSize = writeBufferSize;
        this.metricReference = new CheckpointWriteFileStatistic(checkpointBaseDirectory.getPath());
        this.currentPeriodStatistic =
                new CheckpointWriteFileStatistic(checkpointBaseDirectory.getPath());
    }

    public void registerMetrics(MetricGroup metricGroup) {
        metricGroup.tagGauge(
                CHECKPOINT_WRITE_FILE_RATE_METRIC,
                new CheckpointWriteFileRate(metricReference, currentPeriodStatistic));
        metricGroup.tagGauge(
                CHECKPOINT_WRITE_FILE_LATENCY_METRIC,
                new CheckpointWriteFileLatency(metricReference));
        metricGroup.tagGauge(
                CHECKPOINT_CLOSE_FILE_LATENCY_METRIC,
                new CheckpointCloseFileLatency(metricReference));
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    Path getCheckpointsDirectory() {
        return checkpointsDirectory;
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorage implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsHighlyAvailableStorage() {
        return true;
    }

    @Override
    public void initializeBaseLocationsForCheckpoint() throws IOException {
        if (!fileSystem.mkdirs(sharedStateDirectory)) {
            throw new IOException(
                    "Failed to create directory for shared state: " + sharedStateDirectory);
        }
        if (!fileSystem.mkdirs(taskOwnedStateDirectory)) {
            throw new IOException(
                    "Failed to create directory for task owned state: " + taskOwnedStateDirectory);
        }
    }

    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
            throws IOException {
        checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %s.", checkpointId);

        // prepare all the paths needed for the checkpoints
        final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

        // create the checkpoint exclusive directory
        fileSystem.mkdirs(checkpointDir);

        return new FsCheckpointStorageLocation(
                fileSystem,
                checkpointDir,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                CheckpointStorageLocationReference.getDefault(),
                fileSizeThreshold,
                writeBufferSize,
                currentPeriodStatistic);
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException {

        if (reference.isDefaultReference()) {
            // default reference, construct the default location for that particular checkpoint
            final Path checkpointDir =
                    createCheckpointDirectory(checkpointsDirectory, checkpointId);

            return new FsCheckpointStorageLocation(
                    fileSystem,
                    checkpointDir,
                    sharedStateDirectory,
                    taskOwnedStateDirectory,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize,
                    currentPeriodStatistic);
        } else {
            // location encoded in the reference
            final Path path = decodePathFromReference(reference);

            return new FsCheckpointStorageLocation(
                    path.getFileSystem(),
                    path,
                    path,
                    path,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize,
                    currentPeriodStatistic);
        }
    }

    @Override
    public CheckpointStateOutputStream createTaskOwnedStateStream() {
        // as the comment of CheckpointStorageWorkerView#createTaskOwnedStateStream said we may
        // change into shared state,
        // so we use CheckpointedStateScope.SHARED here.
        return new FsCheckpointStateOutputStream(
                taskOwnedStateDirectory,
                fileSystem,
                writeBufferSize,
                fileSizeThreshold,
                false,
                currentPeriodStatistic);
    }

    @Override
    public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
        if (fileSystem instanceof DuplicatingFileSystem) {
            return new FsCheckpointStateToolset(
                    taskOwnedStateDirectory, (DuplicatingFileSystem) fileSystem);
        } else {
            return new NotDuplicatingCheckpointStateToolset();
        }
    }

    @Override
    protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
        final CheckpointStorageLocationReference reference = encodePathAsReference(location);
        return new FsCheckpointStorageLocation(
                fs,
                location,
                location,
                location,
                reference,
                fileSizeThreshold,
                writeBufferSize,
                currentPeriodStatistic);
    }

    @Override
    public List<String> findCompletedCheckpointPointer() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(checkpointsDirectory);
        if (statuses == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(statuses)
                .filter(
                        fileStatus -> {
                            try {
                                return fileStatus
                                                .getPath()
                                                .getName()
                                                .startsWith(CHECKPOINT_DIR_PREFIX)
                                        && fileSystem.exists(
                                                new Path(fileStatus.getPath(), METADATA_FILE_NAME));
                            } catch (IOException e) {
                                LOG.info(
                                        "Exception when checking {} is completed checkpoint.",
                                        fileStatus.getPath(),
                                        e);
                                return false;
                            }
                        })
                .sorted(
                        Comparator.comparingInt(
                                        (FileStatus fileStatus) -> {
                                            try {
                                                return Integer.parseInt(
                                                        fileStatus
                                                                .getPath()
                                                                .getName()
                                                                .substring(
                                                                        CHECKPOINT_DIR_PREFIX
                                                                                .length()));
                                            } catch (Exception e) {
                                                LOG.info(
                                                        "Exception when parsing checkpoint {} id.",
                                                        fileStatus.getPath(),
                                                        e);
                                                return Integer.MIN_VALUE;
                                            }
                                        })
                                .reversed())
                .map(fileStatus -> fileStatus.getPath().toString())
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private static final String CHECKPOINT_WRITE_FILE_RATE_METRIC = "checkpointWriteFileRate";

    private static final String CHECKPOINT_WRITE_FILE_LATENCY_METRIC = "checkpointWriteFileLatency";

    private static final String CHECKPOINT_CLOSE_FILE_LATENCY_METRIC = "checkpointCloseFileLatency";

    public static final String STORAGE_CLUSTER_TAG = "storageBasePath";

    /** Metric for write hdfs file. */
    public static class CheckpointWriteFileStatistic {
        private final Object lock = new Object();
        private final long timeSpanInSeconds;
        private String storageBaseDir = "-";

        private long writeBytes = 0;
        private long writeLatency = 0;
        private long writeCount = 0;
        private long closeLatency = 0;
        private long closeCount = 0;

        @VisibleForTesting
        public CheckpointWriteFileStatistic() {
            this(CheckpointWriteFileRate.DEFAULT_TIME_SPAN_IN_SECONDS);
        }

        public CheckpointWriteFileStatistic(long timeSpanInSeconds) {
            this.timeSpanInSeconds = timeSpanInSeconds;
        }

        public CheckpointWriteFileStatistic(String storageBaseDir) {
            this(CheckpointWriteFileRate.DEFAULT_TIME_SPAN_IN_SECONDS, storageBaseDir);
        }

        public CheckpointWriteFileStatistic(long timeSpanInSeconds, String storageBaseDir) {
            this.timeSpanInSeconds = timeSpanInSeconds;
            this.storageBaseDir = storageBaseDir;
        }

        public void updateWriteStatistics(long writeBytes, long writeLatency, long writeCount) {
            synchronized (lock) {
                this.writeBytes += writeBytes;
                this.writeLatency += writeLatency;
                this.writeCount += writeCount;
            }
        }

        public void updateCloseStatistics(long closeLatency) {
            synchronized (lock) {
                this.closeLatency += closeLatency;
                this.closeCount += 1;
            }
        }

        public void updateAllStatistics(
                long writeBytes,
                long writeLatency,
                long writeCount,
                long closeLatency,
                long closeCount) {
            synchronized (lock) {
                this.writeBytes += writeBytes;
                this.writeCount += writeCount;
                this.writeLatency += writeLatency;
                this.closeLatency += closeLatency;
                this.closeCount += closeCount;
            }
        }

        public CheckpointWriteFileStatistic getAndResetStatistics() {
            synchronized (lock) {
                CheckpointWriteFileStatistic statistic =
                        new CheckpointWriteFileStatistic(this.timeSpanInSeconds);
                statistic.updateAllStatistics(
                        this.writeBytes,
                        this.writeLatency,
                        this.writeCount,
                        this.closeLatency,
                        this.closeCount);
                this.writeBytes = 0L;
                this.writeCount = 0L;
                this.writeLatency = 0L;
                this.closeLatency = 0L;
                this.closeCount = 0L;
                return statistic;
            }
        }

        public double getWriteRate() {
            synchronized (lock) {
                return ((double) writeBytes) / timeSpanInSeconds;
            }
        }

        public double getAvgWriteLatency() {
            synchronized (lock) {
                return writeCount > 0 ? ((double) writeLatency / writeCount) : 0.0;
            }
        }

        public double getAvgCloseLatency() {
            synchronized (lock) {
                return closeCount > 0 ? ((double) closeLatency / closeCount) : 0.0;
            }
        }

        public String getStorageBaseDir() {
            return storageBaseDir;
        }

        @Override
        public String toString() {
            return "CheckpointWriteFileStatistic{"
                    + "timeSpanInSeconds="
                    + timeSpanInSeconds
                    + ", writeBytes="
                    + writeBytes
                    + ", writeLatency="
                    + writeLatency
                    + ", writeCount="
                    + writeCount
                    + ", closeLatency="
                    + closeLatency
                    + ", closeCount="
                    + closeCount
                    + '}';
        }
    }

    private static class CheckpointWriteFileRate
            implements GrafanaGauge<TagGaugeStore>, View, TagGauge {
        public static final int DEFAULT_TIME_SPAN_IN_SECONDS = 60;

        /** The time-span over which the average is calculated. */
        private final int timeSpanInSeconds;
        /** Metric reference. */
        private final CheckpointWriteFileStatistic metricReference;
        /** Current period statistic reference. */
        private final CheckpointWriteFileStatistic currentPeriodStatistic;
        /** Circular array containing the history of values. */
        private final CheckpointWriteFileStatistic[] values;
        /** The index in the array for the current time. */
        private int index = 0;
        /** The tag gauge store. */
        private final TagGaugeStoreImpl statsStore;
        /** The tag values. */
        private final TagGaugeStore.TagValues tag;

        public CheckpointWriteFileRate(
                CheckpointWriteFileStatistic metricReference,
                CheckpointWriteFileStatistic currentPeriodStatistic) {
            this.timeSpanInSeconds = DEFAULT_TIME_SPAN_IN_SECONDS;
            this.metricReference = metricReference;
            this.currentPeriodStatistic = currentPeriodStatistic;
            this.values =
                    new CheckpointWriteFileStatistic
                            [this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
            this.statsStore =
                    new TagGaugeStoreImpl(
                            1024, true, false, TagGaugeImpl.MetricsReduceType.NO_REDUCE);
            this.tag =
                    new TagGaugeStore.TagValuesBuilder()
                            .addTagValue(STORAGE_CLUSTER_TAG, metricReference.getStorageBaseDir())
                            .build();
        }

        @Override
        public TagGaugeStore getValue() {
            statsStore.addMetric(metricReference.getWriteRate(), tag);
            return statsStore;
        }

        @Override
        public void update() {
            index = (index + 1) % values.length;
            CheckpointWriteFileStatistic old = values[index];
            values[index] = currentPeriodStatistic.getAndResetStatistics();
            if (old == null) {
                old = new CheckpointWriteFileStatistic(this.timeSpanInSeconds);
            }
            metricReference.updateAllStatistics(
                    values[index].writeBytes - old.writeBytes,
                    values[index].writeLatency - old.writeLatency,
                    values[index].writeCount - old.writeCount,
                    values[index].closeLatency - old.closeLatency,
                    values[index].closeCount - old.closeCount);
        }
    }

    private static class CheckpointWriteFileLatency
            implements GrafanaGauge<TagGaugeStore>, TagGauge {
        /** Metric reference. */
        private final CheckpointWriteFileStatistic metricReference;

        private final TagGaugeStoreImpl statsStore;
        private final TagGaugeStore.TagValues tag;

        public CheckpointWriteFileLatency(CheckpointWriteFileStatistic metricReference) {
            this.metricReference = metricReference;
            this.statsStore =
                    new TagGaugeStoreImpl(
                            1024, true, false, TagGaugeImpl.MetricsReduceType.NO_REDUCE);
            this.tag =
                    new TagGaugeStore.TagValuesBuilder()
                            .addTagValue(STORAGE_CLUSTER_TAG, metricReference.getStorageBaseDir())
                            .build();
        }

        @Override
        public TagGaugeStore getValue() {
            statsStore.addMetric(metricReference.getAvgWriteLatency(), tag);
            return statsStore;
        }
    }

    private static class CheckpointCloseFileLatency
            implements GrafanaGauge<TagGaugeStore>, TagGauge {
        /** Metric reference. */
        private final CheckpointWriteFileStatistic metricReference;

        private final TagGaugeStoreImpl statsStore;
        private final TagGaugeStore.TagValues tag;

        public CheckpointCloseFileLatency(CheckpointWriteFileStatistic metricReference) {
            this.metricReference = metricReference;
            this.statsStore =
                    new TagGaugeStoreImpl(
                            1024, true, false, TagGaugeImpl.MetricsReduceType.NO_REDUCE);
            this.tag =
                    new TagGaugeStore.TagValuesBuilder()
                            .addTagValue(STORAGE_CLUSTER_TAG, metricReference.getStorageBaseDir())
                            .build();
        }

        @Override
        public TagGaugeStore getValue() {
            statsStore.addMetric(metricReference.getAvgCloseLatency(), tag);
            return statsStore;
        }
    }
}
