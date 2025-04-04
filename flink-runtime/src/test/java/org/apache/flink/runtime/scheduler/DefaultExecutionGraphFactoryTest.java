/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.DefaultCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.NoOpCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.TestUtils;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.adaptivebatch.NonAdaptiveExecutionPlanSchedulingContext;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultExecutionGraphFactory}. */
class DefaultExecutionGraphFactoryTest {

    private static final Logger log =
            LoggerFactory.getLogger(DefaultExecutionGraphFactoryTest.class);

    @TempDir private File tempDir;
    private File temporaryFile;

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @BeforeEach
    private void setup() {
        temporaryFile = new File(tempDir.getAbsolutePath(), "stateFile");
    }

    @Test
    void testRestoringModifiedJobFromSavepointFails() throws Exception {
        final JobGraph jobGraphWithNewOperator = createJobGraphWithSavepoint(false, 42L, 1);

        final ExecutionGraphFactory executionGraphFactory = createExecutionGraphFactory();

        assertThatThrownBy(
                        () ->
                                executionGraphFactory.createAndRestoreExecutionGraph(
                                        jobGraphWithNewOperator,
                                        new StandaloneCompletedCheckpointStore(1),
                                        new CheckpointsCleaner(),
                                        new StandaloneCheckpointIDCounter(),
                                        NoOpCheckpointStatsTracker.INSTANCE,
                                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                                                .CAN_BE_UNKNOWN,
                                        0L,
                                        new DefaultVertexAttemptNumberStore(),
                                        SchedulerBase.computeVertexParallelismStore(
                                                jobGraphWithNewOperator),
                                        (execution, previousState, newState) -> {},
                                        rp -> false,
                                        NonAdaptiveExecutionPlanSchedulingContext.INSTANCE,
                                        log))
                .withFailMessage(
                        "Expected ExecutionGraph creation to fail because of non restored state.")
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to rollback to checkpoint/savepoint");
    }

    @Test
    void testRestoringModifiedJobFromSavepointWithAllowNonRestoredStateSucceeds() throws Exception {
        // create savepoint data
        final long savepointId = 42L;
        final JobGraph jobGraphWithNewOperator = createJobGraphWithSavepoint(true, savepointId, 1);

        final ExecutionGraphFactory executionGraphFactory = createExecutionGraphFactory();

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        executionGraphFactory.createAndRestoreExecutionGraph(
                jobGraphWithNewOperator,
                completedCheckpointStore,
                new CheckpointsCleaner(),
                new StandaloneCheckpointIDCounter(),
                NoOpCheckpointStatsTracker.INSTANCE,
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN,
                0L,
                new DefaultVertexAttemptNumberStore(),
                SchedulerBase.computeVertexParallelismStore(jobGraphWithNewOperator),
                (execution, previousState, newState) -> {},
                rp -> false,
                NonAdaptiveExecutionPlanSchedulingContext.INSTANCE,
                log);

        final CompletedCheckpoint savepoint = completedCheckpointStore.getLatestCheckpoint();

        assertThat(savepoint).isNotNull();
        assertThat(savepoint.getCheckpointID()).isEqualTo(savepointId);
    }

    @Test
    void testCheckpointStatsTrackerUpdatedWithNewParallelism() throws Exception {
        final long savepointId = 42L;
        final JobGraph jobGraphWithParallelism2 = createJobGraphWithSavepoint(true, savepointId, 2);

        List<Span> spans = new ArrayList<>();
        final JobManagerJobMetricGroup jobManagerJobMetricGroup =
                new UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup() {
                    @Override
                    public void addSpan(SpanBuilder spanBuilder) {
                        spans.add(spanBuilder.build());
                    }
                };
        final ExecutionGraphFactory executionGraphFactory =
                createExecutionGraphFactory(jobManagerJobMetricGroup);

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final CheckpointStatsTracker checkpointStatsTracker =
                new DefaultCheckpointStatsTracker(10, jobManagerJobMetricGroup);
        ExecutionGraph executionGraph =
                executionGraphFactory.createAndRestoreExecutionGraph(
                        jobGraphWithParallelism2,
                        completedCheckpointStore,
                        new CheckpointsCleaner(),
                        new StandaloneCheckpointIDCounter(),
                        checkpointStatsTracker,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN,
                        0L,
                        new DefaultVertexAttemptNumberStore(),
                        new VertexParallelismStore() {
                            @Override
                            public VertexParallelismInformation getParallelismInfo(
                                    JobVertexID vertexId) {
                                return new DefaultVertexParallelismInfo(
                                        1, 1337, integer -> Optional.empty());
                            }

                            @Override
                            public Map<JobVertexID, VertexParallelismInformation>
                                    getAllParallelismInfo() {
                                return Collections.emptyMap();
                            }
                        },
                        (execution, previousState, newState) -> {},
                        rp -> false,
                        NonAdaptiveExecutionPlanSchedulingContext.INSTANCE,
                        log);

        checkpointStatsTracker.reportRestoredCheckpoint(
                savepointId,
                CheckpointProperties.forSavepoint(false, SavepointFormatType.NATIVE),
                "foo",
                1337);

        final Set<ExecutionAttemptID> executionAttemptIDs =
                IterableUtils.toStream(executionGraph.getAllExecutionVertices())
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toSet());
        assertThat(executionAttemptIDs).hasSize(1);
        checkpointStatsTracker.reportInitializationMetrics(
                executionAttemptIDs.iterator().next(),
                new SubTaskInitializationMetricsBuilder(
                                SystemClock.getInstance().absoluteTimeMillis())
                        .build());

        assertThat(spans).hasSize(1);
    }

    private ExecutionGraphFactory createExecutionGraphFactory() {
        return createExecutionGraphFactory(
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
    }

    private ExecutionGraphFactory createExecutionGraphFactory(
            JobManagerJobMetricGroup metricGroup) {
        return new DefaultExecutionGraphFactory(
                new Configuration(),
                ClassLoader.getSystemClassLoader(),
                new DefaultExecutionDeploymentTracker(),
                EXECUTOR_EXTENSION.getExecutor(),
                EXECUTOR_EXTENSION.getExecutor(),
                Duration.ofMillis(0L),
                metricGroup,
                VoidBlobWriter.getInstance(),
                ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER,
                NoOpJobMasterPartitionTracker.INSTANCE);
    }

    @Nonnull
    private JobGraph createJobGraphWithSavepoint(
            boolean allowNonRestoredState, long savepointId, int parallelism) throws IOException {
        // create savepoint data
        final OperatorID operatorID = new OperatorID();
        final File savepointFile =
                TestUtils.createSavepointWithOperatorState(temporaryFile, savepointId, operatorID);

        // set savepoint settings which don't allow non restored state
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(
                        savepointFile.getAbsolutePath(), allowNonRestoredState);

        // create a new operator
        final JobVertex jobVertex = new JobVertex("New operator");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(parallelism);

        // this test will fail in the end due to the previously created Savepoint having a state for
        // a given OperatorID that does not match any operator of the newly created JobGraph
        return TestUtils.createJobGraphFromJobVerticesWithCheckpointing(
                savepointRestoreSettings, jobVertex);
    }
}
