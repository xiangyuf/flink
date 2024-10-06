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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.TernaryBoolean;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

/** Tests for the partitioned state part of {@link EmbeddedRocksDBStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
public class RocksDBStateBackendMigrationTest
        extends StateBackendMigrationTestBase<EmbeddedRocksDBStateBackend> {

    @Parameters(name = "Incremental checkpointing: {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameter public boolean enableIncrementalCheckpointing;

    // Store it because we need it for the cleanup test.
    private String dbPath;

    @Override
    protected EmbeddedRocksDBStateBackend getStateBackend() throws IOException {
        dbPath = TempDirUtils.newFolder(tempFolder).getAbsolutePath();
        EmbeddedRocksDBStateBackend backend =
                new EmbeddedRocksDBStateBackend(
                        TernaryBoolean.fromBoolean(enableIncrementalCheckpointing));

        Configuration configuration = new Configuration();
        configuration.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        backend.setDbStoragePath(dbPath);
        return backend;
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        String checkpointPath = TempDirUtils.newFolder(tempFolder).toURI().toString();
        return new FileSystemCheckpointStorage(checkpointPath);
    }

    @TestTemplate
    protected void testStateMigrationAfterChangingTTLFromDisablingToEnabling() throws Exception {
        final String stateName = "test-ttl";

        ValueStateDescriptor<TestType> initialAccessDescriptor =
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());
        ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore =
                new ValueStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer());
        newAccessDescriptorAfterRestore.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(1)).build());

        ListStateDescriptor<TestType> initialAccessListDescriptor =
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());
        ListStateDescriptor<TestType> newAccessListDescriptorAfterRestore =
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer());
        newAccessListDescriptorAfterRestore.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(1)).build());

        MapStateDescriptor<Integer, TestType> initialAccessMapDescriptor =
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer());
        MapStateDescriptor<Integer, TestType> newAccessMapDescriptorAfterRestore =
                new MapStateDescriptor<>(
                        stateName,
                        IntSerializer.INSTANCE,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer());
        newAccessMapDescriptorAfterRestore.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(1)).build());

        testKeyedValueStateUpgrade(initialAccessDescriptor, newAccessDescriptorAfterRestore);
        testKeyedListStateUpgrade(initialAccessListDescriptor, newAccessListDescriptorAfterRestore);
        testKeyedMapStateUpgrade(initialAccessMapDescriptor, newAccessMapDescriptorAfterRestore);
    }

    @TestTemplate
    protected void testStateMigrationAfterChangingTTLFromEnablingToDisabling() throws Exception {
        final String stateName = "test-ttl";

        ValueStateDescriptor<TestType> initialAccessDescriptor =
                new ValueStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());
        initialAccessDescriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(1)).build());
        ValueStateDescriptor<TestType> newAccessDescriptorAfterRestore =
                new ValueStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer());

        ListStateDescriptor<TestType> initialAccessListDescriptor =
                new ListStateDescriptor<>(stateName, new TestType.V1TestTypeSerializer());
        initialAccessListDescriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(1)).build());
        ListStateDescriptor<TestType> newAccessListDescriptorAfterRestore =
                new ListStateDescriptor<>(
                        stateName,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer());

        MapStateDescriptor<Integer, TestType> initialAccessMapDescriptor =
                new MapStateDescriptor<>(
                        stateName, IntSerializer.INSTANCE, new TestType.V1TestTypeSerializer());
        initialAccessMapDescriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(Duration.ofDays(1)).build());
        MapStateDescriptor<Integer, TestType> newAccessMapDescriptorAfterRestore =
                new MapStateDescriptor<>(
                        stateName,
                        IntSerializer.INSTANCE,
                        // restore with a V2 serializer that has a different schema
                        new TestType.V2TestTypeSerializer());

        testKeyedValueStateUpgrade(initialAccessDescriptor, newAccessDescriptorAfterRestore);
        testKeyedListStateUpgrade(initialAccessListDescriptor, newAccessListDescriptorAfterRestore);
        testKeyedMapStateUpgrade(initialAccessMapDescriptor, newAccessMapDescriptorAfterRestore);
    }
}
