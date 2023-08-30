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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

/**
 * Base class for per-job cluster entry points.
 *
 * @deprecated Per-job mode has been deprecated in Flink 1.15 and will be removed in the future.
 *     Please use application mode instead.
 */
@Deprecated
public abstract class JobClusterEntrypoint extends ClusterEntrypoint {

    public JobClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected Configuration generateClusterConfiguration(Configuration configuration) {
        Configuration generatedConfig = super.generateClusterConfiguration(configuration);
        if (!HighAvailabilityServicesUtils.isJobRecoveryEnable(configuration)) {
            LOG.warn(
                    "The recovery strategy of job manager is enforced to be {} in per-job mode.",
                    HighAvailabilityOptions.RecoverStrategy.RECOVER_JOBS);
            generatedConfig.set(
                    HighAvailabilityOptions.HA_JOB_MANAGER_RECOVERY_STRATEGY,
                    HighAvailabilityOptions.RecoverStrategy.RECOVER_JOBS);
        }
        return generatedConfig;
    }

    @Override
    protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
            Configuration configuration, ScheduledExecutor scheduledExecutor) {
        return new MemoryExecutionGraphInfoStore();
    }
}
