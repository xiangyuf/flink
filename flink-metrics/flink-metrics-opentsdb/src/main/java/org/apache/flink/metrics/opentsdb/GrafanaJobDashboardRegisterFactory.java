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

package org.apache.flink.metrics.opentsdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.dashboard.JobDashboardRegister;
import org.apache.flink.streaming.runtime.dashboard.JobDashboardRegisterFactory;

/** Registration for the grafana dashboard. */
public class GrafanaJobDashboardRegisterFactory implements JobDashboardRegisterFactory {

    /**
     * @param jobName jobName of dashboard.
     * @param jobConfig config of job.
     * @return Register for registering job grafana dashboard.
     */
    public JobDashboardRegister createJobDashboardRegister(
            String jobName, Configuration jobConfig) {
        return new GrafanaJobDashboardRegister(jobName, jobConfig);
    }
}
