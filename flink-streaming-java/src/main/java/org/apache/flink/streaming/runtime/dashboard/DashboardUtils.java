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

package org.apache.flink.streaming.runtime.dashboard;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.plugin.PluginManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/** Utils for Dashboard related. */
public class DashboardUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DashboardUtils.class);

    public static void registerJobDashboard(
            String jobName, Configuration configuration, PluginManager pluginManager) {

        final Iterator<JobDashboardRegisterFactory> factoryIterator =
                pluginManager.load(JobDashboardRegisterFactory.class);
        final List<String> factories =
                configuration.get(MetricOptions.DASHBOARD_REGISTER_FACTORY_CLASSES);
        while (factoryIterator.hasNext()) {
            final JobDashboardRegisterFactory jobDashboardRegisterFactory = factoryIterator.next();
            if (factories.contains(jobDashboardRegisterFactory.getClass().getName())) {
                LOGGER.info("register dashboard:{}", jobDashboardRegisterFactory.getClass());
                try {
                    final JobDashboardRegister jobDashboardRegister =
                            jobDashboardRegisterFactory.createJobDashboardRegister(
                                    jobName, configuration);
                    jobDashboardRegister.registerDashboard();
                } catch (Exception e) {
                    LOGGER.error(
                            "{} register dashboard failed.",
                            jobDashboardRegisterFactory.getClass().getSimpleName(),
                            e);
                }
            }
        }
    }
}
