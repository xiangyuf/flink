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

package org.apache.flink.metrics.opentsdb;

import org.apache.flink.shaded.byted.com.bytedance.metrics.simple.SimpleByteTSDMetrics;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.core.container.ServiceCollection;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.core.container.ServiceRegistrar;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.core.container.TypedService;

/** ServiceRegistrar for metric4j. */
public class Metrics4jIntegrationServiceRegistrar implements ServiceRegistrar {

    @Override
    public void register(ServiceCollection services) {
        services.addService(
                TypedService.Scoped(
                        SimpleByteTSDMetrics.class, SimpleByteTSDMetricsIntegration.class));
    }
}
