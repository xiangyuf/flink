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

import org.apache.flink.metrics.Metric;

import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;

/** Builder for metricEmitter. */
public final class MetricEmitterBuilder {
    private Metric metric;
    private Tags tags;
    private String globalMetricName;
    private String nonGlobalMetricName;
    private SimpleByteTSDMetricsIntegration client;

    public MetricEmitterBuilder() {}

    public MetricEmitterBuilder withMetric(Metric metric) {
        this.metric = metric;
        return this;
    }

    public MetricEmitterBuilder withTags(Tags tags) {
        this.tags = tags;
        return this;
    }

    public MetricEmitterBuilder withGlobalMetricName(String globalMetricName) {
        this.globalMetricName = globalMetricName;
        return this;
    }

    public MetricEmitterBuilder withNonGlobalMetricName(String nonGlobalMetricName) {
        this.nonGlobalMetricName = nonGlobalMetricName;
        return this;
    }

    public MetricEmitterBuilder withClient(SimpleByteTSDMetricsIntegration client) {
        this.client = client;
        return this;
    }

    public MetricEmitter build() {
        return new MetricEmitter(metric, tags, client, globalMetricName, nonGlobalMetricName);
    }
}
