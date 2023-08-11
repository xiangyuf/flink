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
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Gauge;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Metric;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.MetricRegistry;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.MetricType;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.core.container.FromServices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.shaded.byted.com.bytedance.metrics2.api.MetricType.GAUGE;

/** Proxy for SimpleByteTSDMetrics client. */
public class SimpleByteTSDMetricsIntegration extends SimpleByteTSDMetrics {

    private static final Logger LOG =
            LoggerFactory.getLogger(SimpleByteTSDMetricsIntegration.class);

    private final MetricRegistry metricRegistry;
    private final Map<String, Metric> metrics = new ConcurrentHashMap<>();

    private static final int METRICS_NAME_MAX_LENGTH = 255;

    public SimpleByteTSDMetricsIntegration(@FromServices MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public void emitStore(String metric, double value, Tags tags) {
        if (metric != null && metric.length() > METRICS_NAME_MAX_LENGTH) {
            LOG.warn("{} metricName length large than {}", metric, METRICS_NAME_MAX_LENGTH);
        }
        Gauge gauge = (Gauge) metrics.computeIfAbsent(metric, name -> newMetric(metric, GAUGE));
        gauge.withTags(tags).record(value);
    }

    @Override
    public void emitStore(String metric, double value, String tags) {}

    @Override
    public void emitTsStore(String s, double v, String s1, long l) {}

    @Override
    public void emitTimer(String metric, double value, String tags) {}

    @Override
    public void emitCounter(String metric, double value, String tags) {}

    @Override
    public void emitRateCounter(String metric, double value, String tags) {}

    @Override
    public void emitMeter(String metric, double value, String tags) {}

    private Metric newMetric(String name, MetricType counter) {
        Metric metric = null;
        switch (counter) {
            case COUNTER:
                metric =
                        metricRegistry
                                .counter()
                                .name(name)
                                .defineSuffix("")
                                .requireTagsValidated(false)
                                .build();
                break;
            case RATE_COUNTER:
                metric =
                        metricRegistry
                                .counter()
                                .asRateCounter()
                                .name(name)
                                .defineSuffix("")
                                .requireTagsValidated(false)
                                .build();
                break;
            case METER:
                metric =
                        metricRegistry
                                .counter()
                                .asMeter()
                                .name(name)
                                .defineSuffix("")
                                .requireTagsValidated(false)
                                .build();
                break;
            case TIMER:
                metric =
                        metricRegistry
                                .timer()
                                .name(name)
                                .defineSuffix("")
                                .useCKMS(false)
                                .requireTagsValidated(false)
                                .build();
                break;
            case GAUGE:
                metric =
                        metricRegistry
                                .gauge()
                                .name(name)
                                .defineSuffix("")
                                .requireTagsValidated(false)
                                .build();
                break;
            default:
        }
        return metric;
    }
}
