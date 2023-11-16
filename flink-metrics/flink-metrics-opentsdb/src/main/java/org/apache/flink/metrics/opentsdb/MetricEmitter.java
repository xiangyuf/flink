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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.opentsdb.utils.Utils;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Emitter to emit metrics. */
public class MetricEmitter {

    private final Logger log = LoggerFactory.getLogger(OpentsdbReporter.class);

    private Metric metric;
    private Tags tags;
    private String globalMetricName;

    private String nonGlobalMetricName;

    private SimpleByteTSDMetricsIntegration client;

    public MetricEmitter(
            Metric metric,
            Tags tags,
            SimpleByteTSDMetricsIntegration client,
            String globalMetricName,
            String nonGlobalMetricName) {
        this.metric = metric;
        this.tags = tags;
        this.client = client;
        this.globalMetricName = globalMetricName;
        this.nonGlobalMetricName = nonGlobalMetricName;
    }

    public void report() {
        try {
            switch (metric.getMetricType()) {
                case COUNTER:
                    long value = ((Counter) metric).getCount();
                    emitValue(nonGlobalMetricName, globalMetricName, value, tags);
                    return;
                case GAUGE:
                    Object gaugeValue = ((Gauge) metric).getValue();
                    if (gaugeValue instanceof Number) {
                        double d = ((Number) gaugeValue).doubleValue();
                        emitValue(nonGlobalMetricName, globalMetricName, d, tags);
                    } else if (gaugeValue instanceof String) {
                        double d = Double.parseDouble((String) gaugeValue);
                        emitValue(nonGlobalMetricName, globalMetricName, d, tags);
                    } else if (gaugeValue instanceof TagGaugeStore) {
                        log.error(
                                "The value of Metric {} is TagGaugeStore, but it's metricType is not TAG_GAUGE."
                                        + " this metric will be ignored.",
                                Utils.prefix(nonGlobalMetricName, globalMetricName));
                    } else {
                        log.warn(
                                "can't handle the type guage, the gaugeValue type is {}, the gauge name is {}",
                                gaugeValue.getClass(),
                                Utils.prefix(nonGlobalMetricName, globalMetricName));
                    }
                    return;
                case TAG_GAUGE:
                    TagGaugeStore tagGaugeStoreValue = ((TagGauge) metric).getValue();
                    final List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics =
                            tagGaugeStoreValue.getMetricValuesList();
                    final Map<Tags, Double> compositedMetrics = new HashMap<>();
                    for (TagGaugeStore.TagGaugeMetric tagGaugeMetric : tagGaugeMetrics) {
                        final Tags compositeTags =
                                Tags.merge(
                                        tags,
                                        Tags.keyValues(
                                                tagGaugeMetric.getTagValues().getTagValues()));
                        compositedMetrics.put(compositeTags, tagGaugeMetric.getMetricValue());
                    }

                    // send composited metrics
                    for (Map.Entry<Tags, Double> entry : compositedMetrics.entrySet()) {
                        emitValue(
                                nonGlobalMetricName,
                                globalMetricName,
                                entry.getValue(),
                                entry.getKey());
                    }
                    tagGaugeStoreValue.metricReported();
                    return;
                case HISTOGRAM:
                    Histogram histogram = (Histogram) metric;
                    HistogramStatistics statistics = histogram.getStatistics();
                    emitValue(
                            Utils.addSuffix(nonGlobalMetricName, "mean"),
                            Utils.addSuffix(globalMetricName, "mean"),
                            statistics.getMean(),
                            tags);
                    emitValue(
                            Utils.addSuffix(nonGlobalMetricName, "p99"),
                            Utils.addSuffix(globalMetricName, "p99"),
                            statistics.getQuantile(0.99),
                            tags);
                    emitValue(
                            Utils.addSuffix(nonGlobalMetricName, "max"),
                            Utils.addSuffix(globalMetricName, "max"),
                            statistics.getMax(),
                            tags);
                    return;
                case METER:
                    Meter meter = (Meter) metric;
                    emitValue(
                            Utils.addSuffix(nonGlobalMetricName, "rate"),
                            Utils.addSuffix(globalMetricName, "rate"),
                            meter.getRate(),
                            tags);
                    emitValue(
                            Utils.addSuffix(nonGlobalMetricName, "count"),
                            Utils.addSuffix(globalMetricName, "count"),
                            meter.getRate(),
                            tags);
                    return;
                default:
                    log.warn(
                            "Cannot add unknown metric type {}. This indicates that the reporter "
                                    + "does not support this metric type.",
                            metric.getClass().getName());
            }
        } catch (Exception e) {
            log.error(
                    "report metric fail, non metric name: {}, global metric name: {} ",
                    nonGlobalMetricName,
                    globalMetricName,
                    e);
        }
    }

    public Tags getTags() {
        return tags;
    }

    public String getGlobalMetricName() {
        return globalMetricName;
    }

    public String getNonGlobalMetricName() {
        return nonGlobalMetricName;
    }

    private void emitValue(
            String nonGlobalMetricName, String globalMetricName, double value, Tags tags) {
        if (!StringUtils.isNullOrWhitespaceOnly(nonGlobalMetricName)) {
            client.emitStore(nonGlobalMetricName, value, tags);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(globalMetricName)) {
            client.emitStore(globalMetricName, value, tags);
        }
    }
}
