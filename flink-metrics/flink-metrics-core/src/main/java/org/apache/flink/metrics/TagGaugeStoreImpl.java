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

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------

package org.apache.flink.metrics;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

/** Store for {@link TagGaugeImpl}. */
public class TagGaugeStoreImpl implements TagGaugeStore {

    private final int maxSize;

    private final List<TagGaugeMetric> metricValuesList;

    private final boolean clearAfterReport;

    private final boolean clearWhenFull;

    private final TagGaugeImpl.MetricsReduceType metricsReduceType;

    public TagGaugeStoreImpl(
            int maxSize,
            boolean clearAfterReport,
            boolean clearWhenFull,
            TagGaugeImpl.MetricsReduceType metricsReduceType) {
        this.maxSize = maxSize;
        this.metricValuesList = new LinkedList<>();
        this.clearAfterReport = clearAfterReport;
        this.clearWhenFull = clearWhenFull;
        this.metricsReduceType = metricsReduceType;
    }

    public void addMetric(double metricValue, TagValues tagValues) {

        if (maxSize <= 0) {
            return;
        }

        if (metricValuesList.size() == maxSize) {
            if (clearWhenFull) {
                metricValuesList.clear();
            } else {
                if (metricValuesList.size() > 0) {
                    metricValuesList.remove(0);
                }
            }
        }

        metricValuesList.add(new TagGaugeMetric(metricValue, tagValues));
    }

    public boolean isClearAfterReport() {
        return clearAfterReport;
    }

    public TagGaugeImpl.MetricsReduceType getMetricsReduceType() {
        return metricsReduceType;
    }

    public void reset() {
        metricValuesList.clear();
    }

    public List<TagGaugeMetric> getMetricValuesList() {
        switch (this.metricsReduceType) {
            case SUM:
                return metricValuesList.stream()
                        .collect(
                                Collectors.groupingBy(
                                        metrics -> metrics.getTagValues().getTagValues()))
                        .values()
                        .stream()
                        .map(
                                tagGaugeMetrics ->
                                        tagGaugeMetrics.stream()
                                                .reduce(
                                                        (metric1, metric2) ->
                                                                new TagGaugeMetric(
                                                                        metric1.getMetricValue()
                                                                                + metric2
                                                                                        .getMetricValue(),
                                                                        metric1.getTagValues())))
                        .map(Optional::get)
                        .collect(Collectors.toList());
            case MAX:
                return metricValuesList.stream()
                        .collect(
                                Collectors.groupingBy(
                                        metrics -> metrics.getTagValues().getTagValues()))
                        .values()
                        .stream()
                        .map(
                                tagGaugeMetrics ->
                                        tagGaugeMetrics.stream()
                                                .reduce(
                                                        (metric1, metric2) ->
                                                                new TagGaugeMetric(
                                                                        Math.max(
                                                                                metric1
                                                                                        .getMetricValue(),
                                                                                metric2
                                                                                        .getMetricValue()),
                                                                        metric1.getTagValues())))
                        .map(Optional::get)
                        .collect(Collectors.toList());
            case MIN:
                return metricValuesList.stream()
                        .collect(
                                Collectors.groupingBy(
                                        metrics -> metrics.getTagValues().getTagValues()))
                        .values()
                        .stream()
                        .map(
                                tagGaugeMetrics ->
                                        tagGaugeMetrics.stream()
                                                .reduce(
                                                        (metric1, metric2) ->
                                                                new TagGaugeMetric(
                                                                        Math.min(
                                                                                metric1
                                                                                        .getMetricValue(),
                                                                                metric2
                                                                                        .getMetricValue()),
                                                                        metric1.getTagValues())))
                        .map(Optional::get)
                        .collect(Collectors.toList());
            case AVG:
                return metricValuesList.stream()
                        .collect(
                                Collectors.groupingBy(
                                        metrics -> metrics.getTagValues().getTagValues()))
                        .values()
                        .stream()
                        .map(
                                tagGaugeMetrics -> {
                                    OptionalDouble average =
                                            tagGaugeMetrics.stream()
                                                    .mapToDouble(TagGaugeMetric::getMetricValue)
                                                    .average();
                                    Optional<TagGaugeMetric> optionalTagGaugeMetric =
                                            tagGaugeMetrics.stream().findAny();
                                    return new TagGaugeMetric(
                                            average.getAsDouble(),
                                            optionalTagGaugeMetric.get().getTagValues());
                                })
                        .collect(Collectors.toList());
            case NO_REDUCE:
                return metricValuesList;
            default:
                throw new RuntimeException(
                        "Unknown MetricsReduceType " + this.metricsReduceType + " for TagGauge");
        }
    }

    @Override
    public void metricReported() {
        if (isClearAfterReport()) {
            reset();
        }
    }
}
