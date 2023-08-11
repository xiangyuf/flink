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

package org.apache.flink.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gauge with tags. */
public class TagGauge implements Gauge<TagGaugeStore> {
    private static final Logger LOG = LoggerFactory.getLogger(TagGauge.class);

    private final TagGaugeStoreImpl store;

    TagGauge(
            int maxSize,
            boolean clearAfterReport,
            boolean clearWhenFull,
            MetricsReduceType metricsReduceType) {
        this.store =
                new TagGaugeStoreImpl(maxSize, clearAfterReport, clearWhenFull, metricsReduceType);
    }

    public void addMetric(Object metricValue, TagGaugeStoreImpl.TagValues tagValues) {
        if (metricValue instanceof Number) {
            store.addMetric(((Number) metricValue).doubleValue(), tagValues);
        } else if (metricValue instanceof String) {
            try {
                store.addMetric(Double.parseDouble((String) metricValue), tagValues);
            } catch (NumberFormatException exception) {
                LOG.info("Fail to parse double value, error string: {}", metricValue);
            }
        } else {
            // abandon
        }
    }

    public void reset() {
        store.reset();
    }

    @Override
    public TagGaugeStoreImpl getValue() {
        return store;
    }

    /** Reduce type for the metrics data with same tags. */
    public enum MetricsReduceType {
        NO_REDUCE,
        SUM,
        MAX,
        MIN,
        AVG
    }

    /** Build for {@link TagGauge}. */
    public static class TagGaugeBuilder {

        private int size = 1024;
        private boolean clearAfterReport = false;
        private boolean clearWhenFull = false;
        private MetricsReduceType metricsReduceType = MetricsReduceType.SUM;

        public TagGaugeBuilder() {}

        public TagGaugeBuilder setMaxSize(int maxSize) {
            this.size = maxSize;
            return this;
        }

        public TagGaugeBuilder setClearAfterReport(boolean clearAfterReport) {
            this.clearAfterReport = clearAfterReport;
            return this;
        }

        public TagGaugeBuilder setClearWhenFull(boolean clearWhenFull) {
            this.clearWhenFull = clearWhenFull;
            return this;
        }

        public TagGaugeBuilder setMetricsReduceType(MetricsReduceType metricsReduceType) {
            this.metricsReduceType = metricsReduceType;
            return this;
        }

        public TagGauge build() {
            return new TagGauge(size, clearAfterReport, clearWhenFull, metricsReduceType);
        }
    }
}
