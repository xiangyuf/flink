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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Interface of Store for {@link TagGauge}. */
public interface TagGaugeStore {

    List<TagGaugeMetric> getMetricValuesList();

    default void metricReported() {}

    /** TagValues. */
    class TagValues {

        private final Map<String, String> tagValues;

        TagValues(Map<String, String> tagValues) {
            this.tagValues = tagValues;
        }

        public Map<String, String> getTagValues() {
            return tagValues;
        }
    }

    /** Build for TagValues. */
    class TagValuesBuilder {

        private final Map<String, String> tagValuesMap;

        public TagValuesBuilder() {
            this.tagValuesMap = new HashMap<>(8);
        }

        public TagValuesBuilder addTagValue(String tag, String value) {
            tagValuesMap.put(tag, value);
            return this;
        }

        public TagValues build() {
            return new TagValues(tagValuesMap);
        }
    }

    /** TagGaugeMetric. */
    class TagGaugeMetric {

        private final double metricValue;
        private final TagValues tagValues;

        public TagGaugeMetric(double metricValue, TagValues tagValues) {
            this.metricValue = metricValue;
            this.tagValues = tagValues;
        }

        public double getMetricValue() {
            return metricValue;
        }

        public TagValues getTagValues() {
            return tagValues;
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TagGaugeMetric tagGaugeMetrics = (TagGaugeMetric) obj;
            return this.getMetricValue() == tagGaugeMetrics.getMetricValue()
                    && this.getTagValues()
                            .getTagValues()
                            .equals(tagGaugeMetrics.getTagValues().getTagValues());
        }

        public int hashCode() {
            return Objects.hash(metricValue, tagValues.getTagValues());
        }
    }
}
