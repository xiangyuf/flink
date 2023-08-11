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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Histogram with tags.
 *
 * <p>Note: it shouldn't be used as normal histogram, {@link #update(long)} and {@link
 * #getStatistics()} are not supposed to be used. TODO：rewrite it
 */
public class TagBucketHistogram implements Histogram {

    // max number of tags
    private static final int MAX_TAG_NUMBERS = 100;

    // default bucket list
    public static final long[] DEFAULT_BUCKETS = {
        0L, 60L, 120L, 180L, 240L, 300L, 360L, 420L, 480L, 540L, 600L, 660L, 720L, 780L, 840L, 900L
    };

    // default quantile list, that is TP50/TP90/TP95/TP99
    public static final double[] DEFAULT_QUANTILES = {0.5, 0.9, 0.95, 0.99};

    // empty tag
    private static final Map<String, String> EMPTY_TAG = new HashMap<>();

    // bucket list
    private final long[] buckets;

    // quantile list
    private final double[] quantiles;

    // histograms with different tags
    private final Map<Map<String, String>, BucketHistogram> tagHistograms;

    // properties of histograms
    private final Map<String, String> props;

    private TagBucketHistogram(long[] buckets, double[] quantiles, Map<String, String> props) {
        this.buckets = addLeadingZeroIfNot(buckets);
        this.quantiles = quantiles;
        this.tagHistograms = new ConcurrentHashMap<>();
        this.props = props;
    }

    /**
     * Update the histogram with the given value and empty tag.
     *
     * @param value Value to update the histogram with
     */
    @Override
    @Deprecated
    public void update(long value) {
        tagHistograms.compute(
                EMPTY_TAG,
                (k, v) -> {
                    if (v == null) {
                        v = new BucketHistogram(buckets, quantiles);
                    }
                    v.update(value);
                    return v;
                });
    }

    /**
     * Update the histogram with the given value and given tags.
     *
     * @param value Value to update the histogram with
     * @param tags Tags associated with the value
     */
    public void update(long value, Map<String, String> tags) {
        // if reach max number of tags, return immediately
        if (!tagHistograms.containsKey(tags) && tagHistograms.size() == MAX_TAG_NUMBERS) {
            return;
        }
        tagHistograms.compute(
                tags,
                (k, v) -> {
                    if (v == null) {
                        v = new BucketHistogram(buckets, quantiles);
                    }
                    v.update(value);
                    return v;
                });
    }

    /**
     * Get the count of seen elements with empty tag.
     *
     * @return Count of seen elements
     */
    @Override
    public long getCount() {
        return tagHistograms.containsKey(EMPTY_TAG) ? tagHistograms.get(EMPTY_TAG).getCount() : 0;
    }

    /**
     * Create statistics for the currently recorded elements of empty tag.
     *
     * @return Statistics about the currently recorded elements of empty tag
     */
    @Override
    @Deprecated
    public HistogramStatistics getStatistics() {
        return tagHistograms.containsKey(EMPTY_TAG)
                ? tagHistograms.get(EMPTY_TAG).getStatistics()
                : null;
    }

    public Map<Map<String, String>, BucketHistogramStatistic> getStatisticsOfAllTags() {
        Map<Map<String, String>, BucketHistogramStatistic> statistics = new HashMap<>();
        for (Map.Entry<Map<String, String>, BucketHistogram> entry : tagHistograms.entrySet()) {
            Map<String, String> tags = entry.getKey();
            BucketHistogram histogram = tagHistograms.remove(tags);
            statistics.put(tags, (BucketHistogramStatistic) histogram.getStatistics());
        }
        return statistics;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public static TagBucketHistogramBuilder builder() {
        return new TagBucketHistogramBuilder();
    }

    private static long[] addLeadingZeroIfNot(long[] buckets) {
        long[] res;
        int s = 0;
        if (buckets[0] != 0) {
            res = new long[buckets.length + 1];
            s = 1;
        } else {
            res = new long[buckets.length];
        }
        System.arraycopy(buckets, 0, res, s, buckets.length);
        return res;
    }

    /** Builder class of {@link BucketHistogram}. */
    public static class TagBucketHistogramBuilder {
        private long[] buckets = DEFAULT_BUCKETS;
        private double[] quantiles = DEFAULT_QUANTILES;
        private Map<String, String> props = null;

        public TagBucketHistogramBuilder setBuckets(List<Long> buckets) {
            this.buckets = buckets.stream().mapToLong(l -> l).toArray();
            return this;
        }

        public TagBucketHistogramBuilder setQuantiles(List<Double> quantiles) {
            this.quantiles = quantiles.stream().mapToDouble(d -> d).toArray();
            return this;
        }

        public TagBucketHistogramBuilder setProps(Map<String, String> props) {
            this.props = props;
            return this;
        }

        public TagBucketHistogram build() {
            Arrays.sort(buckets);
            Arrays.sort(quantiles);
            return new TagBucketHistogram(buckets, quantiles, props);
        }
    }

    /**
     * A histogram with buckets aimed to calculate percentiles of data.
     *
     * <p>The histogram is not thread safe, no other thread should update it when {@link
     * #getStatistics()} is called.
     *
     * <p>Note that the buckets should start from zero and every bucket is left point inclusive and
     * right point exclusive.
     *
     * <p>For example, the buckets array is [0, 3, 5, 10] and the interval it represents is: [0, 3),
     * [3, 5), [5, 10), [10, +infinity).
     */
    static class BucketHistogram implements Histogram {

        // the bucket list, the leading number should be zero.
        private final long[] buckets;

        // the list of quantiles to be calculated.
        private final double[] quantiles;

        // number of elements that fall into the corresponding bucket
        private final int[] cnt;

        // total number of elements
        private int total = 0;

        /**
         * Initialize histogram with buckets and quantiles.
         *
         * <p>Note: buckets should be sorted and leading number should be zero and quantiles should
         * also be sorted.
         *
         * @param buckets bucket list
         * @param quantiles quantile list
         */
        BucketHistogram(long[] buckets, double[] quantiles) {
            assert buckets[0] == 0;
            this.buckets = buckets;
            this.quantiles = quantiles;
            this.cnt = new int[this.buckets.length];
        }

        /**
         * Update the histogram with the given value.
         *
         * @param value Value to update the histogram with
         */
        @Override
        public void update(long value) {
            int pos = Arrays.binarySearch(buckets, value);
            pos = pos < 0 ? (-pos - 2) : pos;
            if (pos >= cnt.length) {
                pos = cnt.length - 1;
            }
            cnt[pos]++;
            total++;
        }

        /**
         * Get the count of seen elements.
         *
         * @return Count of seen elements
         */
        @Override
        public long getCount() {
            return total;
        }

        /**
         * Create statistics for the currently recorded elements. Make sure no other thread update
         * it when the method is called.
         *
         * @return Statistics about the currently recorded elements
         */
        @Override
        public HistogramStatistics getStatistics() {
            return new BucketHistogramStatistic(buckets, cnt, total, quantiles);
        }
    }
}
