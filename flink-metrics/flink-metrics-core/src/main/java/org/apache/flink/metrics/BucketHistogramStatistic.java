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
import java.util.Map;

/** Histogram statistics implementation returned by {@link TagBucketHistogram.BucketHistogram}. */
public class BucketHistogramStatistic extends HistogramStatistics {

    private final int size;
    private final Map<Double, Double> quantiles;

    public BucketHistogramStatistic(long[] buckets, int[] cnt, int size, double[] quantiles) {
        this.size = size;
        this.quantiles = getQuantiles(buckets, cnt, size, quantiles);
    }

    public Map<Double, Double> getQuantiles() {
        return quantiles;
    }

    /**
     * Returns the value for the given quantile based on the represented histogram statistics.
     *
     * @param quantile Quantile to calculate the value of
     * @return the value for the given quantile
     */
    @Override
    public double getQuantile(double quantile) {
        return quantiles.getOrDefault(quantile, -1.0);
    }

    /**
     * Returns the elements of the statistics' sample.
     *
     * @return Elements of the statistics' sample
     */
    @Override
    public long[] getValues() {
        throw new UnsupportedOperationException("BucketHistogramStatistic doesn't store values");
    }

    /**
     * Returns the size of the statistics' sample.
     *
     * @return Size of the statistics' sample
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns the mean of the histogram values.
     *
     * @return Mean of the histogram values
     */
    @Override
    public double getMean() {
        return quantiles.getOrDefault(0.5, -1.0);
    }

    /**
     * Returns the standard deviation of the distribution reflected by the histogram statistics.
     *
     * @return Standard deviation of histogram distribution
     */
    @Override
    public double getStdDev() {
        throw new UnsupportedOperationException(
                "Since BucketHistogramStatistic doesn't store values,"
                        + "standard deviation is not calculated.");
    }

    /**
     * Returns the maximum value of the histogram.
     *
     * @return Maximum value of the histogram
     */
    @Override
    public long getMax() {
        throw new UnsupportedOperationException(
                "Since BucketHistogramStatistic doesn't store values,"
                        + "maximum value is not calculated.");
    }

    /**
     * Returns the minimum value of the histogram.
     *
     * @return Minimum value of the histogram
     */
    @Override
    public long getMin() {
        throw new UnsupportedOperationException(
                "Since BucketHistogramStatistic doesn't store values,"
                        + "minimum value is not calculated.");
    }

    private static Map<Double, Double> getQuantiles(
            long[] buckets, int[] cnt, int size, double[] quantiles) {
        Map<Double, Double> quantileMap = new HashMap<>();
        long sum = 0;
        int qIdx = 0; // index of quantiles
        int target = (int) Math.ceil(quantiles[qIdx] * size);
        for (int i = 0; i < cnt.length; i++) {
            sum += cnt[i];
            if (sum >= target) {
                double value = getBucketMean(buckets, i);
                quantileMap.put(quantiles[qIdx], value);
                if (++qIdx == quantiles.length) {
                    return quantileMap;
                }

                target = (int) Math.ceil(quantiles[qIdx] * size);
                while (sum >= target) {
                    quantileMap.put(quantiles[qIdx], value);
                    if (++qIdx == quantiles.length) {
                        return quantileMap;
                    }
                    target = (int) Math.ceil(quantiles[qIdx] * size);
                }
            }
        }
        return quantileMap;
    }

    /**
     * Return mean value of the bucket.
     *
     * @param buckets a series of buckets
     * @param pos position of the target bucket
     * @return mean value of the bucket
     */
    protected static double getBucketMean(long[] buckets, int pos) {
        if (pos + 1 == buckets.length) { // at the interval which values >= buckets[pos]
            return buckets[pos];
        } else {
            return (buckets[pos] + buckets[pos + 1]) / 2.0;
        }
    }
}
