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

package opentsdb;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.opentsdb.MetricEmitter;
import org.apache.flink.metrics.opentsdb.OpentsdbReporter;
import org.apache.flink.metrics.opentsdb.utils.Utils;
import org.apache.flink.metrics.util.TestMetricGroup;

import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** UT for OpentsdbReporter. */
public class OpentsdbReporterTest {

    private static final Pattern CHECK_PATTERN = Pattern.compile("[\\w-]+");

    @Test
    public void testReadWhitelist() {
        OpentsdbReporter reporter = new OpentsdbReporter();
        reporter.loadAllMetrics();

        Set<String> globalNeededMetrics = reporter.getGlobalNeededMetrics();

        Set<String> nonGlobalNeededMetrics = reporter.getNonGlobalNeededMetrics();

        Assert.assertTrue(globalNeededMetrics.size() > 0);
        Assert.assertTrue(nonGlobalNeededMetrics.size() > 0);

        globalNeededMetrics.forEach(
                metric -> Assert.assertTrue(CHECK_PATTERN.matcher(metric).matches()));
        nonGlobalNeededMetrics.forEach(
                metric -> Assert.assertTrue(CHECK_PATTERN.matcher(metric).matches()));
    }

    @Test
    public void testReadWhitelistWithFallback() {
        OpentsdbReporter reporter = new OpentsdbReporter();
        reporter.setWhitelistFile("not_exist");
        reporter.loadAllMetrics();

        Assert.assertTrue(reporter.getGlobalNeededMetrics().size() > 0);
        Assert.assertTrue(reporter.getNonGlobalNeededMetrics().size() > 0);
    }

    @Test
    public void testLoadFixedTags() {
        OpentsdbReporter reporter = new OpentsdbReporter();
        reporter.loadFixedTags("K1=V1,K2=V2,K3=V3,K4=V4=V6");
        Assert.assertEquals(reporter.getFixedTags().size(), 3);
    }

    @Test
    public void testOpen() {
        OpentsdbReporter reporter = new OpentsdbReporter();
        MetricConfig config = new MetricConfig();
        config.put("jobname", "HelloWorld");
        config.put("prefix", "flink");
        reporter.open(config);
        Assert.assertEquals("HelloWorld", reporter.getJobName());
        Assert.assertEquals("flink", reporter.getPrefix());
        Assert.assertEquals(true, reporter.getGlobalNeededMetrics().contains("fullRestarts"));
        Assert.assertEquals(true, reporter.getNonGlobalNeededMetrics().contains("downtime"));
    }

    @Test
    public void testPrefix() {
        OpentsdbReporter reporter = new OpentsdbReporter();
        Assert.assertEquals("Hello.World", Utils.prefix("Hello", "World"));
    }

    @Test
    public void testGetMetricNameAndTags1() {
        OpentsdbReporter reporter = new OpentsdbReporter();
        reporter.loadAllMetrics();
        reporter.open(new MetricConfig());
        Map<String, String> variableMap = new HashMap<>();
        variableMap.put("key", "value");
        String metricName = "downtime";
        Tags except = Tags.merge(Tags.keyValues(variableMap), Tags.keyValues("jobname", "flink"));
        MetricGroup metricGroup =
                TestMetricGroup.newBuilder()
                        .setLogicalScopeFunction(
                                (characterFilter, character) ->
                                        characterFilter.filterCharacters("logicalScope"))
                        .setVariables(variableMap)
                        .build();
        Counter counter = new SimpleCounter();
        counter.inc(1);
        final MetricEmitter metricEmitter =
                reporter.generateMetricEmitter(counter, metricName, metricGroup);

        Assert.assertEquals("job.downtime", metricEmitter.getGlobalMetricName());
        Assert.assertEquals("logicalScope.downtime", metricEmitter.getNonGlobalMetricName());
        Assert.assertEquals(except, metricEmitter.getTags());
    }
}
