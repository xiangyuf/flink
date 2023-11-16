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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.opentsdb.utils.Utils;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.byted.com.bytedance.metrics.simple.SimpleByteTSDMetrics;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.MetricClient;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.MetricRegistryConfig;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.sdk.byted.BytedanceMetricClientBuilder;
import org.apache.flink.shaded.byted.com.bytedance.metrics2.sdk.byted.BytedanceRegistryConfig;
import org.apache.flink.shaded.byted.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_JOB_ID;
import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_OPERATOR_ID;
import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_OPERATOR_NAME;
import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_TASK_ATTEMPT_ID;
import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_TASK_NAME;
import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_TASK_VERTEX_ID;

/** Metrics reporter for ByteDance OpenTSDB. */
public class OpentsdbReporter implements MetricReporter, Scheduled {

    private final Logger log = LoggerFactory.getLogger(OpentsdbReporter.class);

    private SimpleByteTSDMetricsIntegration client;
    private String jobName;
    private String prefix; // It is the prefix of all metric and used in MetricsClient's constructor
    private static final String DEFAULT_METRICS_WHITELIST_FILE = "metrics-whitelist.yaml";
    private String whitelistFile;
    private final Map<String, String> fixedTags = new HashMap<>();

    private final Set<String> nonGlobalNeededMetrics = new HashSet<>();

    private static final int METRICS_REPORTER_MAX_METRIC_PER_REGISTRY_DEFAULT = 10240;

    private static final char SCOPE_SEPARATOR = '.';

    private final Map<Metric, MetricEmitter> metricCollectorMap = new ConcurrentHashMap<>();

    private final Set<String> noNeededScope =
            Sets.newHashSet(
                    SCOPE_JOB_ID, SCOPE_TASK_VERTEX_ID, SCOPE_TASK_ATTEMPT_ID, SCOPE_OPERATOR_ID);

    // *************************************************************************
    //     Global Aggregated Metric (add metric name below if needed)
    // *************************************************************************

    private static final String GLOBAL_PREFIX = "job";

    // Global metrics
    private final Set<String> globalNeededMetrics = new HashSet<>();

    @Override
    public void open(MetricConfig config) {
        this.prefix = config.getString("prefix", "flink");
        this.client = createMetricClient();
        this.jobName = config.getString("jobname", "flink");
        this.whitelistFile = config.getString("whitelist_file", DEFAULT_METRICS_WHITELIST_FILE);
        String tagString = config.getString("fixed_tags", null);
        loadFixedTags(tagString);
        log.info(
                "prefix = {} jobName = {} whitelistFile = {} fixedTags = {}",
                this.prefix,
                this.jobName,
                this.whitelistFile,
                this.fixedTags);
        loadAllMetrics();
    }

    private SimpleByteTSDMetricsIntegration createMetricClient() {
        MetricClient client =
                new BytedanceMetricClientBuilder()
                        .configureMetricSpecValidator(
                                c ->
                                        c.setMaxMetricPerRegistry(
                                                METRICS_REPORTER_MAX_METRIC_PER_REGISTRY_DEFAULT))
                        .build();

        MetricRegistryConfig registryConfig =
                BytedanceRegistryConfig.newBuilder().setPrefix(prefix).build();
        return (SimpleByteTSDMetricsIntegration)
                client.getRegistry("default", registryConfig)
                        .getExtension(SimpleByteTSDMetrics.class);
    }

    @VisibleForTesting
    public void loadFixedTags(String tagString) {
        if (!StringUtils.isNullOrWhitespaceOnly(tagString)) {
            Arrays.stream(tagString.split(","))
                    .forEach(
                            s -> {
                                String[] tagKV = s.split("=");
                                if (tagKV.length != 2) {
                                    log.error(
                                            "Tag {} format is not 'Key=Value', this tag will be ignore.",
                                            s);
                                } else {
                                    fixedTags.put(tagKV[0], tagKV[1]);
                                }
                            });
        }
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    public void loadAllMetrics() {
        Yaml yaml = new Yaml();
        Map<String, Object> metrics;
        try {
            metrics = yaml.load(getClass().getClassLoader().getResourceAsStream(whitelistFile));
        } catch (Exception e) {
            if (!DEFAULT_METRICS_WHITELIST_FILE.equals(whitelistFile)) {
                log.error(
                        "load metrics whitelist from {} failed, fallback to {}.",
                        whitelistFile,
                        DEFAULT_METRICS_WHITELIST_FILE,
                        e);
                metrics =
                        yaml.load(
                                getClass()
                                        .getClassLoader()
                                        .getResourceAsStream(DEFAULT_METRICS_WHITELIST_FILE));
            } else {
                throw e;
            }
        }

        // load global metrics
        Map<String, Object> global = (Map<String, Object>) metrics.get("global");
        List<String> globalMetrics = (List<String>) global.get("name");
        globalNeededMetrics.addAll(globalMetrics);

        // load non-global metrics
        Map<String, Object> nonGlobal = (Map<String, Object>) metrics.get("non-global");
        List<String> nonGlobalMetrics = (List<String>) nonGlobal.get("name");
        nonGlobalNeededMetrics.addAll(nonGlobalMetrics);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            if (!nonGlobalNeededMetrics.contains(metricName)
                    && !globalNeededMetrics.contains(metricName)) {
                return;
            }
            MetricEmitter metricEmitterBuilder = generateMetricEmitter(metric, metricName, group);
            metricCollectorMap.put(metric, metricEmitterBuilder);
        }
    }

    @VisibleForTesting
    public MetricEmitter generateMetricEmitter(
            Metric metric, String metricName, MetricGroup group) {
        // 1. Generate tag list.
        Map<String, String> tagsMap = new HashMap<>();
        final Map<String, String> allVariables = group.getAllVariables();
        boolean isTaskOperatorLevelMetric = isTaskOperatorLevelMetric(allVariables);
        for (Map.Entry<String, String> entry : allVariables.entrySet()) {
            // remove id tags.
            if (!noNeededScope.contains(entry.getKey())) {
                // origin tag key is like <host>. So we need remove <>.
                tagsMap.put(
                        entry.getKey().replaceAll("<|>", ""),
                        CHARACTER_FILTER.filterCharacters(entry.getValue()));
            }
        }
        tagsMap.putAll(fixedTags);
        tagsMap.put("jobname", this.jobName);
        Tags tags = Tags.keyValues(tagsMap);
        // 2. Generate metric full name.
        boolean emitNonGlobal = nonGlobalNeededMetrics.contains(metricName);
        boolean emitGlobal = globalNeededMetrics.contains(metricName);
        String metricFullName = emitNonGlobal ? getScopeName(metricName, group) : null;
        // add jobName in task/operator level metric name
        if (isTaskOperatorLevelMetric) {
            metricFullName = Utils.prefix(jobName, metricFullName);
        }
        String globalMetricName = emitGlobal ? Utils.prefix(GLOBAL_PREFIX, metricName) : null;
        MetricEmitterBuilder metricEmitterBuilder =
                new MetricEmitterBuilder()
                        .withMetric(metric)
                        .withTags(tags)
                        .withGlobalMetricName(globalMetricName)
                        .withNonGlobalMetricName(metricFullName)
                        .withClient(client);

        log.debug(
                "Register Metric={}, metric full name:{}, global metric name:{}, tags:{}",
                metricName,
                metricFullName,
                globalMetricName,
                tagsMap);
        // 3. Generate metricEmitter.
        return metricEmitterBuilder.build();
    }

    private boolean isTaskOperatorLevelMetric(Map<String, String> tagsMap) {
        return tagsMap.containsKey((SCOPE_TASK_NAME)) || tagsMap.containsKey(SCOPE_OPERATOR_NAME);
    }

    private String getScopeName(String metricName, MetricGroup group) {
        return getLogicalScope(group)
                + SCOPE_SEPARATOR
                + CHARACTER_FILTER.filterCharacters(metricName);
    }

    private String getLogicalScope(MetricGroup group) {
        return LogicalScopeProvider.castFrom(group)
                .getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
    }

    private static final CharacterFilter CHARACTER_FILTER =
            new CharacterFilter() {
                @Override
                public String filterCharacters(String input) {
                    return Utils.formatMetricsName(input);
                }
            };

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        metricCollectorMap.remove(metric);
    }

    @Override
    public void close() {
        report();
        log.info("OpentsdbReporter closed.");
    }

    @Override
    public void report() {
        try {
            for (MetricEmitter metricEmitter : metricCollectorMap.values()) {
                metricEmitter.report();
            }
        } catch (Exception e) {
            log.error("Failed to send Metrics", e);
        }
    }

    public String getJobName() {
        return jobName;
    }

    public String getPrefix() {
        return prefix;
    }

    public Set<String> getGlobalNeededMetrics() {
        return globalNeededMetrics;
    }

    public Set<String> getNonGlobalNeededMetrics() {
        return nonGlobalNeededMetrics;
    }

    public Map<String, String> getFixedTags() {
        return fixedTags;
    }

    @VisibleForTesting
    public void setWhitelistFile(String whitelistFile) {
        this.whitelistFile = whitelistFile;
    }

    @VisibleForTesting
    public long getMetricCount() {
        return metricCollectorMap.size();
    }
}
