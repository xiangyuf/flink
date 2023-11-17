/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.opentsdb;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.opentsdb.utils.KafkaUtil;
import org.apache.flink.metrics.opentsdb.utils.Utils;
import org.apache.flink.streaming.runtime.dashboard.JobDashboardRegister;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Provides methods for registering grafana dashboard. */
public class GrafanaJobDashboardRegister implements JobDashboardRegister {
    private static final Logger LOG = LoggerFactory.getLogger(GrafanaJobDashboardRegister.class);

    private final String jobName;
    private final String cluster;
    private final String dataSource;
    private final Configuration jobConfig;
    private final String url;
    private final String token;
    private final String k8sExitDocUrl;
    private final String flinkExitDocUrl;

    public GrafanaJobDashboardRegister(String jobName, Configuration jobConfig) {
        this.jobName = jobName;
        this.cluster = jobConfig.getString(ConfigConstants.CLUSTER_NAME_KEY, "");
        this.jobConfig = jobConfig;
        this.dataSource = jobConfig.getString(GrafanaOptions.DASHBOARD_DATA_SOURCE);
        LOG.info("dataSource = {}", dataSource);
        String grafanaDomainUrl =
                jobConfig.getString(
                        GrafanaOptions.GRAFANA_REGISTRATION_DOMAIN_URL,
                        jobConfig.getString(GrafanaOptions.GRAFANA_DOMAIN_URL));
        this.url = String.format(Utils.METRIC_REGISTER_URL_TEMPLATE, grafanaDomainUrl);
        this.token = jobConfig.getString(GrafanaOptions.REGISTER_DASHBOARD_TOKEN);
        this.flinkExitDocUrl = jobConfig.getString(GrafanaOptions.FLINK_EXIT_CODE_DOC_URL);
        this.k8sExitDocUrl = jobConfig.getString(GrafanaOptions.K8S_EXIT_CODE_DOC_URL);

        checkArgument(
                this.url != null && token != null,
                "dashboard url or token not exists, please config by "
                        + GrafanaOptions.GRAFANA_REGISTRATION_DOMAIN_URL.key()
                        + " and "
                        + GrafanaOptions.REGISTER_DASHBOARD_TOKEN.key());
    }

    @Override
    public void registerDashboard() {
        LOG.info("register grafana dashboard");
        int maxRetryTimes = 5;
        int retryTimes = 0;
        boolean registerDashboardSuccessfully = false;
        while (retryTimes++ < maxRetryTimes && !registerDashboardSuccessfully) {
            try {
                registerDashboardSuccessfully = registerDashboardInternal();
            } catch (Throwable e) {
                registerDashboardSuccessfully = false;
                LOG.info("Failed to registering dashboard, retry", e);
            }
        }
        if (registerDashboardSuccessfully) {
            LOG.info("Succeed in registering dashboard.");
        } else {
            LOG.warn("Failed to registering dashboard!");
        }
    }

    private boolean registerDashboardInternal() throws IOException {
        String dashboardJson = renderDashboard();

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", token);
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");

        LOG.error("url:{}, token:{} dashboard template :\n {}", url, token, dashboardJson);

        OpentsDBHttpUtil.HttpResponsePojo response =
                OpentsDBHttpUtil.sendPost(url, dashboardJson, headers);
        int statusCode = response.getStatusCode();
        boolean success = statusCode == 200;
        if (!success) {
            String resStr = response.getContent();
            LOG.warn("Failed to register dashboard, response: {}", resStr);
        }
        return success;
    }

    private String renderDashboard() {
        List<String> rows = new ArrayList<>();
        String grafanaDomainUrl = jobConfig.getString(GrafanaOptions.GRAFANA_DOMAIN_URL);

        List<String> overViewPanels = Lists.newArrayList();
        overViewPanels.add(renderTemplate(DashboardTemplate.OVERVIEW_TEMPLATE));
        overViewPanels.add(renderTemplate(DashboardTemplate.JOB_INFO_TEMPLATE));

        String kafkaServerUrl =
                jobConfig.getString(
                        KafkaUtil.KAFKA_SERVER_URL_KEY, KafkaUtil.KAFKA_SERVER_URL_DEFAUL);
        List<String> kafkaMetricsList = KafkaUtil.getKafkaLagSizeMetrics(kafkaServerUrl);
        if (!kafkaMetricsList.isEmpty()) {
            LOG.info("Register kafka metric");
            List<Tuple2<String, String>> kafkaConsumerUrls =
                    KafkaUtil.getKafkaConsumerUrls(kafkaServerUrl, grafanaDomainUrl, dataSource);
            String kafkaLagSizeRow = renderKafkaLagSizeRow(kafkaMetricsList, kafkaConsumerUrls);
            overViewPanels.add(kafkaLagSizeRow);
        }
        overViewPanels.add(renderTemplate(DashboardTemplate.PENDING_RECORD_TEMPLATE));
        overViewPanels.add(renderTemplate(DashboardTemplate.RECORD_NUM_TEMPLATE));
        overViewPanels.add(renderTemplate(DashboardTemplate.TASK_STATUS_TEMPLATE));

        // add overview row
        rows.add(renderRowTemplate(DashboardTemplate.OVERVIEW_ROW_TEMPLATE, overViewPanels, false));

        // add jvm row
        List<String> jvmPanels = Lists.newArrayList();
        jvmPanels.add(renderTemplate(DashboardTemplate.JM_MEMORY_TEMPLATE));
        jvmPanels.add(renderTemplate(DashboardTemplate.TM_MEMORY_TEMPLATE));
        jvmPanels.add(renderTemplate(DashboardTemplate.JM_GC_TEMPLATE));
        jvmPanels.add(renderTemplate(DashboardTemplate.TM_GC_TEMPLATE));
        jvmPanels.add(renderTemplate(DashboardTemplate.JM_THREAD_TEMPLATE));
        jvmPanels.add(renderTemplate(DashboardTemplate.TM_THREAD_TEMPLATE));
        rows.add(renderRowTemplate(DashboardTemplate.JVM_ROW_TEMPLATE, jvmPanels, false));

        // add network row
        List<String> networkPanels = Lists.newArrayList();
        networkPanels.add(renderTemplate(DashboardTemplate.POOL_USAGE_TEMPLATE));
        networkPanels.add(renderTemplate(DashboardTemplate.NETWORK_MEMORY_TEMPLATE));
        rows.add(renderRowTemplate(DashboardTemplate.NETWORK_ROW_TEMPLATE, networkPanels));

        // add schedule info row
        List<String> scheduleInfoPanels = Lists.newArrayList();
        scheduleInfoPanels.add(renderTemplate(DashboardTemplate.TASK_MANAGER_SLOT_TEMPLATE));
        scheduleInfoPanels.add(renderTemplate(DashboardTemplate.COMPLETED_CONTAINER_TEMPLATE));
        rows.add(
                renderRowTemplate(
                        DashboardTemplate.SCHEDULE_INFO_ROW_TEMPLATE, scheduleInfoPanels, true));

        // add watermark row
        List<String> watermarkPanels = Lists.newArrayList();
        watermarkPanels.add(renderTemplate(DashboardTemplate.LATE_RECORDS_DROPPED_TEMPLATE));
        rows.add(renderRowTemplate(DashboardTemplate.WATERMARK_ROW_TEMPLATE, watermarkPanels));

        // add checkpoint row
        rows.add(renderTemplate(DashboardTemplate.CHECKPOINT_OVERVIEW_TEMPLATE));

        // add streaming warehouse row
        List<String> streamingWarehousePanels = Lists.newArrayList();
        streamingWarehousePanels.add(renderTemplate(DashboardTemplate.BYTES_OUT_TEMPLATE));
        streamingWarehousePanels.add(renderTemplate(DashboardTemplate.RECORD_OUT_TEMPLATE));
        streamingWarehousePanels.add(
                renderTemplate(DashboardTemplate.CURRENT_FETCH_EVENT_TIME_LAG_TEMPLATE));
        rows.add(
                renderRowTemplate(
                        DashboardTemplate.STREAMINGWAREHOUSE_ROW_TEMPLATE,
                        streamingWarehousePanels,
                        true));

        String template;

        try {
            template = renderFromResource(DashboardTemplate.DASHBOARD_TEMPLATE);
        } catch (IOException e) {
            LOG.error("Fail to render row template.", e);
            return "";
        }
        String rowsStr = String.join(",", rows);
        Map<String, String> map = new HashMap<>();
        map.put("rows", rowsStr);
        map.put("jobname", jobName);
        map.put("cluster", cluster);
        map.put("FLINK_EXIT_DOC_URL", flinkExitDocUrl);
        map.put("K8S_EXIT_DOC_URL", k8sExitDocUrl);
        String dashboardJson = renderAutoIncreasingGlobalId(renderString(template, map));
        return dashboardJson;
    }

    private String renderString(String content, Map<String, String> map) {
        Set<Map.Entry<String, String>> sets = map.entrySet();
        try {
            for (Map.Entry<String, String> entry : sets) {
                String regex = "${" + entry.getKey() + "}";
                content = content.replace(regex, entry.getValue());
            }
        } catch (Exception e) {
            LOG.error("Failed to render string", e);
        }
        return content;
    }

    private String renderRowTemplate(String rowTemplateName, List<String> panels) {

        return renderRowTemplate(rowTemplateName, panels, true);
    }

    private String renderRowTemplate(
            String rowTemplateName, List<String> panels, boolean collapse) {

        try {
            String rowTemplate = renderFromResource(rowTemplateName);
            Map<String, String> panelValues = new HashMap<>();
            panelValues.put("collapsed", String.valueOf(collapse));
            if (collapse) {
                if (CollectionUtils.isNotEmpty(panels)) {
                    panelValues.put("panels", String.join(",", panels));
                } else {
                    panelValues.put("panels", "");
                }
                return renderString(rowTemplate, panelValues);
            } else {
                panelValues.put("panels", "");
                String row = renderString(rowTemplate, panelValues);
                return row + "," + String.join(",", panels);
            }
        } catch (IOException e) {
            LOG.error("Fail to render row template.", e);
            return "";
        }
    }

    private String renderKafkaLagSizeRow(
            List<String> lags, List<Tuple2<String, String>> kafkaConsumerUrls) {
        String lagSizeTargetTemplate;
        String lagSizeTemplate;
        String linkTemplate;

        try {
            lagSizeTargetTemplate =
                    renderFromResource(DashboardTemplate.KAFKA_LAG_SIZE_TARGET_TEMPLATE);
            lagSizeTemplate = renderFromResource(DashboardTemplate.KAFKA_LAG_SIZE_TEMPLATE);
            linkTemplate = renderFromResource(DashboardTemplate.KAFKA_LAG_LINK_TEMPLATE);
        } catch (IOException e) {
            LOG.error("Fail to render row template.", e);
            return "";
        }
        List<String> lagsList = new ArrayList<>();
        for (String l : lags) {
            Map<String, String> lagSizeTargetValues = new HashMap<>();
            lagSizeTargetValues.put("lag", l);
            lagsList.add(renderString(lagSizeTargetTemplate, lagSizeTargetValues));
        }
        List<String> linksList = new ArrayList<>();
        Map<String, String> linkValues = new HashMap<>();
        for (Tuple2<String, String> tuple : kafkaConsumerUrls) {
            linkValues.put("topic", tuple.f0);
            linkValues.put("url", tuple.f1);
            linksList.add(renderString(linkTemplate, linkValues));
        }
        String targets = String.join(",", lagsList);
        String links = String.join(",", linksList);
        Map<String, String> lagSizeValues = new HashMap<>();
        lagSizeValues.put("targets", targets);
        lagSizeValues.put("links", links);
        lagSizeValues.put("datasource", dataSource);
        String lagSizeRow = renderString(lagSizeTemplate, lagSizeValues);
        return lagSizeRow;
    }

    private String renderTemplate(String template) {
        String result;

        try {
            result = renderFromResource(template);
        } catch (IOException e) {
            LOG.error("Fail to render row template.", e);
            return "";
        }
        Map<String, String> recordNumValues = new HashMap<>();
        recordNumValues.put("datasource", dataSource);
        String poolUsageRow = renderString(result, recordNumValues);
        return poolUsageRow;
    }

    private String renderFromResource(String resource) throws IOException {
        StringBuilder contentBuilder = new StringBuilder();
        try (InputStream stream =
                GrafanaJobDashboardRegister.class
                        .getClassLoader()
                        .getResourceAsStream("templates/" + resource)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            String line;
            line = br.readLine();
            while (line != null) {
                if (!line.startsWith("#")) {
                    contentBuilder.append(line);
                    contentBuilder.append("\n");
                }
                line = br.readLine();
            }
        }
        return contentBuilder.toString();
    }

    private String renderAutoIncreasingGlobalId(String template) {
        String idOrder = renderAutoIncreasingGlobalId(template, "\\$\\{id\\}");
        String rowOrder = renderAutoIncreasingGlobalId(idOrder, "\\$\\{rowOrder\\}");
        return rowOrder;
    }

    private String renderAutoIncreasingGlobalId(String template, String regex) {
        int id = 0;
        Matcher matcher = Pattern.compile(regex).matcher(template);
        matcher.reset();
        boolean result = matcher.find();
        if (result) {
            StringBuffer sb = new StringBuffer();
            do {
                matcher.appendReplacement(sb, String.valueOf(id += 100));
                result = matcher.find();
            } while (result);
            matcher.appendTail(sb);
            return sb.toString();
        }
        return template;
    }
}
