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

package org.apache.flink.metrics.opentsdb.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.opentsdb.OpentsDBHttpUtil;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Kafka util to parse topic information. */
public class KafkaUtil {
    private static final String METRICS_PREFIX_QUERY_URL =
            "/queryClusterMetricsPrefix.do?cluster=%s";
    private static final String KAFKA_CONSUMER_DASHBOARD_URL_FORMAT =
            "%s/dashboard/db/data-inf-kafka-user-consumer_offset?orgId=1&var-consumer_group=%s&var-topic=%s&var-topic_related_metric_prefix=%s&var-broker_related_metric_prefix=%s&var-data_source=%s&var-dc=*&refresh=1m";
    private static final String CLUSTER_METRICS_PREFIX_KEY = "clusterMetricsPrefix";
    private static final String TOPIC_RELATED_METRIC_PREFIX = "topic_related_metric_prefix";
    private static final String BROKER_RELATED_METRIC_PREFIX = "broker_related_metric_prefix";

    public static final String KAFKA_SERVER_URL_KEY = "kafka_server_url";
    // kafka domain address is not allowed in flink code.
    public static final String KAFKA_SERVER_URL_DEFAUL = "no_kafka_url";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
    private static final int MAX_RETRY_TIMES = 3;
    private static final String METRICS_SEPARATOR = ",";

    public static List<String> getKafkaLagSizeMetrics(String kafkaServerUrl) {
        List<String> kafkaMetricsList = new ArrayList<>();
        final JsonNode kafkaTopics = getKafkaTopics();
        if (kafkaTopics.isArray()) {
            for (JsonNode jsonNode : kafkaTopics) {
                String kafkaCluster = jsonNode.get("cluster").asText();
                Tuple2<String, String> metricPrefix =
                        KafkaUtil.getKafkaTopicPrefix(kafkaCluster, kafkaServerUrl);
                String kafkaTopicPrefix = metricPrefix.f0;
                String topic = jsonNode.get("topic").asText();
                String consumer = jsonNode.get("consumer").asText();
                String metric =
                        String.format("%s.%s.%s.lag.size", kafkaTopicPrefix, topic, consumer);
                kafkaMetricsList.add(metric);
            }
        }
        return kafkaMetricsList;
    }

    public static List<Tuple2<String, String>> getKafkaConsumerUrls(
            String kafkaServerUrl, String kafkaGrafanaUrl, String dataSource) {
        List<Tuple2<String, String>> kafkaConsumerUrls = new ArrayList<>();

        final JsonNode kafkaTopics = getKafkaTopics();
        if (kafkaTopics.isArray()) {
            for (JsonNode jsonNode : kafkaTopics) {
                String kafkaCluster = jsonNode.get("cluster").asText();
                Tuple2<String, String> metricPrefix =
                        KafkaUtil.getKafkaTopicPrefix(kafkaCluster, kafkaServerUrl);
                String kafkaTopicPrefix = metricPrefix.f0;
                String kafkaBrokerPrefix = metricPrefix.f1;
                String topic = jsonNode.get("topic").asText();
                String consumer = jsonNode.get("consumer").asText();
                String url =
                        KafkaUtil.getKafkaConsumerDashboardUrl(
                                kafkaGrafanaUrl,
                                consumer,
                                topic,
                                kafkaTopicPrefix,
                                kafkaBrokerPrefix,
                                dataSource);
                kafkaConsumerUrls.add(Tuple2.of(topic, url));
            }
        }
        return kafkaConsumerUrls;
    }

    public static JsonNode getKafkaTopics() {
        String kafkaMetricsStr = System.getProperty("flink_kafka_metrics", "[]");
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            final JsonNode jsonNode = objectMapper.readTree(kafkaMetricsStr);
            return jsonNode;
        } catch (Exception e) {
            LOG.error("Failed to render lag size metrics", e);
        }
        return objectMapper.createObjectNode();
    }

    /** @return Kafka topic prefix. Return null if there is something wrong. */
    public static Tuple2<String, String> getKafkaTopicPrefix(
            String cluster, String kafkaServerUrl) {
        String url = String.format(kafkaServerUrl + METRICS_PREFIX_QUERY_URL, cluster);
        LOG.info("kafka metrics query url = {}", url);
        int retryTimes = 0;
        while (retryTimes++ < MAX_RETRY_TIMES) {
            try {
                OpentsDBHttpUtil.HttpResponsePojo response = OpentsDBHttpUtil.sendGet(url);
                int statusCode = response.getStatusCode();
                boolean success = statusCode == 200;
                String resStr = response.getContent();
                if (!success) {
                    LOG.warn("Failed to get kafka metrics prefix, response: {}", resStr);
                    return null;
                }
                ObjectMapper objectMapper = new ObjectMapper();
                final JsonNode jsonNode = objectMapper.readTree(resStr);
                JsonNode kafkaMetricPrefix = jsonNode.get(CLUSTER_METRICS_PREFIX_KEY);
                if (!kafkaMetricPrefix.isMissingNode()) {

                    String topicMetricPrefix =
                            kafkaMetricPrefix.get(TOPIC_RELATED_METRIC_PREFIX).asText();
                    if (!StringUtils.isNullOrWhitespaceOnly(topicMetricPrefix)
                            && topicMetricPrefix.contains(METRICS_SEPARATOR)) {
                        topicMetricPrefix = topicMetricPrefix.split(METRICS_SEPARATOR)[0];
                    }
                    String brokerMetricPrefix =
                            kafkaMetricPrefix.get(BROKER_RELATED_METRIC_PREFIX).asText();
                    if (!StringUtils.isNullOrWhitespaceOnly(topicMetricPrefix)
                            && brokerMetricPrefix.contains(METRICS_SEPARATOR)) {
                        brokerMetricPrefix = brokerMetricPrefix.split(METRICS_SEPARATOR)[0];
                    }
                    return Tuple2.of(topicMetricPrefix, brokerMetricPrefix);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to get kafka topic prefix. kafka cluster = {}, "
                                + "kafkaServerUrl = {}",
                        cluster,
                        kafkaServerUrl,
                        e);
            }
        }
        return null;
    }

    public static String getKafkaConsumerDashboardUrl(
            String domainUrl,
            String consumerGroup,
            String topic,
            String topicPrefix,
            String brokerPrefix,
            String datasource) {
        return String.format(
                KAFKA_CONSUMER_DASHBOARD_URL_FORMAT,
                domainUrl,
                consumerGroup,
                topic,
                topicPrefix,
                brokerPrefix,
                datasource);
    }
}
