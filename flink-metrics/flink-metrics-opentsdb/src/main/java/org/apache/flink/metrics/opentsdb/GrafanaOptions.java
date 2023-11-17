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

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Grafana dashboard options. */
public class GrafanaOptions {

    public static final ConfigOption<String> GRAFANA_REGISTRATION_DOMAIN_URL =
            key("grafana.registration.domain_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Api url to register grafana.");

    public static final ConfigOption<String> GRAFANA_DOMAIN_URL =
            key("grafana.domain_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("domain url of grafana dashboard.");

    public static final ConfigOption<String> REGISTER_DASHBOARD_TOKEN =
            key("register-dashboard.token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Token to register grafana dashboard.");

    public static final ConfigOption<String> DASHBOARD_DATA_SOURCE =
            key("grafana.data_source")
                    .stringType()
                    .defaultValue("bytetsd")
                    .withDescription("Data source of grafana dashboard.");

    public static final ConfigOption<String> FLINK_EXIT_CODE_DOC_URL =
            key("grafana.flink_exit_code_doc_url")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Url of flink exit code doc.");

    public static final ConfigOption<String> K8S_EXIT_CODE_DOC_URL =
            key("grafana.k8s_exit_code_doc_url")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Url of k8s exit code doc.");
}
