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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.handler.cluster.DashboardConfigHandler;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JobManagerExternalUrlInfo;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;

/**
 * Response of the {@link DashboardConfigHandler} containing general configuration values such as
 * the time zone and the refresh interval.
 */
public class DashboardConfiguration implements ResponseBody {

    public static final String FIELD_NAME_REFRESH_INTERVAL = "refresh-interval";
    public static final String FIELD_NAME_TIMEZONE_OFFSET = "timezone-offset";
    public static final String FIELD_NAME_TIMEZONE_NAME = "timezone-name";
    public static final String FIELD_NAME_FLINK_VERSION = "flink-version";
    public static final String FIELD_NAME_FLINK_REVISION = "flink-revision";
    public static final String FIELD_NAME_FLINK_FEATURES = "features";
    public static final String FIELD_NAME_JM_LOG_URL = "jm-log-url";
    public static final String FIELD_NAME_JM_WEBSHELL_URL = "jm-webshell-url";
    public static final String FIELD_NAME_JM_FLAMEGRAPH_URL = "jm-flamegraph-url";

    public static final String FIELD_NAME_FEATURE_WEB_SUBMIT = "web-submit";

    public static final String FIELD_NAME_FEATURE_WEB_CANCEL = "web-cancel";

    public static final String FIELD_NAME_FEATURE_WEB_HISTORY = "web-history";

    public static final String FIELD_NAME_FEATURE_LOG_URL = "log-url";

    public static final String FIELD_NAME_FEATURE_WEBSHELL_URL = "webshell-url";

    public static final String FIELD_NAME_FEATURE_FLAMEGRAPH_URL = "flamegraph-url";

    @JsonProperty(FIELD_NAME_REFRESH_INTERVAL)
    private final long refreshInterval;

    @JsonProperty(FIELD_NAME_TIMEZONE_NAME)
    private final String timeZoneName;

    @JsonProperty(FIELD_NAME_TIMEZONE_OFFSET)
    private final int timeZoneOffset;

    @JsonProperty(FIELD_NAME_FLINK_VERSION)
    private final String flinkVersion;

    @JsonProperty(FIELD_NAME_FLINK_REVISION)
    private final String flinkRevision;

    @JsonProperty(FIELD_NAME_JM_LOG_URL)
    private final String jmLogUrl;

    @JsonProperty(FIELD_NAME_JM_WEBSHELL_URL)
    private final String jmWebshellUrl;

    @JsonProperty(FIELD_NAME_JM_FLAMEGRAPH_URL)
    private final String jmFlameGraphUrl;

    @JsonProperty(FIELD_NAME_FLINK_FEATURES)
    private final Features features;

    public DashboardConfiguration(
            long refreshInterval,
            String timeZoneName,
            int timeZoneOffset,
            String flinkVersion,
            String flinkRevision,
            Features features) {
        this(
                refreshInterval,
                timeZoneName,
                timeZoneOffset,
                flinkVersion,
                flinkRevision,
                "",
                "",
                "",
                features);
    }

    @JsonCreator
    public DashboardConfiguration(
            @JsonProperty(FIELD_NAME_REFRESH_INTERVAL) long refreshInterval,
            @JsonProperty(FIELD_NAME_TIMEZONE_NAME) String timeZoneName,
            @JsonProperty(FIELD_NAME_TIMEZONE_OFFSET) int timeZoneOffset,
            @JsonProperty(FIELD_NAME_FLINK_VERSION) String flinkVersion,
            @JsonProperty(FIELD_NAME_FLINK_REVISION) String flinkRevision,
            @JsonProperty(FIELD_NAME_JM_LOG_URL) String jmLogUrl,
            @JsonProperty(FIELD_NAME_JM_WEBSHELL_URL) String jmWebshellUrl,
            @JsonProperty(FIELD_NAME_JM_FLAMEGRAPH_URL) String jmFlameGraphUrl,
            @JsonProperty(FIELD_NAME_FLINK_FEATURES) Features features) {
        this.refreshInterval = refreshInterval;
        this.timeZoneName = Preconditions.checkNotNull(timeZoneName);
        this.timeZoneOffset = timeZoneOffset;
        this.flinkVersion = Preconditions.checkNotNull(flinkVersion);
        this.flinkRevision = Preconditions.checkNotNull(flinkRevision);
        this.jmLogUrl = Preconditions.checkNotNull(jmLogUrl);
        this.jmWebshellUrl = Preconditions.checkNotNull(jmWebshellUrl);
        this.jmFlameGraphUrl = Preconditions.checkNotNull(jmFlameGraphUrl);
        this.features = features;
    }

    @JsonIgnore
    public long getRefreshInterval() {
        return refreshInterval;
    }

    @JsonIgnore
    public int getTimeZoneOffset() {
        return timeZoneOffset;
    }

    @JsonIgnore
    public String getTimeZoneName() {
        return timeZoneName;
    }

    @JsonIgnore
    public String getFlinkVersion() {
        return flinkVersion;
    }

    @JsonIgnore
    public String getFlinkRevision() {
        return flinkRevision;
    }

    @JsonIgnore
    public String getJmLogUrl() {
        return jmLogUrl;
    }

    @JsonIgnore
    public String getJmWebshellUrl() {
        return jmWebshellUrl;
    }

    @JsonIgnore
    public String getJmFlameGraphUrl() {
        return jmFlameGraphUrl;
    }

    @JsonIgnore
    public Features getFeatures() {
        return features;
    }

    /** Collection of features that are enabled/disabled. */
    public static final class Features {

        @JsonProperty(FIELD_NAME_FEATURE_WEB_SUBMIT)
        private final boolean webSubmitEnabled;

        @JsonProperty(FIELD_NAME_FEATURE_WEB_CANCEL)
        private final boolean webCancelEnabled;

        @JsonProperty(FIELD_NAME_FEATURE_WEB_HISTORY)
        private final boolean isHistoryServer;

        @JsonProperty(FIELD_NAME_FEATURE_LOG_URL)
        private final boolean logUrlEnabled;

        @JsonProperty(FIELD_NAME_FEATURE_WEBSHELL_URL)
        private final boolean webshellUrlEnabled;

        @JsonProperty(FIELD_NAME_FEATURE_FLAMEGRAPH_URL)
        private final boolean flameGraphUrlEnabled;

        public Features(
                boolean webSubmitEnabled, boolean webCancelEnabled, boolean isHistoryServer) {
            this(webSubmitEnabled, webCancelEnabled, isHistoryServer, false, false, false);
        }

        @JsonCreator
        public Features(
                @JsonProperty(FIELD_NAME_FEATURE_WEB_SUBMIT) boolean webSubmitEnabled,
                @JsonProperty(FIELD_NAME_FEATURE_WEB_CANCEL) boolean webCancelEnabled,
                @JsonProperty(FIELD_NAME_FEATURE_WEB_HISTORY) boolean isHistoryServer,
                @JsonProperty(FIELD_NAME_FEATURE_LOG_URL) boolean logUrlEnabled,
                @JsonProperty(FIELD_NAME_FEATURE_WEBSHELL_URL) boolean webshellUrlEnabled,
                @JsonProperty(FIELD_NAME_FEATURE_FLAMEGRAPH_URL) boolean flameGraphUrlEnabled) {
            this.webSubmitEnabled = webSubmitEnabled;
            this.webCancelEnabled = webCancelEnabled;
            this.isHistoryServer = isHistoryServer;
            this.logUrlEnabled = logUrlEnabled;
            this.webshellUrlEnabled = webshellUrlEnabled;
            this.flameGraphUrlEnabled = flameGraphUrlEnabled;
        }

        @JsonIgnore
        public boolean isWebSubmitEnabled() {
            return webSubmitEnabled;
        }

        @JsonIgnore
        public boolean isWebCancelEnabled() {
            return webCancelEnabled;
        }

        @JsonIgnore
        public boolean isHistoryServer() {
            return isHistoryServer;
        }

        @JsonIgnore
        public boolean isLogUrlEnabled() {
            return logUrlEnabled;
        }

        @JsonIgnore
        public boolean isWebshellUrlEnabled() {
            return webshellUrlEnabled;
        }

        @JsonIgnore
        public boolean isFlameGraphUrlEnabled() {
            return flameGraphUrlEnabled;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Features features = (Features) o;
            return webSubmitEnabled == features.webSubmitEnabled
                    && webCancelEnabled == features.webCancelEnabled
                    && isHistoryServer == features.isHistoryServer
                    && logUrlEnabled == features.logUrlEnabled
                    && webshellUrlEnabled == features.webshellUrlEnabled
                    && flameGraphUrlEnabled == features.flameGraphUrlEnabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    webSubmitEnabled,
                    webCancelEnabled,
                    isHistoryServer,
                    logUrlEnabled,
                    webshellUrlEnabled,
                    flameGraphUrlEnabled);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DashboardConfiguration that = (DashboardConfiguration) o;
        return refreshInterval == that.refreshInterval
                && timeZoneOffset == that.timeZoneOffset
                && Objects.equals(timeZoneName, that.timeZoneName)
                && Objects.equals(flinkVersion, that.flinkVersion)
                && Objects.equals(flinkRevision, that.flinkRevision)
                && Objects.equals(jmLogUrl, that.jmLogUrl)
                && Objects.equals(jmWebshellUrl, that.jmWebshellUrl)
                && Objects.equals(jmFlameGraphUrl, that.jmFlameGraphUrl)
                && Objects.equals(features, that.features);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                refreshInterval,
                timeZoneName,
                timeZoneOffset,
                flinkVersion,
                flinkRevision,
                jmLogUrl,
                jmWebshellUrl,
                jmFlameGraphUrl,
                features);
    }

    public static DashboardConfiguration from(
            long refreshInterval,
            ZonedDateTime zonedDateTime,
            boolean webSubmitEnabled,
            boolean webCancelEnabled,
            boolean isHistoryServer) {
        return from(
                refreshInterval,
                zonedDateTime,
                webSubmitEnabled,
                webCancelEnabled,
                isHistoryServer,
                new Configuration(),
                JobManagerExternalUrlInfo.empty());
    }

    public static DashboardConfiguration from(
            long refreshInterval,
            ZonedDateTime zonedDateTime,
            boolean webSubmitEnabled,
            boolean webCancelEnabled,
            boolean isHistoryServer,
            Configuration clusterConfiguration,
            JobManagerExternalUrlInfo jobManagerExternalUrlInfo) {

        final String flinkVersion = EnvironmentInformation.getVersion();

        final EnvironmentInformation.RevisionInformation revision =
                EnvironmentInformation.getRevisionInformation();
        final String flinkRevision;

        if (revision != null) {
            flinkRevision = revision.commitId + " @ " + revision.commitDate;
        } else {
            flinkRevision = "unknown revision";
        }

        final boolean logUrlEnabled = clusterConfiguration.getBoolean(WebOptions.LOG_URL_ENABLED);
        final boolean webshellUrlEnabled =
                clusterConfiguration.getBoolean(WebOptions.WEBSHELL_URL_ENABLED);
        final boolean flameGraphUrlEnabled =
                clusterConfiguration.getBoolean(WebOptions.FLAME_GRAPH_URL_ENABLED);

        return new DashboardConfiguration(
                refreshInterval,
                zonedDateTime.getZone().getDisplayName(TextStyle.FULL, Locale.getDefault()),
                // convert zone date time into offset in order to not do the day light saving
                // adaptions wrt the offset
                zonedDateTime.toOffsetDateTime().getOffset().getTotalSeconds() * 1000,
                flinkVersion,
                flinkRevision,
                jobManagerExternalUrlInfo.getLogUrl(),
                jobManagerExternalUrlInfo.getWebshellUrl(),
                jobManagerExternalUrlInfo.getFlameGraphUrl(),
                new Features(
                        webSubmitEnabled,
                        webCancelEnabled,
                        isHistoryServer,
                        logUrlEnabled,
                        webshellUrlEnabled,
                        flameGraphUrlEnabled));
    }

    public static DashboardConfiguration fromDashboardConfiguration(
            DashboardConfiguration dashboardConfiguration,
            JobManagerExternalUrlInfo jobManagerExternalUrlInfo) {
        return new DashboardConfiguration(
                dashboardConfiguration.getRefreshInterval(),
                dashboardConfiguration.getTimeZoneName(),
                dashboardConfiguration.getTimeZoneOffset(),
                dashboardConfiguration.getFlinkVersion(),
                dashboardConfiguration.getFlinkRevision(),
                jobManagerExternalUrlInfo.getLogUrl(),
                jobManagerExternalUrlInfo.getWebshellUrl(),
                jobManagerExternalUrlInfo.getFlameGraphUrl(),
                dashboardConfiguration.getFeatures());
    }
}
