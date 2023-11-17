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

package org.apache.flink.metrics.opentsdb.utils;

import org.apache.flink.util.StringUtils;

/** Common utils for Metrics. */
public class Utils {

    public static final String METRIC_REGISTER_URL_TEMPLATE = "%s/api/dashboards/db";

    /**
     * Parse input to write metric. Replace the charset which didn't in [A-Za-z0-9-_] to '_'.
     * Because some characters metrics doesn't support. Example "flink-test$job" to
     * "flink_test_job".
     */
    public static String formatMetricsName(String input) {
        String result = input.replaceAll("[^\\w.]", "_").replaceAll("\\.+", ".");
        return result;
    }

    public static String formatOperatorName(String input) {
        return input.replaceAll("-", "_").replaceAll("_+", "_");
    }

    /*
     *  Connects args by comma
     * */
    public static String prefix(String... names) {
        if (names.length > 0) {
            StringBuilder stringBuilder = new StringBuilder(names[0]);

            for (int i = 1; i < names.length; i++) {
                stringBuilder.append('.').append(names[i]);
            }

            return stringBuilder.toString();
        } else {
            return "";
        }
    }

    public static String addSuffix(String content, String suffix) {
        if (StringUtils.isNullOrWhitespaceOnly(content)) {
            return "";
        }
        return Utils.prefix(content, suffix);
    }
}
