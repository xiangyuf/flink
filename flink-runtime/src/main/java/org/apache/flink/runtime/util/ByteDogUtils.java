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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;

import org.apache.commons.lang3.StringUtils;

/**
 * Utility class to assemble URLs that redirect to the online profiling toolbox provided by ByteDog.
 */
public class ByteDogUtils {

    public static String getByteDogCpuFlameGraphBigDataUrl(
            Configuration flinkConfig, String ipOrHost, String appId) {
        String baseUrl = getByteDogCpuFlameGraphBaseUrl(flinkConfig);
        boolean isInvalid =
                StringUtils.isEmpty(baseUrl)
                        || StringUtils.isEmpty(ipOrHost)
                        || StringUtils.isEmpty(appId);
        return isInvalid
                ? ""
                : String.format(
                        "%s?type=java&ip=%s&duration=30&targetType=JAVA_FLAMEGRAPH_ON_YARN&app_id=%s",
                        baseUrl, ipOrHost, appId);
    }

    private static String getByteDogCpuFlameGraphBaseUrl(Configuration flinkConfig) {
        String domainName = flinkConfig.getString(WebOptions.BYTEDOG_BASE_URL);
        return StringUtils.isEmpty(domainName)
                ? ""
                : String.format("%s/cpuflame/single", domainName);
    }
}
