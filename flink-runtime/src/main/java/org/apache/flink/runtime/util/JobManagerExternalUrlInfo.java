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

/** Encapsulates information on the JobManager external URLs to expose. */
public class JobManagerExternalUrlInfo {

    private static final JobManagerExternalUrlInfo EMPTY =
            new JobManagerExternalUrlInfo("", "", "");

    private final String logUrl;
    private final String webshellUrl;
    private final String flameGraphUrl;

    public JobManagerExternalUrlInfo(String logUrl, String webshellUrl, String flameGraphUrl) {
        this.logUrl = logUrl;
        this.webshellUrl = webshellUrl;
        this.flameGraphUrl = flameGraphUrl;
    }

    public static JobManagerExternalUrlInfo empty() {
        return EMPTY;
    }

    public String getLogUrl() {
        return logUrl;
    }

    public String getWebshellUrl() {
        return webshellUrl;
    }

    public String getFlameGraphUrl() {
        return flameGraphUrl;
    }
}
