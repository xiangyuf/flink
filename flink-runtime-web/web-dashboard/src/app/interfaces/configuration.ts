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

export interface JvmInfo {
  version: string;
  arch: string;
  options: string[];
}

export interface EnvironmentInfo {
  jvm: JvmInfo;
  classpath: string[];
}

export interface ClusterConfiguration {
  key: string;
  value: string;
}

export interface Configuration {
  'refresh-interval': number;
  'timezone-name': string;
  'timezone-offset': number;
  'flink-version': string;
  'flink-revision': string;
  'jm-log-url': string;
  'jm-webshell-url': string;
  'jm-flamegraph-url': string;
  features: {
    'web-history': boolean;
    'web-submit': boolean;
    'web-cancel': boolean;
    'log-url': boolean;
    'webshell-url': boolean;
    'flamegraph-url': boolean;
  };
}
