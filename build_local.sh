#!/bin/bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
set -eo pipefail

. /etc/profile

export JAVA_HOME=/usr/local/jdk

mvn clean install -U -DskipTests -Psql-jars -Pdocs-and-source | grep -v "Progress" | grep -v "Downloading" | grep -v "Downloaded"

# copy flink-1.17 to output
mkdir -p output/deploy/flink-1.17
cp -r flink-dist/target/flink-1.17-byted-SNAPSHOT-bin/flink-1.17-byted-SNAPSHOT/* output/deploy/flink-1.17/
# common jar conflict
bash tools/common-jar-check/common_jar_check.sh "output/deploy/flink-1.17/"

# flink test
mvn test -Dflink.forkCount=8 -Dflink.forkCountTestPackage=2 -pl flink-runtime,flink-core,flink-metrics/flink-metrics-core,flink-metrics/flink-metrics-dropwizard,flink-metrics/flink-metrics-prometheus,flink-metrics/flink-metrics-slf4j,flink-metrics/flink-metrics-statsd,flink-metrics/flink-metrics-influxdb,flink-metrics/flink-metrics-jmx,flink-metrics/flink-metrics-graphite,flink-metrics/flink-metrics-datadog,flink-table/flink-sql-parser,flink-table/flink-sql-parser-hive,flink-table/flink-table-api-java,flink-table/flink-table-api-java-bridge,flink-table/flink-table-common,flink-table/flink-table-code-splitter,flink-table/flink-table-planner,flink-table/flink-table-planner-loader,flink-table/flink-table-runtime,flink-table/flink-table-test-utils,flink-connectors/flink-connector-hive,flink-connectors/flink-connector-kafka,flink-formats/flink-json,flink-formats/flink-protobuf,flink-formats/flink-orc,flink-formats/flink-parquet,flink-clients,flink-yarn,flink-kubernetes,flink-state-backends/flink-statebackend-rocksdb,flink-libraries/flink-cep,flink-libraries/flink-state-processing-api
