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

rm -rf output

mvn clean package -U -Dfast -DskipTests -Dflink.hadoop.version=3.2.1 -Psql-jars | grep -v "Progress"

# copy flink-1.17 to output
mkdir -p output
cp -r flink-dist/target/flink-1.17-byted-SNAPSHOT-bin/flink-1.17-byted-SNAPSHOT/* output/

# Copy all connectors to lib directory for streaming warehouse, then we can startup
# sql-gateway and flink session cluster with all supported connectors such as kafka and paimon.
cp -r output/connectors/*.jar output/lib/

# common jar conflict
bash tools/common-jar-check/common_jar_check.sh "output/"
