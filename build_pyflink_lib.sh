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
root_path=$(dirname $(readlink -f $0))
rm -rf ${root_path}/dist
python ${root_path}/flink-python/pyflink/gen_version.py
# compile java
mvn clean package -U -Dfast -DskipTests -Dflink.hadoop.version=3.2.1 -Psql-jars | grep -v "Progress"
# archive pyflink-lib
rm -rf ${root_path}/flink-python/apache-flink-libraries/dist
cd flink-python/apache-flink-libraries
python setup.py sdist
cd ${root_path}
cp -r ${root_path}/flink-python/apache-flink-libraries/dist ${root_path}
