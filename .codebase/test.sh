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

if [ $# != 1 ] ; then
  echo "USAGE: $0 stage"
  echo " e.g.: $0 core"
  exit 1;
fi

. /etc/profile

source ".codebase/stage.sh"

export JAVA_HOME=/usr/local/jdk

# enable coredumps for this process
ulimit -c unlimited

# configure JVMs to produce heap dumps
export JAVA_TOOL_OPTIONS="-XX:+HeapDumpOnOutOfMemoryError"

# some tests provide additional logs if they find this variable
export IS_CI=true

export LOG4J_PROPERTIES=/home/code/.codebase/log4j.properties

MVN_COMPILE_MODULES=$(get_compile_modules_for_stage $1)
MVN_TEST_MODULES=$(get_test_modules_for_stage $1)
MVN_TEST_OPTIONS="-Dflink.tests.with-openssl -Dflink.tests.check-segment-multiple-free -Darchunit.freeze.store.default.allowStoreUpdate=false -Dakka.rpc.force-invocation-serialization -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"

mvn -Dfast -Pskip-webui-build -DskipTests -Psql-jars $MVN_COMPILE_MODULES install  | grep -v "Progress" | grep -v "Downloading" | grep -v "Downloaded"

mvn -Dfast -Pskip-webui-build $MVN_TEST_OPTIONS $MVN_TEST_MODULES verify | tee /home/code/verify-$1.log
