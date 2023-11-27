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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.SharedClientHAServices;
import org.apache.flink.runtime.highavailability.SharedClientHAServicesFactory;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.util.FlinkException;

import java.util.concurrent.ConcurrentHashMap;

/** Factory for creating shared client high availability services. */
public class DefaultSharedClientHAServicesFactory implements SharedClientHAServicesFactory {

    public static final DefaultSharedClientHAServicesFactory INSTANCE =
            new DefaultSharedClientHAServicesFactory();

    private static final ConcurrentHashMap<String, SharedClientHAServices>
            sharedClientHAServicesMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, LeaderRetriever> leaderRetrievers =
            new ConcurrentHashMap<>();

    @Override
    public SharedClientHAServices createSharedClientHAServices(Configuration configuration)
            throws Exception {
        String clusterId = configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
        LeaderRetriever leaderRetriever =
                leaderRetrievers.computeIfAbsent(clusterId, id -> new LeaderRetriever());

        return sharedClientHAServicesMap.computeIfAbsent(
                clusterId,
                id -> {
                    try {
                        return HighAvailabilityServicesUtils.createSharedClientHAService(
                                configuration,
                                leaderRetriever,
                                exception ->
                                        leaderRetriever.handleError(
                                                new FlinkException(
                                                        "Fatal error happened with client HA "
                                                                + "services.",
                                                        exception)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
