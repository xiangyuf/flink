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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ProfilingInfoList;
import org.apache.flink.runtime.util.profiler.ProfilingService;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Rest handler which serves the profiling list from the JobManager. */
public class JobManagerProfilingListHandler
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, ProfilingInfoList, EmptyMessageParameters> {

    private final ProfilingService profilingService;

    public JobManagerProfilingListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, ProfilingInfoList, EmptyMessageParameters>
                    messageHeaders,
            final Configuration configuration) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.profilingService = ProfilingService.getInstance(configuration);
    }

    @Override
    protected CompletableFuture<ProfilingInfoList> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return profilingService.getProfilingList("JobManager").thenApply(ProfilingInfoList::new);
    }
}
