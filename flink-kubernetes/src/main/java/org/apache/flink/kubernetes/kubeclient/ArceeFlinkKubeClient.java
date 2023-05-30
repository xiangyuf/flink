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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;

import com.bytedance.openplatform.arcee.ArceeClient;
import com.bytedance.openplatform.arcee.ArceeClientImpl;
import com.bytedance.openplatform.arcee.resources.v1alpha1.ArceeApplication;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.bytedance.openplatform.arcee.Constants.ARCEE_ANNOTATION_APPLICATION_NAME_KEY;
import static com.bytedance.openplatform.arcee.Constants.ARCEE_ANNOTATION_GANG_SCHEDULING_TASK_NUM_KEY;

/** The Arcee implementation of {@link FlinkKubeClient}. */
public class ArceeFlinkKubeClient extends Fabric8FlinkKubeClient {

    private static final Logger LOG = LoggerFactory.getLogger(ArceeFlinkKubeClient.class);

    private final ArceeClient arceeClient;
    // save the master application atomic reference for setting owner reference of task manager pods
    private final AtomicReference<ArceeApplication> masterApplicationRef;

    public ArceeFlinkKubeClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ExecutorService executorService) {
        super(flinkConfig, client, executorService);

        arceeClient = new ArceeClientImpl(client);
        masterApplicationRef = new AtomicReference<>();
    }

    @Override
    public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
        final ArceeApplication application = kubernetesJMSpec.getApplication();
        addMinMemberAnnotations(
                application.getSpec().getAmSpec().getPodSpec().getMetadata().getAnnotations());

        try {
            LOG.debug("Start to create arcee application with spec {}", application);
            final ArceeApplication masterApplication =
                    this.arceeClient.createArceeApplication(super.namespace, application);
            masterApplicationRef.set(masterApplication);
        } catch (Exception e) {
            LOG.error(
                    "catch exception while creating application {}",
                    application.getMetadata().getName(),
                    e);
            throw new RuntimeException(
                    "catch exception while creating application "
                            + application.getMetadata().getName());
        }

        createAccompanyingResources(kubernetesJMSpec.getAccompanyingResources());
    }

    @Override
    public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
        return CompletableFuture.runAsync(
                () -> {
                    Map<String, String> labels =
                            kubernetesPod.getInternalResource().getMetadata().getLabels();
                    if (labels == null) {
                        labels = new HashMap<>();
                    }
                    labels.put(ARCEE_ANNOTATION_APPLICATION_NAME_KEY, super.clusterId);

                    Map<String, String> annotations =
                            kubernetesPod.getInternalResource().getMetadata().getAnnotations();
                    if (annotations == null) {
                        annotations = new HashMap<>();
                    }
                    annotations.put(ARCEE_ANNOTATION_APPLICATION_NAME_KEY, super.clusterId);
                    annotations.put(
                            Constants.POD_GROUP_NAME_ANNOTATION_KEY,
                            Constants.POD_GROUP_NAME_PREFIX + super.clusterId);
                    addMinMemberAnnotations(annotations);

                    if (this.masterApplicationRef.get() == null) {
                        ArceeApplication masterApplication = null;
                        try {
                            masterApplication =
                                    this.arceeClient.getArceeApplication(
                                            super.namespace, super.clusterId);
                        } catch (Exception e) {
                            LOG.error(
                                    "failed to get application "
                                            + super.clusterId
                                            + " in namespace "
                                            + super.namespace,
                                    e);
                        }
                        if (masterApplication == null) {
                            throw new RuntimeException(
                                    "Failed to find Arcee application named "
                                            + clusterId
                                            + " in namespace "
                                            + this.namespace);
                        }
                        masterApplicationRef.compareAndSet(null, masterApplication);
                    }
                    setOwnerReference(
                            this.masterApplicationRef.get(),
                            Collections.singletonList(kubernetesPod.getInternalResource()));

                    LOG.debug(
                            "Start to create pod with metadata {}, spec {}",
                            kubernetesPod.getInternalResource().getMetadata(),
                            kubernetesPod.getInternalResource().getSpec());

                    this.internalClient.pods().create(kubernetesPod.getInternalResource());
                },
                kubeClientExecutorService);
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {
        try {
            this.arceeClient.killArceeApplication(super.namespace, super.clusterId);
        } catch (Exception e) {
            LOG.error("failed to stop and cleanup cluster " + clusterId, e);
        }
    }

    @Override
    public void reportApplicationStatus(
            String clusterId, ApplicationStatus finalStatus, @Nullable String diagnostics) {
        try {
            if (finalStatus.equals(ApplicationStatus.SUCCEEDED)) {
                this.arceeClient.reportApplicationFinishedStatus(
                        super.namespace,
                        super.clusterId,
                        finalStatus.processExitCode(),
                        finalStatus.name(),
                        diagnostics);
            } else if (finalStatus.equals(ApplicationStatus.CANCELED)) {
                this.arceeClient.reportApplicationKilledStatus(
                        super.namespace,
                        super.clusterId,
                        finalStatus.processExitCode(),
                        finalStatus.name(),
                        diagnostics);
            } else {
                this.arceeClient.reportApplicationFailedStatus(
                        super.namespace,
                        super.clusterId,
                        finalStatus.processExitCode(),
                        finalStatus.name(),
                        diagnostics);
                LOG.warn(
                        "Report failed status of cluster {} to arcee, real status is {}, exit code is {}",
                        clusterId,
                        finalStatus.name(),
                        finalStatus.processExitCode());
            }
        } catch (Exception e) {
            LOG.error("failed to report status of cluster " + clusterId, e);
        }
    }

    private void createAccompanyingResources(List<HasMetadata> resources) {
        if (resources == null || this.masterApplicationRef.get() == null) {
            return;
        }

        resources.forEach(
                resource -> {
                    if (resource.getMetadata().getLabels() == null) {
                        resource.getMetadata().setLabels(new HashMap<>());
                    }
                    resource.getMetadata()
                            .getLabels()
                            .put(ARCEE_ANNOTATION_APPLICATION_NAME_KEY, super.clusterId);
                });

        // Note that we should use the uid of the created Application for the OwnerReference.
        setOwnerReference(this.masterApplicationRef.get(), resources);

        this.internalClient.resourceList(resources).createOrReplace();
    }

    private void setOwnerReference(ArceeApplication application, List<HasMetadata> resources) {
        final OwnerReference applicationOwnerReference =
                new OwnerReferenceBuilder()
                        .withName(application.getMetadata().getName())
                        .withApiVersion(application.getApiVersion())
                        .withUid(application.getMetadata().getUid())
                        .withKind(application.getKind())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build();
        resources.forEach(
                resource ->
                        resource.getMetadata()
                                .setOwnerReferences(
                                        Collections.singletonList(applicationOwnerReference)));
    }

    private void addMinMemberAnnotations(Map<String, String> annotations) {
        if (annotations != null
                && annotations.containsKey(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY)) {
            annotations.put(
                    ARCEE_ANNOTATION_GANG_SCHEDULING_TASK_NUM_KEY,
                    annotations.get(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY));
        }
    }
}
