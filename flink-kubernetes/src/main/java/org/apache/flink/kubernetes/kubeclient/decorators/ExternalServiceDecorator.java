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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1beta1.HTTPIngressRuleValueBuilder;
import io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBackendBuilder;
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRuleBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Creates an external Service to expose the rest port of the Flink JobManager(s). */
public class ExternalServiceDecorator extends AbstractKubernetesStepDecorator {

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters;

    public ExternalServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        final Service service =
                kubernetesJobManagerParameters
                        .getRestServiceExposedType()
                        .serviceType()
                        .buildUpExternalRestService(kubernetesJobManagerParameters);

        if (!kubernetesJobManagerParameters.enableIngress()) {
            return Collections.singletonList(service);
        } else {
            final Ingress ingress = buildIngress(kubernetesJobManagerParameters);
            return Arrays.asList(service, ingress);
        }
    }

    /** Build the ingress through the {@link KubernetesJobManagerParameters} for the cluster. */
    public static Ingress buildIngress(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        final String serviceName =
                getExternalServiceName(kubernetesJobManagerParameters.getClusterId());

        final String ingressName = getIngressName(kubernetesJobManagerParameters.getClusterId());
        final String host =
                getIngressHost(
                        kubernetesJobManagerParameters.getClusterId(),
                        kubernetesJobManagerParameters.getIngressHost());
        final Map<String, String> annotations =
                kubernetesJobManagerParameters.getIngressAnnotations();
        final IngressRule ingressRule =
                new IngressRuleBuilder()
                        .withHost(host)
                        .withHttp(
                                new HTTPIngressRuleValueBuilder()
                                        .addNewPath()
                                        .withPath("/")
                                        .withBackend(
                                                new IngressBackendBuilder()
                                                        .withServiceName(serviceName)
                                                        .withServicePort(
                                                                new IntOrString(
                                                                        kubernetesJobManagerParameters
                                                                                .getRestPort()))
                                                        .build())
                                        .endPath()
                                        .build())
                        .build();
        final Ingress ingress =
                new IngressBuilder()
                        .withApiVersion(Constants.INGRESS_API_VERSION)
                        .withNewMetadata()
                        .withName(ingressName)
                        .withNamespace(kubernetesJobManagerParameters.getNamespace())
                        .withAnnotations(annotations)
                        .endMetadata()
                        .withNewSpec()
                        .withRules(ingressRule)
                        .endSpec()
                        .build();
        return ingress;
    }

    /** Generate name of the external rest Service. */
    public static String getExternalServiceName(String clusterId) {
        return clusterId + Constants.FLINK_REST_SERVICE_SUFFIX;
    }

    /** Generate name of the created ingress. */
    public static String getIngressName(String clusterId) {
        return clusterId + Constants.FLINK_INGRESS_SUFFIX;
    }

    /** Generate hostname of the created ingress. */
    public static String getIngressHost(String clusterId, String host) {
        return clusterId + "." + host;
    }

    /**
     * Generate namespaced name of the external rest Service by cluster Id, This is used by other
     * project, so do not delete it.
     */
    public static String getNamespacedExternalServiceName(String clusterId, String namespace) {
        return getExternalServiceName(clusterId) + "." + namespace;
    }
}
