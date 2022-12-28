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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesCheckpointStoreUtil;
import org.apache.flink.kubernetes.highavailability.KubernetesJobGraphStoreUtil;
import org.apache.flink.kubernetes.highavailability.KubernetesStateHandleStore;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractFileDownloadDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStoreUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.NoOpJobGraphStoreWatcher;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.utils.Constants.CHECKPOINT_ID_KEY_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.COMPLETED_CHECKPOINT_FILE_SUFFIX;
import static org.apache.flink.kubernetes.utils.Constants.DNS_POLICY_DEFAULT;
import static org.apache.flink.kubernetes.utils.Constants.DNS_POLICY_HOSTNETWORK;
import static org.apache.flink.kubernetes.utils.Constants.JOB_GRAPH_STORE_KEY_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.apache.flink.kubernetes.utils.Constants.SUBMITTED_JOBGRAPH_FILE_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Common utils for Kubernetes. */
public class KubernetesUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

    private static final YAMLMapper yamlMapper = new YAMLMapper();

    private static final String LEADER_PREFIX = "org.apache.flink.k8s.leader.";
    private static final char LEADER_INFORMATION_SEPARATOR = ',';

    /**
     * Check whether the port config option is a fixed port. If not, the fallback port will be set
     * to configuration.
     *
     * @param flinkConfig flink configuration
     * @param port config option need to be checked
     * @param fallbackPort the fallback port that will be set to the configuration
     */
    public static void checkAndUpdatePortConfigOption(
            Configuration flinkConfig, ConfigOption<String> port, int fallbackPort) {
        if (KubernetesUtils.parsePort(flinkConfig, port) == 0) {
            flinkConfig.setString(port, String.valueOf(fallbackPort));
            LOG.info(
                    "Kubernetes deployment requires a fixed port. Configuration {} will be set to {}",
                    port.key(),
                    fallbackPort);
        }
    }

    /**
     * Parse a valid port for the config option. A fixed port is expected, and do not support a
     * range of ports.
     *
     * @param flinkConfig flink config
     * @param port port config option
     * @return valid port
     */
    public static Integer parsePort(Configuration flinkConfig, ConfigOption<String> port) {
        checkNotNull(flinkConfig.get(port), port.key() + " should not be null.");

        try {
            return Integer.parseInt(flinkConfig.get(port));
        } catch (NumberFormatException ex) {
            throw new FlinkRuntimeException(
                    port.key()
                            + " should be specified to a fixed port. Do not support a range of ports.",
                    ex);
        }
    }

    /** Generate name of the Deployment. */
    public static String getDeploymentName(String clusterId) {
        return clusterId;
    }

    /**
     * Get task manager selectors for the current Flink cluster. They could be used to watch the
     * pods status.
     *
     * @return Task manager labels.
     */
    public static Map<String, String> getTaskManagerSelectors(String clusterId) {
        final Map<String, String> labels = getCommonLabels(clusterId);
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
        return Collections.unmodifiableMap(labels);
    }

    /**
     * Get job manager selectors for the current Flink cluster.
     *
     * @return JobManager selectors.
     */
    public static Map<String, String> getJobManagerSelectors(String clusterId) {
        final Map<String, String> labels = getCommonLabels(clusterId);
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        return Collections.unmodifiableMap(labels);
    }

    /**
     * Get the common labels for Flink native clusters. All the Kubernetes resources will be set
     * with these labels.
     *
     * @param clusterId cluster id
     * @return Return common labels map
     */
    public static Map<String, String> getCommonLabels(String clusterId) {
        final Map<String, String> commonLabels = new HashMap<>();
        commonLabels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
        commonLabels.put(Constants.LABEL_APP_KEY, clusterId);

        return commonLabels;
    }

    /**
     * Get ConfigMap labels for the current Flink cluster. They could be used to filter and clean-up
     * the resources.
     *
     * @param clusterId cluster id
     * @param type the config map use case. It could only be {@link
     *     Constants#LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY} now.
     * @return Return ConfigMap labels.
     */
    public static Map<String, String> getConfigMapLabels(String clusterId, String type) {
        final Map<String, String> labels = new HashMap<>(getCommonLabels(clusterId));
        labels.put(Constants.LABEL_CONFIGMAP_TYPE_KEY, type);
        return Collections.unmodifiableMap(labels);
    }

    /**
     * Check the ConfigMap list should only contain the expected one.
     *
     * @param configMaps ConfigMap list to check
     * @param expectedConfigMapName expected ConfigMap Name
     * @return Return the expected ConfigMap
     */
    public static KubernetesConfigMap checkConfigMaps(
            List<KubernetesConfigMap> configMaps, String expectedConfigMapName) {
        assert (configMaps.size() == 1);
        assert (configMaps.get(0).getName().equals(expectedConfigMapName));
        return configMaps.get(0);
    }

    /**
     * Get the {@link LeaderInformation} from ConfigMap.
     *
     * @param configMap ConfigMap contains the leader information
     * @return Parsed leader information. It could be {@link LeaderInformation#empty()} if there is
     *     no corresponding data in the ConfigMap.
     */
    public static LeaderInformation getLeaderInformationFromConfigMap(
            KubernetesConfigMap configMap) {
        final String leaderAddress = configMap.getData().get(LEADER_ADDRESS_KEY);
        final String sessionIDStr = configMap.getData().get(LEADER_SESSION_ID_KEY);
        final UUID sessionID = sessionIDStr == null ? null : UUID.fromString(sessionIDStr);
        if (leaderAddress == null && sessionIDStr == null) {
            return LeaderInformation.empty();
        }
        return LeaderInformation.known(sessionID, leaderAddress);
    }

    /**
     * Create a {@link DefaultJobGraphStore} with {@link NoOpJobGraphStoreWatcher}.
     *
     * @param configuration configuration to build a RetrievableStateStorageHelper
     * @param flinkKubeClient flink kubernetes client
     * @param configMapName ConfigMap name
     * @param lockIdentity lock identity to check the leadership
     * @return a {@link DefaultJobGraphStore} with {@link NoOpJobGraphStoreWatcher}
     * @throws Exception when create the storage helper
     */
    public static JobGraphStore createJobGraphStore(
            Configuration configuration,
            FlinkKubeClient flinkKubeClient,
            String configMapName,
            String lockIdentity)
            throws Exception {

        final KubernetesStateHandleStore<JobGraph> stateHandleStore =
                createJobGraphStateHandleStore(
                        configuration, flinkKubeClient, configMapName, lockIdentity);
        return new DefaultJobGraphStore<>(
                stateHandleStore,
                NoOpJobGraphStoreWatcher.INSTANCE,
                KubernetesJobGraphStoreUtil.INSTANCE);
    }

    /**
     * Create a {@link KubernetesStateHandleStore} which storing {@link JobGraph}.
     *
     * @param configuration configuration to build a RetrievableStateStorageHelper
     * @param flinkKubeClient flink kubernetes client
     * @param configMapName ConfigMap name
     * @param lockIdentity lock identity to check the leadership
     * @return a {@link KubernetesStateHandleStore} which storing {@link JobGraph}.
     * @throws Exception when create the storage helper
     */
    public static KubernetesStateHandleStore<JobGraph> createJobGraphStateHandleStore(
            Configuration configuration,
            FlinkKubeClient flinkKubeClient,
            String configMapName,
            String lockIdentity)
            throws Exception {

        final RetrievableStateStorageHelper<JobGraph> stateStorage =
                new FileSystemStateStorageHelper<>(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        SUBMITTED_JOBGRAPH_FILE_PREFIX);

        return new KubernetesStateHandleStore<>(
                flinkKubeClient,
                configMapName,
                stateStorage,
                k -> k.startsWith(JOB_GRAPH_STORE_KEY_PREFIX),
                lockIdentity);
    }

    /**
     * Create a {@link DefaultCompletedCheckpointStore} with {@link KubernetesStateHandleStore}.
     *
     * @param configuration configuration to build a RetrievableStateStorageHelper
     * @param kubeClient flink kubernetes client
     * @param configMapName ConfigMap name
     * @param executor executor to run blocking calls
     * @param lockIdentity lock identity to check the leadership
     * @param maxNumberOfCheckpointsToRetain max number of checkpoints to retain on state store
     *     handle
     * @param restoreMode the mode in which the job is restoring
     * @return a {@link DefaultCompletedCheckpointStore} with {@link KubernetesStateHandleStore}.
     * @throws Exception when create the storage helper failed
     */
    public static CompletedCheckpointStore createCompletedCheckpointStore(
            Configuration configuration,
            FlinkKubeClient kubeClient,
            Executor executor,
            String configMapName,
            @Nullable String lockIdentity,
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            RestoreMode restoreMode)
            throws Exception {

        final RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage =
                new FileSystemStateStorageHelper<>(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        COMPLETED_CHECKPOINT_FILE_SUFFIX);
        final KubernetesStateHandleStore<CompletedCheckpoint> stateHandleStore =
                new KubernetesStateHandleStore<>(
                        kubeClient,
                        configMapName,
                        stateStorage,
                        k -> k.startsWith(CHECKPOINT_ID_KEY_PREFIX),
                        lockIdentity);
        Collection<CompletedCheckpoint> checkpoints =
                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                        stateHandleStore, KubernetesCheckpointStoreUtil.INSTANCE);

        return new DefaultCompletedCheckpointStore<>(
                maxNumberOfCheckpointsToRetain,
                stateHandleStore,
                KubernetesCheckpointStoreUtil.INSTANCE,
                checkpoints,
                sharedStateRegistryFactory.create(ioExecutor, checkpoints, restoreMode),
                executor);
    }

    /**
     * Get resource requirements from memory and cpu.
     *
     * @param resourceRequirements resource requirements in pod template
     * @param mem Memory in mb.
     * @param memoryLimitFactor limit factor for the memory, used to set the limit resources.
     * @param cpu cpu.
     * @param cpuLimitFactor limit factor for the cpu, used to set the limit resources.
     * @param externalResources external resources
     * @param externalResourceConfigKeys config keys of external resources
     * @return KubernetesResource requirements.
     */
    public static ResourceRequirements getResourceRequirements(
            ResourceRequirements resourceRequirements,
            int mem,
            double memoryLimitFactor,
            double cpu,
            double cpuLimitFactor,
            Map<String, ExternalResource> externalResources,
            Map<String, String> externalResourceConfigKeys) {
        final Quantity cpuQuantity = new Quantity(String.valueOf(cpu));
        final Quantity cpuLimitQuantity = new Quantity(String.valueOf(cpu * cpuLimitFactor));
        final Quantity memQuantity = new Quantity(mem + Constants.RESOURCE_UNIT_MB);
        final Quantity memQuantityLimit =
                new Quantity(((int) (mem * memoryLimitFactor)) + Constants.RESOURCE_UNIT_MB);

        ResourceRequirementsBuilder resourceRequirementsBuilder =
                new ResourceRequirementsBuilder(resourceRequirements)
                        .addToRequests(Constants.RESOURCE_NAME_MEMORY, memQuantity)
                        .addToRequests(Constants.RESOURCE_NAME_CPU, cpuQuantity)
                        .addToLimits(Constants.RESOURCE_NAME_MEMORY, memQuantityLimit)
                        .addToLimits(Constants.RESOURCE_NAME_CPU, cpuLimitQuantity);

        // Add the external resources to resource requirement.
        for (Map.Entry<String, ExternalResource> externalResource : externalResources.entrySet()) {
            final String configKey = externalResourceConfigKeys.get(externalResource.getKey());
            if (!StringUtils.isNullOrWhitespaceOnly(configKey)) {
                final Quantity resourceQuantity =
                        new Quantity(
                                String.valueOf(externalResource.getValue().getValue().longValue()));
                resourceRequirementsBuilder
                        .addToRequests(configKey, resourceQuantity)
                        .addToLimits(configKey, resourceQuantity);
                LOG.info(
                        "Request external resource {} with config key {}.",
                        resourceQuantity.getAmount(),
                        configKey);
            }
        }

        return resourceRequirementsBuilder.build();
    }

    public static List<String> getStartCommandWithBashWrapper(String command) {
        return Arrays.asList("bash", "-c", command);
    }

    public static List<File> checkJarFileForApplicationMode(Configuration configuration) {
        return configuration.get(PipelineOptions.JARS).stream()
                .map(
                        FunctionUtils.uncheckedFunction(
                                uri -> {
                                    final URI jarURI = PackagedProgramUtils.resolveURI(uri);
                                    if (jarURI.getScheme().equals("local") && jarURI.isAbsolute()) {
                                        return new File(jarURI.getPath());
                                    } else if (!jarURI.getScheme()
                                            .equals(ConfigConstants.FILE_SCHEME)) {
                                        // for remote file, return downloaded path
                                        String jarFile =
                                                AbstractFileDownloadDecorator.getDownloadedPath(
                                                        jarURI, configuration);
                                        return new File(jarFile);
                                    }
                                    // local disk file (scheme: file) should be uploaded to external
                                    // storage and save its remote URI in this key
                                    throw new IllegalArgumentException(
                                            "Only \"local\" or remote storage (hdfs/s3 etc) is supported as schema for application mode."
                                                    + " This assumes that the jar is located in the image, not the Flink client. Or the jar could be downloaded."
                                                    + " If you provide a disk file, have you specified a correct upload path on external storage so Flink could upload your disk file to that place?"
                                                    + " An example of such local path is: local:///opt/flink/examples/streaming/WindowJoin.jar"
                                                    + " An example of remote path is: s3://test-client-log/WindowJoin.jar");
                                }))
                .collect(Collectors.toList());
    }

    public static List<URL> getExternalFiles(Configuration configuration) {
        if (configuration.contains(PipelineOptions.EXTERNAL_RESOURCES)) {
            // filter the files in pipeline.jars because user jar will be added to classpath
            // separately.
            List<String> userJars =
                    ConfigUtils.decodeListFromConfig(
                            configuration, PipelineOptions.JARS, jar -> jar);
            return configuration.get(PipelineOptions.EXTERNAL_RESOURCES).stream()
                    .filter(
                            resource ->
                                    !userJars.contains(resource)
                                            && FileUtils.isJarFile(Paths.get(resource)))
                    .map(
                            FunctionUtils.uncheckedFunction(
                                    uri -> {
                                        final URI jarURI = PackagedProgramUtils.resolveURI(uri);
                                        if (jarURI.getScheme().equals(ConfigConstants.LOCAL_SCHEME)
                                                && jarURI.isAbsolute()) {
                                            return new File(jarURI.getPath()).toURI().toURL();
                                        } else if (!jarURI.getScheme()
                                                .equals(ConfigConstants.FILE_SCHEME)) {
                                            // for remote file, return downloaded path
                                            String jarFile =
                                                    AbstractFileDownloadDecorator.getDownloadedPath(
                                                            jarURI, configuration);
                                            return new File(jarFile).toURI().toURL();
                                        }
                                        // local disk file (scheme: file) should be uploaded to
                                        // external storage and save its remote URI in this key
                                        throw new IllegalArgumentException(
                                                "Only \"local\" or remote storage (hdfs/s3 etc) is supported as schema for application mode."
                                                        + " This assumes that the jar is located in the image, not the Flink client. Or the jar could be downloaded."
                                                        + " If you provide a disk file, have you specified a correct upload path on external storage so Flink could upload your disk file to that place?"
                                                        + " An example of such local path is: local:///opt/flink/examples/streaming/WindowJoin.jar"
                                                        + " An example of remote path is: s3://test-client-log/WindowJoin.jar");
                                    }))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public static FlinkPod loadPodFromTemplateFile(
            FlinkKubeClient kubeClient, File podTemplateFile, String mainContainerName) {
        final KubernetesPod pod = kubeClient.loadPodFromTemplateFile(podTemplateFile);
        final List<Container> otherContainers = new ArrayList<>();
        Container mainContainer = null;

        if (null != pod.getInternalResource().getSpec()) {
            for (Container container : pod.getInternalResource().getSpec().getContainers()) {
                if (mainContainerName.equals(container.getName())) {
                    mainContainer = container;
                } else {
                    otherContainers.add(container);
                }
            }
            pod.getInternalResource().getSpec().setContainers(otherContainers);
        } else {
            // Set an empty spec for pod template
            pod.getInternalResource().setSpec(new PodSpecBuilder().build());
        }

        if (mainContainer == null) {
            LOG.info(
                    "Could not find main container {} in pod template, using empty one to initialize.",
                    mainContainerName);
            mainContainer = new ContainerBuilder().build();
        }

        return new FlinkPod(pod.getInternalResource(), mainContainer);
    }

    public static File getTaskManagerPodTemplateFileInPod() {
        return new File(
                Constants.POD_TEMPLATE_DIR_IN_POD, Constants.TASK_MANAGER_POD_TEMPLATE_FILE_NAME);
    }

    /**
     * Resolve the user defined value with the precedence. First an explicit config option value is
     * taken, then the value in pod template and at last the default value of a config option if
     * nothing is specified.
     *
     * @param flinkConfig flink configuration
     * @param configOption the config option to define the Kubernetes fields
     * @param valueOfConfigOptionOrDefault the value defined by explicit config option or default
     * @param valueOfPodTemplate the value defined in the pod template
     * @param fieldDescription Kubernetes fields description
     * @param <T> The type of value associated with the configuration option.
     * @return the resolved value
     */
    public static <T> String resolveUserDefinedValue(
            Configuration flinkConfig,
            ConfigOption<T> configOption,
            String valueOfConfigOptionOrDefault,
            @Nullable String valueOfPodTemplate,
            String fieldDescription) {
        final String resolvedValue;
        if (valueOfPodTemplate != null) {
            // The config option is explicitly set.
            if (flinkConfig.contains(configOption)) {
                resolvedValue = valueOfConfigOptionOrDefault;
                LOG.info(
                        "The {} configured in pod template will be overwritten to '{}' "
                                + "because of explicitly configured options.",
                        fieldDescription,
                        resolvedValue);
            } else {
                resolvedValue = valueOfPodTemplate;
            }
        } else {
            resolvedValue = valueOfConfigOptionOrDefault;
        }
        return resolvedValue;
    }

    /**
     * Resolve the DNS policy defined value. Return DNS_POLICY_HOSTNETWORK if host network enabled.
     * If not, check whether there is a DNS policy overridden in pod template.
     *
     * @param dnsPolicy DNS policy defined in pod template spec
     * @param hostNetworkEnabled Host network enabled or not
     * @return the resolved value
     */
    public static String resolveDNSPolicy(String dnsPolicy, boolean hostNetworkEnabled) {
        if (hostNetworkEnabled) {
            return DNS_POLICY_HOSTNETWORK;
        }
        if (!StringUtils.isNullOrWhitespaceOnly(dnsPolicy)) {
            return dnsPolicy;
        }
        return DNS_POLICY_DEFAULT;
    }

    /**
     * Get the service account from the input pod first, if not specified, the service account name
     * will be used.
     *
     * @param flinkPod the Flink pod to parse the service account
     * @return the parsed service account
     */
    @Nullable
    public static String getServiceAccount(FlinkPod flinkPod) {
        final String serviceAccount =
                flinkPod.getPodWithoutMainContainer().getSpec().getServiceAccount();
        if (serviceAccount == null) {
            return flinkPod.getPodWithoutMainContainer().getSpec().getServiceAccountName();
        }
        return serviceAccount;
    }

    /**
     * Try to get the pretty print yaml for Kubernetes resource.
     *
     * @param kubernetesResource kubernetes resource
     * @return the pretty print yaml, or the {@link KubernetesResource#toString()} if parse failed.
     */
    public static String tryToGetPrettyPrintYaml(KubernetesResource kubernetesResource) {
        try {
            return yamlMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(kubernetesResource);
        } catch (Exception ex) {
            LOG.debug(
                    "Failed to get the pretty print yaml, fallback to {}", kubernetesResource, ex);
            return kubernetesResource.toString();
        }
    }

    /** Checks if hostNetwork is enabled. */
    public static boolean isHostNetwork(Configuration configuration) {
        return configuration.getBoolean(KubernetesConfigOptions.KUBERNETES_HOSTNETWORK_ENABLED);
    }

    /**
     * Creates a config map with the given name if it does not exist.
     *
     * @param flinkKubeClient to use for creating the config map
     * @param configMapName name of the config map
     * @param clusterId clusterId to which the map belongs
     * @throws FlinkException if the config map could not be created
     */
    public static void createConfigMapIfItDoesNotExist(
            FlinkKubeClient flinkKubeClient, String configMapName, String clusterId)
            throws FlinkException {

        int attempt = 0;
        CompletionException lastException = null;

        final int maxAttempts = 10;
        final KubernetesConfigMap configMap =
                new KubernetesConfigMap(
                        new ConfigMapBuilder()
                                .withNewMetadata()
                                .withName(configMapName)
                                .withLabels(
                                        getConfigMapLabels(
                                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                                .endMetadata()
                                .build());

        while (!flinkKubeClient.getConfigMap(configMapName).isPresent() && attempt < maxAttempts) {
            try {
                flinkKubeClient.createConfigMap(configMap).join();
            } catch (CompletionException e) {
                // retrying
                lastException = ExceptionUtils.firstOrSuppressed(e, lastException);
            }

            attempt++;
        }

        if (attempt >= maxAttempts && lastException != null) {
            throw new FlinkException(
                    String.format("Could not create the config map %s.", configMapName),
                    lastException);
        }
    }

    /**
     * Get staging directory used to store JARS and RESOURCES. This directory will be deleted when
     * cluster is killed. It will be a child directory of user defined upload directory.
     */
    public static String getStagingDirectory(String uploadDir, String clusterId) {
        return new Path(new Path(uploadDir), clusterId).toString();
    }

    /**
     * Upload local disk files in JARS and RESOURCES to remote storage system. <\p> This method will
     * rewrite the JARS and RESOURCES URIs into the configuration. For the files that need to upload
     * ("file" scheme), it will save the remote URI. For the files that has been already in remote
     * storage or in the image, it will save the original URI.
     *
     * @param flinkConfig
     */
    public static void uploadLocalDiskFilesToRemote(
            Configuration flinkConfig, String targetDirPath) {
        // we don't support uploading a folder because some downloader (e.g. CSI driver) don't
        // support it.
        removeFolderInExternalFiles(flinkConfig);
        // count number of files that need to upload
        long numOfDiskFiles =
                Stream.concat(
                                flinkConfig.getOptional(PipelineOptions.JARS)
                                        .orElse(Collections.emptyList()).stream(),
                                flinkConfig.getOptional(PipelineOptions.EXTERNAL_RESOURCES)
                                        .orElse(Collections.emptyList()).stream())
                        .filter(
                                uri -> {
                                    final URI jarURI;
                                    try {
                                        jarURI = PackagedProgramUtils.resolveURI(uri);
                                        return jarURI.getScheme()
                                                .equals(ConfigConstants.FILE_SCHEME);
                                    } catch (URISyntaxException e) {
                                        LOG.warn("Can not resolve URI for path: {}", uri, e);
                                        return false;
                                    }
                                })
                        .count();
        AtomicReference<Path> targetDirOptional = new AtomicReference<>(null);
        if (numOfDiskFiles > 0) {
            // If there are any file to be uploaded, we should ensure the target dir is existing
            targetDirOptional.set(new Path(targetDirPath));
            Path targetDir = targetDirOptional.get();
            try {
                FileSystem fileSystem = targetDir.getFileSystem();
                if (!fileSystem.exists(targetDir)) {
                    fileSystem.mkdirs(targetDir);
                }
            } catch (IOException e) {
                LOG.error("create target dir {} failed:", targetDir, e);
                return;
            }
        }
        // upload user jar
        List<String> toBeDownloadedFiles = new ArrayList<>();
        List<String> jars =
                flinkConfig.get(PipelineOptions.JARS).stream()
                        .map(
                                FunctionUtils.uncheckedFunction(
                                        uri -> {
                                            final URI jarURI = PackagedProgramUtils.resolveURI(uri);
                                            if (jarURI.getScheme()
                                                    .equals(ConfigConstants.FILE_SCHEME)) {
                                                // upload to target dir
                                                String uploadedPath =
                                                        copyFileToTargetRemoteDir(
                                                                jarURI, targetDirOptional.get());
                                                toBeDownloadedFiles.add(uploadedPath);
                                                return uploadedPath;
                                            } else if (jarURI.getScheme()
                                                    .equals(ConfigConstants.LOCAL_SCHEME)) {
                                                // return the path directly if it is a local file
                                                // path (located inside the image)
                                                return jarURI.toString();
                                            } else {
                                                // if it is a remote file path, add it into
                                                // download-file list and return this path directly
                                                toBeDownloadedFiles.add(jarURI.toString());
                                                return jarURI.toString();
                                            }
                                        }))
                        .collect(Collectors.toList());
        // replace path of user jar by the uploaded path
        flinkConfig.set(PipelineOptions.JARS, jars);
        // upload resources
        List<String> resources =
                flinkConfig.getOptional(PipelineOptions.EXTERNAL_RESOURCES)
                        .orElse(Collections.emptyList()).stream()
                        .map(
                                FunctionUtils.uncheckedFunction(
                                        uri -> {
                                            final URI jarURI = PackagedProgramUtils.resolveURI(uri);
                                            if (jarURI.getScheme()
                                                    .equals(ConfigConstants.FILE_SCHEME)) {
                                                return copyFileToTargetRemoteDir(
                                                        jarURI, targetDirOptional.get());
                                            } else {
                                                // return directly if it is a local (located inside
                                                // the image) or remote file path
                                                return jarURI.toString();
                                            }
                                        }))
                        .collect(Collectors.toList());
        // add remote files in "JARS" to "EXTERNAL_RESOURCES", FileDownloadDecorator will setup to
        // download these files.
        resources.addAll(toBeDownloadedFiles);
        // replace path of resources by the uploaded path
        flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES, resources);
    }

    private static void removeFolderInExternalFiles(Configuration flinkConfig) {
        List<String> filesWithoutFolder =
                flinkConfig.getOptional(PipelineOptions.EXTERNAL_RESOURCES)
                        .orElse(Collections.emptyList()).stream()
                        .filter(
                                path -> {
                                    try {
                                        URI uri = PackagedProgramUtils.resolveURI(path);
                                        if (uri.getScheme().equals(ConfigConstants.FILE_SCHEME)
                                                && new File(uri.getPath()).isDirectory()) {
                                            LOG.warn(
                                                    "Remove folder in external resources: {}", uri);
                                            return false;
                                        }
                                        return true;
                                    } catch (URISyntaxException e) {
                                        LOG.error(
                                                "can not resolve uri from this path:{}, ignore it",
                                                path,
                                                e);
                                        return false;
                                    }
                                })
                        .collect(Collectors.toList());
        flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES, filesWithoutFolder);
    }

    /**
     * Copy one file to target directory.
     *
     * @param uri The uri of this file
     * @param targetDir destination directory of this file
     * @return The copied path of the file if copy succeed otherwise return the original path
     */
    private static String copyFileToTargetRemoteDir(URI uri, Path targetDir) {
        Path path = new Path(uri);
        try {
            Path targetPath = new Path(targetDir, path.getName());
            LOG.info("upload local file {} into remote dir {}", path, targetPath);
            FileUtils.copy(path, targetPath, false);
            return targetPath.toString();
        } catch (IOException e) {
            LOG.error("upload local file {} into remote dir {} failed:", path, targetDir, e);
        }
        // will return the original path if copy failed
        return uri.toString();
    }

    /** Cluster components. */
    public enum ClusterComponent {
        JOB_MANAGER,
        TASK_MANAGER
    }

    public static String encodeLeaderInformation(LeaderInformation leaderInformation) {
        Preconditions.checkArgument(leaderInformation.getLeaderSessionID() != null);
        Preconditions.checkArgument(leaderInformation.getLeaderAddress() != null);

        return leaderInformation.getLeaderSessionID().toString()
                + LEADER_INFORMATION_SEPARATOR
                + leaderInformation.getLeaderAddress();
    }

    public static Optional<LeaderInformation> parseLeaderInformationSafely(String value) {
        try {
            return Optional.of(parseLeaderInformation(value));
        } catch (Throwable throwable) {
            LOG.debug("Could not parse value {} into LeaderInformation.", value, throwable);
            return Optional.empty();
        }
    }

    private static LeaderInformation parseLeaderInformation(String value) {
        final int splitIndex = value.indexOf(LEADER_INFORMATION_SEPARATOR);

        Preconditions.checkState(
                splitIndex >= 0,
                String.format(
                        "Expecting '<session_id>%c<leader_address>'",
                        LEADER_INFORMATION_SEPARATOR));

        final UUID leaderSessionId = UUID.fromString(value.substring(0, splitIndex));
        final String leaderAddress = value.substring(splitIndex + 1);

        return LeaderInformation.known(leaderSessionId, leaderAddress);
    }

    public static String createSingleLeaderKey(String componentId) {
        return LEADER_PREFIX + componentId;
    }

    public static boolean isSingleLeaderKey(String key) {
        return key.startsWith(LEADER_PREFIX);
    }

    public static String extractLeaderName(String key) {
        return key.substring(LEADER_PREFIX.length());
    }

    public static List<ContainerPort> getContainerPortsWithUserPorts(
            Map<String, Integer> ports, Map<String, Integer> userDefinedPorts) {
        for (Map.Entry<String, Integer> userPort : userDefinedPorts.entrySet()) {
            if (ports.containsKey(userPort.getKey())) {
                throw new IllegalArgumentException(
                        "Port name "
                                + userPort.getKey()
                                + " already used by system, "
                                + "Please use name other than rest/socket/blobserver/jobmanager-rpc/taskmanager-rpc.");
            }
            if (ports.containsValue(userPort.getValue())) {
                throw new IllegalArgumentException(
                        "Port " + userPort.getValue() + " already used.");
            }
            ports.put(userPort.getKey(), userPort.getValue());
        }
        return ports.entrySet().stream()
                .map(
                        e ->
                                new ContainerPortBuilder()
                                        .withName(e.getKey())
                                        .withContainerPort(e.getValue())
                                        .build())
                .collect(Collectors.toList());
    }

    /** Generate namespaced name of the service. */
    public static String getNamespacedServiceName(Service service) {
        return service.getMetadata().getName() + "." + service.getMetadata().getNamespace();
    }

    /**
     * Get Annotations with ConfigOption. First get all annotations by option key directly, Then get
     * all annotations with optionKey as prefix.
     *
     * @return annotations.
     */
    public static Map<String, String> getAnnotations(
            Configuration configuration, ConfigOption<Map<String, String>> option) {
        Map<String, String> annotations =
                new HashMap<>(configuration.getOptional(option).orElse(Collections.emptyMap()));
        String annotationPrefix = option.key() + ".";
        annotations.putAll(
                ConfigurationUtils.getPrefixedKeyValuePairs(annotationPrefix, configuration));
        return annotations;
    }

    private KubernetesUtils() {}
}
