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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The file download decorator using csi drive to download files. This decorator will add a csi
 * volume to pod. And then the csi driver deployed in Kubernetes cluster can download the files from
 * remote storage.
 */
public class CSIFileDownloadDecorator extends AbstractFileDownloadDecorator {

    public CSIFileDownloadDecorator(AbstractKubernetesParameters kubernetesParameters) {
        super(kubernetesParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (remoteFiles.isEmpty()) {
            return flinkPod;
        }
        return downloadFilesByCsi(flinkPod);
    }

    private FlinkPod downloadFilesByCsi(FlinkPod flinkPod) {
        final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());
        // csi type volume
        final Volume csiVolume =
                new VolumeBuilder()
                        .withName(Constants.FILE_DOWNLOAD_VOLUME)
                        .withNewCsi()
                        .withDriver(kubernetesParameters.getCsiDriverName())
                        .withNewNodePublishSecretRef()
                        .withName(kubernetesParameters.getSecretName())
                        .endNodePublishSecretRef()
                        .withVolumeAttributes(getCsiVolumeAttributes(0))
                        .endCsi()
                        .build();
        final Pod basicPod =
                new PodBuilder(flinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToVolumes(csiVolume)
                        .endSpec()
                        .build();
        return new FlinkPod.Builder(flinkPod)
                .withPod(basicPod)
                .withMainContainer(basicMainContainer)
                .build();
    }

    private Container decorateMainContainer(Container mainContainer) {
        // csi driver want to be allocated a proper disk otherwise the downloading will be very
        // slow.
        Map<String, Quantity> diskRequirement =
                kubernetesParameters.getCsiDiskResourceRequirement();
        final ResourceRequirements resourceRequirements =
                new ResourceRequirementsBuilder(mainContainer.getResources())
                        .addToRequests(diskRequirement)
                        .addToLimits(diskRequirement)
                        .build();
        return new ContainerBuilder(mainContainer)
                .addNewVolumeMount()
                .withName(Constants.FILE_DOWNLOAD_VOLUME)
                .withMountPath(fileMountedPath)
                .endVolumeMount()
                .withResources(resourceRequirements)
                .build();
    }

    /**
     * Get the csi volume attribute map required by csi driver. The attribute map should contain
     * three items: 1. volumeType: Data or LOG; Here we use data for file downloading 2.
     * ssdAffinity: Prefer; Means we prefer use ssd to download file 3. resourceList: Json
     * representation of Map:String -> LocalResource where the key is th saving path of one
     * LocalResource, the value "LocalResource" has three field: remote file path, timestamp for
     * this file, and the resource type (ARCHIVE, FILE, PATTERN). The CSI driver uses (path,
     * timestamp) to identify the file version, for timestamp of a file, set it to zero if you can
     * guarantee the path will never be overridden so one path always represent one version.
     *
     * @param fileTimestamp the file timestamp for all remote files. Using same timestamp means they
     *     all belong to the same version. 0 means all file in this path belongs to the same
     *     version.
     * @return the created csi volume attribute map
     */
    public Map<String, String> getCsiVolumeAttributes(long fileTimestamp) {
        Map<String, String> volumeAttributes = new HashMap<>();
        volumeAttributes.put("volumeType", "Data");
        volumeAttributes.put("ssdAffinity", "Prefer");
        String resourceList =
                "{"
                        + remoteFiles.stream()
                                .map(LocalResource::new)
                                .map(
                                        localResource ->
                                                String.format(
                                                        "\"%s\": %s",
                                                        localResource.getFileDownloadPath(
                                                                pathToFileName),
                                                        localResource.toJsonString(fileTimestamp)))
                                .collect(Collectors.joining(", "))
                        + "}";
        volumeAttributes.put("resourceList", resourceList);
        return volumeAttributes;
    }

    /** Class to represent a local resource in CSI volume definition. */
    public static class LocalResource {

        public static final int ARCHIVE = 0;
        public static final int FILE = 1;
        public static final int PATTERN = 2;

        private final URI path;
        private final int resourceType;

        private LocalResource(URI path) {
            this.path = path;
            if (path.getPath().endsWith(Constants.JAR_FILE_EXTENSION)) {
                // if this is a jar file, we shouldn't mark it as ARCHIVE because we don't want to
                // extract it in containers.
                this.resourceType = LocalResource.FILE;
            } else {
                this.resourceType = LocalResource.ARCHIVE;
            }
        }

        public String getFileDownloadPath(Map<String, String> pathToFileName) {
            // just download in the volume root directory
            return pathToFileName.get(this.path.toString());
        }

        public String toJsonString(long timestamp) {
            return String.format(
                    "{\"path\": \"%s\", \"timestamp\": %d, \"resourceType\": %d}",
                    path, timestamp, resourceType);
        }
    }
}
