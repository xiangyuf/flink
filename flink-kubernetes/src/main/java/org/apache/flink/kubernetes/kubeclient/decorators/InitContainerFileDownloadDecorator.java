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

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The file download decorator using init container. This decorator will add an init-container to
 * pod which use flink native file system to download files from remote storage.
 */
public class InitContainerFileDownloadDecorator extends AbstractFileDownloadDecorator {

    public InitContainerFileDownloadDecorator(AbstractKubernetesParameters kubernetesParameters) {
        super(kubernetesParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (remoteFiles.isEmpty()) {
            return flinkPod;
        }
        return downloadFilesByFlink(flinkPod);
    }

    private FlinkPod downloadFilesByFlink(FlinkPod flinkPod) {
        final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());
        // emptyDir type volume
        String fileDownloadVolumeSize =
                kubernetesParameters
                        .getFlinkConfiguration()
                        .getString(KubernetesConfigOptions.FILE_DOWNLOAD_VOLUME_SIZE);

        final Volume emptyDirVolume;
        if (StringUtils.isNullOrWhitespaceOnly(fileDownloadVolumeSize)) {
            emptyDirVolume =
                    new VolumeBuilder()
                            .withName(Constants.FILE_DOWNLOAD_VOLUME)
                            .withNewEmptyDir()
                            .endEmptyDir()
                            .build();
        } else {
            emptyDirVolume =
                    new VolumeBuilder()
                            .withName(Constants.FILE_DOWNLOAD_VOLUME)
                            .withNewEmptyDir()
                            .withNewSizeLimit(fileDownloadVolumeSize)
                            .endEmptyDir()
                            .build();
        }
        // init container to download remote files
        final Container initContainer = createInitContainer(basicMainContainer);
        final Pod basicPod =
                new PodBuilder(flinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToInitContainers(initContainer)
                        .addToVolumes(emptyDirVolume)
                        .endSpec()
                        .build();
        return new FlinkPod.Builder(flinkPod)
                .withPod(basicPod)
                .withMainContainer(basicMainContainer)
                .build();
    }

    private Container createInitContainer(Container basicMainContainer) {
        String remoteFiles =
                this.remoteFiles.stream().map(URI::toString).collect(Collectors.joining(";"));
        // By default, use command `bin/flink download [source file list] [target directory]`
        String downloadTemplate =
                kubernetesParameters
                        .getFlinkConfiguration()
                        .getString(PipelineOptions.DOWNLOAD_TEMPLATE);
        String downloadCommand =
                downloadTemplate
                        .replace("%files%", remoteFiles)
                        .replace("%target%", fileMountedPath);
        return new ContainerBuilder(basicMainContainer)
                .withName("downloader")
                .withArgs(Arrays.asList("/bin/bash", "-c", downloadCommand))
                .build();
    }

    private Container decorateMainContainer(Container mainContainer) {
        return new ContainerBuilder(mainContainer)
                .addNewVolumeMount()
                .withName(Constants.FILE_DOWNLOAD_VOLUME)
                .withMountPath(fileMountedPath)
                .endVolumeMount()
                .build();
    }
}
