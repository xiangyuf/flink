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

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.util.function.FunctionUtils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** This decorator set up the pod to download remote file from HTTP/HDFS/S3 etc. */
public abstract class AbstractFileDownloadDecorator extends AbstractKubernetesStepDecorator {

    protected final String fileMountedPath;
    protected final AbstractKubernetesParameters kubernetesParameters;
    protected final Set<URI> remoteFiles = new LinkedHashSet<>();
    protected final Map<String, String> pathToFileName;

    public AbstractFileDownloadDecorator(AbstractKubernetesParameters kubernetesParameters) {
        this.kubernetesParameters = checkNotNull(kubernetesParameters);
        this.fileMountedPath =
                kubernetesParameters
                        .getFlinkConfiguration()
                        .getString(PipelineOptions.FILE_MOUNTED_PATH);
        List<URI> uris =
                getRemoteFilesForApplicationMode(kubernetesParameters.getFlinkConfiguration());
        this.remoteFiles.addAll(uris);
        pathToFileName = renameFileIfRequired(remoteFiles);
        checkState(
                new HashSet<>(pathToFileName.values()).size() == pathToFileName.size(),
                "exists multiple files with same name");
        this.kubernetesParameters
                .getFlinkConfiguration()
                .set(ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING, pathToFileName);
    }

    private static List<URI> getRemoteFilesForApplicationMode(Configuration configuration) {
        if (!configuration.contains(PipelineOptions.EXTERNAL_RESOURCES)) {
            return Collections.emptyList();
        }
        return configuration.get(PipelineOptions.EXTERNAL_RESOURCES).stream()
                .map(FunctionUtils.uncheckedFunction(PackagedProgramUtils::resolveURI))
                .filter(
                        uri ->
                                !uri.getScheme().equals(ConfigConstants.LOCAL_SCHEME)
                                        && !uri.getScheme().equals(ConfigConstants.FILE_SCHEME))
                .collect(Collectors.toList());
    }

    /** The save path of remote file downloaded should be obtained from this method. */
    public static String getDownloadedPath(URI uri, Configuration configuration) {
        String fileMountedPath = configuration.getString(PipelineOptions.FILE_MOUNTED_PATH);
        Map<String, String> pathToFileName =
                configuration.get(ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING);
        if (pathToFileName != null && pathToFileName.containsKey(uri.toString())) {
            return new File(fileMountedPath, pathToFileName.get(uri.toString())).getPath();
        }
        return new File(fileMountedPath, new File(uri.getPath()).getName()).getPath();
    }

    private static Map<String, String> renameFileIfRequired(Set<URI> remoteFiles) {
        Map<String, String> pathToFileName = new HashMap<>();
        Set<String> fileNameSet = new HashSet<>();
        List<String> sameNameFiles = new ArrayList<>();
        for (URI uri : remoteFiles) {
            String fileName = new File(uri.getPath()).getName();
            if (fileNameSet.contains(fileName)) {
                sameNameFiles.add(uri.toString());
            } else {
                fileNameSet.add(fileName);
                pathToFileName.put(uri.toString(), fileName);
            }
        }
        for (String files : sameNameFiles) {
            int prefix = 0;
            String fileName = new File(files).getName();
            String newFileName = fileName;
            while (fileNameSet.contains(newFileName)) {
                // if there exists multiple files with the same name, we need to rename files.
                // try choosing a number and set it to prefix of the original file name
                newFileName = String.format("%d_%s", prefix, fileName);
                prefix++;
            }
            fileNameSet.add(newFileName);
            pathToFileName.put(files, newFileName);
        }
        return pathToFileName;
    }

    public static AbstractFileDownloadDecorator create(
            AbstractKubernetesParameters kubernetesParameters) {
        KubernetesConfigOptions.DownloadMode downloadMode =
                kubernetesParameters
                        .getFlinkConfiguration()
                        .get(KubernetesConfigOptions.FILE_DOWNLOAD_MODE);
        switch (downloadMode) {
            case CSI:
                return new CSIFileDownloadDecorator(kubernetesParameters);
            case INIT_CONTAINER:
            default:
                return new InitContainerFileDownloadDecorator(kubernetesParameters);
        }
    }
}
