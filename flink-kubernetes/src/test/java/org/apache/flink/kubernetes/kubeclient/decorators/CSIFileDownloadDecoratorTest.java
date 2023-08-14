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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for FileDownloaderDecorator. */
public class CSIFileDownloadDecoratorTest extends KubernetesJobManagerTestBase {

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();
    }

    @Test
    public void testGetCsiVolumeAttributes() {
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Collections.singletonList("hdfs://haruna/AppMaster.jar"));
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                Collections.singletonList("hdfs://haruna/AppMaster2.jar"));
        CSIFileDownloadDecorator csiFileDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.FILE_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_RESOURCES,
                        ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING);
        CSIFileDownloadDecorator csiDepsDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.DEPENDENCIES_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_DEPENDENCIES,
                        ApplicationConfiguration.EXTERNAL_DEPENDENCIES_NAME_MAPPING);
        Map<String, String> expected = getCommonVolumeAttributesMap();
        Map<String, String> expectedDeps = getCommonVolumeAttributesMap();
        long timestamp = System.currentTimeMillis();
        expected.put(
                "resourceList",
                String.format(
                        "{\"AppMaster.jar\": {\"path\": \"hdfs://haruna/AppMaster.jar\", \"timestamp\": %d, \"resourceType\": %d}}",
                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE));
        expectedDeps.put(
                "resourceList",
                String.format(
                        "{\"AppMaster2.jar\": {\"path\": \"hdfs://haruna/AppMaster2.jar\", \"timestamp\": %d, \"resourceType\": %d}}",
                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE));
        Map<String, String> actual =
                csiFileDownloadDecorator.getCsiVolumeAttributes(
                        csiFileDownloadDecorator.remoteFiles,
                        csiFileDownloadDecorator.pathToFileName,
                        timestamp);
        Map<String, String> actualDeps =
                csiDepsDownloadDecorator.getCsiVolumeAttributes(
                        csiDepsDownloadDecorator.remoteFiles,
                        csiDepsDownloadDecorator.pathToFileName,
                        timestamp);
        assertThat(actual).isEqualTo(expected);
        assertThat(actualDeps).isEqualTo(expectedDeps);
    }

    @Test
    public void testGetCsiVolumeAttributesForTwoFiles() {
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList("hdfs://haruna/AppMaster1.jar", "hdfs://haruna/AppMaster2.jar"));
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                Arrays.asList("hdfs://haruna/AppMaster3.jar", "hdfs://haruna/AppMaster4.jar"));
        CSIFileDownloadDecorator csiFileDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.FILE_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_RESOURCES,
                        ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING);
        CSIFileDownloadDecorator csiDepsDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.DEPENDENCIES_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_DEPENDENCIES,
                        ApplicationConfiguration.EXTERNAL_DEPENDENCIES_NAME_MAPPING);
        Map<String, String> expected = getCommonVolumeAttributesMap();
        Map<String, String> expectedDeps = getCommonVolumeAttributesMap();
        long timestamp = System.currentTimeMillis();
        expected.put(
                "resourceList",
                "{"
                        + String.join(
                                ", ",
                                String.format(
                                        "\"AppMaster1.jar\": {\"path\": \"hdfs://haruna/AppMaster1.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"AppMaster2.jar\": {\"path\": \"hdfs://haruna/AppMaster2.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE))
                        + "}");
        expectedDeps.put(
                "resourceList",
                "{"
                        + String.join(
                                ", ",
                                String.format(
                                        "\"AppMaster3.jar\": {\"path\": \"hdfs://haruna/AppMaster3.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"AppMaster4.jar\": {\"path\": \"hdfs://haruna/AppMaster4.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE))
                        + "}");
        Map<String, String> actual =
                csiFileDownloadDecorator.getCsiVolumeAttributes(
                        csiFileDownloadDecorator.remoteFiles,
                        csiFileDownloadDecorator.pathToFileName,
                        timestamp);
        Map<String, String> actualDeps =
                csiDepsDownloadDecorator.getCsiVolumeAttributes(
                        csiDepsDownloadDecorator.remoteFiles,
                        csiDepsDownloadDecorator.pathToFileName,
                        timestamp);
        assertThat(actual).isEqualTo(expected);
        assertThat(actualDeps).isEqualTo(expectedDeps);
    }

    @Test
    public void testGetCsiVolumeAttributesForSameNameFiles() {
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList(
                        "hdfs://haruna/AppMaster2.jar",
                        "hdfs://haruna/0_AppMaster2.jar",
                        "hdfs://haruna/flink/AppMaster2.jar"));
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                Arrays.asList(
                        "hdfs://haruna/AppMaster3.jar",
                        "hdfs://haruna/0_AppMaster3.jar",
                        "hdfs://haruna/flink/AppMaster3.jar"));
        CSIFileDownloadDecorator csiFileDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.FILE_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_RESOURCES,
                        ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING);
        CSIFileDownloadDecorator csiDepsDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.DEPENDENCIES_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_DEPENDENCIES,
                        ApplicationConfiguration.EXTERNAL_DEPENDENCIES_NAME_MAPPING);
        Map<String, String> expected = getCommonVolumeAttributesMap();
        Map<String, String> expectedDeps = getCommonVolumeAttributesMap();
        long timestamp = System.currentTimeMillis();
        expected.put(
                "resourceList",
                "{"
                        + String.join(
                                ", ",
                                String.format(
                                        "\"AppMaster2.jar\": {\"path\": \"hdfs://haruna/AppMaster2.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"0_AppMaster2.jar\": {\"path\": \"hdfs://haruna/0_AppMaster2.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"1_AppMaster2.jar\": {\"path\": \"hdfs://haruna/flink/AppMaster2.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE))
                        + "}");
        expectedDeps.put(
                "resourceList",
                "{"
                        + String.join(
                                ", ",
                                String.format(
                                        "\"AppMaster3.jar\": {\"path\": \"hdfs://haruna/AppMaster3.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"0_AppMaster3.jar\": {\"path\": \"hdfs://haruna/0_AppMaster3.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"1_AppMaster3.jar\": {\"path\": \"hdfs://haruna/flink/AppMaster3.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE))
                        + "}");
        Map<String, String> actual =
                csiFileDownloadDecorator.getCsiVolumeAttributes(
                        csiFileDownloadDecorator.remoteFiles,
                        csiFileDownloadDecorator.pathToFileName,
                        timestamp);
        Map<String, String> actualDeps =
                csiDepsDownloadDecorator.getCsiVolumeAttributes(
                        csiDepsDownloadDecorator.remoteFiles,
                        csiDepsDownloadDecorator.pathToFileName,
                        timestamp);
        assertThat(actual).isEqualTo(expected);
        assertThat(actualDeps).isEqualTo(expectedDeps);
    }

    @Test
    public void testGetCsiVolumeAttributesForMixZipAndJar() {
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList("hdfs://haruna/AppMaster1.jar", "hdfs://haruna/AppMaster2.zip"));
        this.flinkConfig.set(
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                Arrays.asList("hdfs://haruna/AppMaster3.jar", "hdfs://haruna/AppMaster4.zip"));
        CSIFileDownloadDecorator csiFileDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.FILE_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_RESOURCES,
                        ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING);
        CSIFileDownloadDecorator csiDepsDownloadDecorator =
                new CSIFileDownloadDecorator(
                        kubernetesJobManagerParameters,
                        PipelineOptions.DEPENDENCIES_MOUNTED_PATH,
                        PipelineOptions.EXTERNAL_DEPENDENCIES,
                        ApplicationConfiguration.EXTERNAL_DEPENDENCIES_NAME_MAPPING);
        Map<String, String> expected = getCommonVolumeAttributesMap();
        Map<String, String> expectedDeps = getCommonVolumeAttributesMap();
        long timestamp = System.currentTimeMillis();
        expected.put(
                "resourceList",
                "{"
                        + String.join(
                                ", ",
                                String.format(
                                        "\"AppMaster1.jar\": {\"path\": \"hdfs://haruna/AppMaster1.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"AppMaster2.zip\": {\"path\": \"hdfs://haruna/AppMaster2.zip\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.ARCHIVE))
                        + "}");
        expectedDeps.put(
                "resourceList",
                "{"
                        + String.join(
                                ", ",
                                String.format(
                                        "\"AppMaster3.jar\": {\"path\": \"hdfs://haruna/AppMaster3.jar\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.FILE),
                                String.format(
                                        "\"AppMaster4.zip\": {\"path\": \"hdfs://haruna/AppMaster4.zip\", \"timestamp\": %d, \"resourceType\": %d}",
                                        timestamp, CSIFileDownloadDecorator.LocalResource.ARCHIVE))
                        + "}");
        Map<String, String> actual =
                csiFileDownloadDecorator.getCsiVolumeAttributes(
                        csiFileDownloadDecorator.remoteFiles,
                        csiFileDownloadDecorator.pathToFileName,
                        timestamp);
        Map<String, String> actualDeps =
                csiDepsDownloadDecorator.getCsiVolumeAttributes(
                        csiDepsDownloadDecorator.remoteFiles,
                        csiDepsDownloadDecorator.pathToFileName,
                        timestamp);
        assertThat(actual).isEqualTo(expected);
        assertThat(actualDeps).isEqualTo(expectedDeps);
    }

    private Map<String, String> getCommonVolumeAttributesMap() {
        Map<String, String> commonAttributes = new HashMap<>();
        commonAttributes.put("volumeType", "Data");
        commonAttributes.put("ssdAffinity", "Prefer");
        return commonAttributes;
    }
}
