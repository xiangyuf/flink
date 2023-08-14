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

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.kubernetes.KubernetesPodTemplateTestUtils;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** Tests for {@link KubernetesUtils}. */
class KubernetesUtilsTest extends KubernetesTestBase {

    private static final FlinkPod EMPTY_POD = new FlinkPod.Builder().build();

    public File jarFolder;

    @BeforeEach
    void setupTempJarFolder(@TempDir File jarFolder) {
        this.jarFolder = jarFolder;
    }

    @Test
    void testParsePortRange() {
        final Configuration cfg = new Configuration();
        cfg.set(BlobServerOptions.PORT, "50100-50200");
        assertThatThrownBy(
                        () -> KubernetesUtils.parsePort(cfg, BlobServerOptions.PORT),
                        "Should fail with an exception.")
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(FlinkRuntimeException.class)
                                        .hasMessageContaining(BlobServerOptions.PORT.key()));
    }

    @Test
    void testParsePortNull() {
        final Configuration cfg = new Configuration();
        ConfigOption<String> testingPort =
                ConfigOptions.key("test.port").stringType().noDefaultValue();
        assertThatThrownBy(
                        () -> KubernetesUtils.parsePort(cfg, testingPort),
                        "Should fail with an exception.")
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(NullPointerException.class)
                                        .hasMessageContaining(
                                                testingPort.key() + " should not be null."));
    }

    @Test
    void testCheckWithDynamicPort() {
        testCheckAndUpdatePortConfigOption("0", "6123", "6123");
    }

    @Test
    void testCheckWithFixedPort() {
        testCheckAndUpdatePortConfigOption("6123", "16123", "6123");
    }

    @Test
    void testLoadPodFromNoSpecTemplate() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getNoSpecPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getMainContainer()).isEqualTo(EMPTY_POD.getMainContainer());
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers()).hasSize(0);
    }

    @Test
    void testLoadPodFromTemplateWithNonExistPathShouldFail() {
        final String nonExistFile = "/path/of/non-exist.yaml";
        final String msg = String.format("Pod template file %s does not exist.", nonExistFile);
        assertThatThrownBy(
                        () ->
                                KubernetesUtils.loadPodFromTemplateFile(
                                        flinkKubeClient,
                                        new File(nonExistFile),
                                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME),
                        "Kubernetes client should fail when the pod template file does not exist.")
                .satisfies(FlinkAssertions.anyCauseMatches(msg));
    }

    @Test
    void testLoadPodFromTemplateWithNoMainContainerShouldReturnEmptyMainContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        "nonExistMainContainer");
        assertThat(flinkPod.getMainContainer()).isEqualTo(EMPTY_POD.getMainContainer());
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers()).hasSize(2);
    }

    @Test
    void testLoadPodFromTemplateAndCheckMetaData() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        // The pod name is defined in the test/resources/testing-pod-template.yaml.
        final String expectedPodName = "pod-template";
        assertThat(flinkPod.getPodWithoutMainContainer().getMetadata().getName())
                .isEqualTo(expectedPodName);
    }

    @Test
    void testLoadPodFromTemplateAndCheckInitContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getInitContainers()).hasSize(1);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getInitContainers().get(0))
                .isEqualTo(KubernetesPodTemplateTestUtils.createInitContainer());
    }

    @Test
    void testLoadPodFromTemplateAndCheckMainContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getMainContainer().getName())
                .isEqualTo(KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getMainContainer().getVolumeMounts())
                .contains(KubernetesPodTemplateTestUtils.createVolumeMount());
    }

    @Test
    void testLoadPodFromTemplateAndCheckSideCarContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers()).hasSize(1);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers().get(0))
                .isEqualTo(KubernetesPodTemplateTestUtils.createSideCarContainer());
    }

    @Test
    void testLoadPodFromTemplateAndCheckVolumes() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getVolumes())
                .contains(KubernetesPodTemplateTestUtils.createVolumes());
    }

    @Test
    void testResolveUserDefinedValueWithNotDefinedInPodTemplate() {
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        null,
                        "container image");
        assertThat(resolvedImage).isEqualTo(CONTAINER_IMAGE);
    }

    @Test
    void testResolveUserDefinedValueWithDefinedInPodTemplateAndConfigOptionExplicitlySet() {
        final String imageInPodTemplate = "image-in-pod-template:v1";
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        imageInPodTemplate,
                        "container image");
        assertThat(resolvedImage).isEqualTo(CONTAINER_IMAGE);
    }

    @Test
    void testResolveUserDefinedValueWithDefinedInPodTemplateAndConfigOptionNotSet() {
        final String imageInPodTemplate = "image-in-pod-template:v1";
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        new Configuration(),
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        imageInPodTemplate,
                        "container image");
        assertThat(resolvedImage).isEqualTo(imageInPodTemplate);
    }

    private void testCheckAndUpdatePortConfigOption(
            String port, String fallbackPort, String expectedPort) {
        final Configuration cfg = new Configuration();
        cfg.setString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE, port);
        KubernetesUtils.checkAndUpdatePortConfigOption(
                cfg,
                HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
                Integer.valueOf(fallbackPort));
        assertThat(cfg.get(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE))
                .isEqualTo(expectedPort);
    }

    @Test
    public void testUploadDiskFilesWithOnlyRemoteFiles() throws IOException {
        final Configuration config = new Configuration();

        config.setString(PipelineOptions.JARS.key(), "hdfs:///path/of/user.jar");
        config.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList(
                        "hdfs:///path/of/file1.jar",
                        "hdfs:///path/file2.jar",
                        "hdfs:///path/file3.jar"));
        config.set(
                PipelineOptions.EXTERNAL_DEPENDENCIES, Arrays.asList("hdfs:///path/of/file2.jar"));
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_RESOURCES,
                "hdfs:///upload",
                PipelineOptions.EXTERNAL_RESOURCES,
                PipelineOptions.JARS);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                "hdfs:///upload/.dependencies",
                PipelineOptions.EXTERNAL_DEPENDENCIES);
        Assertions.assertEquals(
                4,
                config.get(PipelineOptions.EXTERNAL_RESOURCES).size(),
                "all remote files need to be added in external-resource list");
        Assertions.assertEquals(
                1,
                config.get(PipelineOptions.EXTERNAL_DEPENDENCIES).size(),
                "all remote dependencies files need to be added in external-dependencies list");
    }

    @Test
    public void testUploadDiskFilesContainsDiskFile() throws IOException {
        final Configuration config = new Configuration();
        File uploadDir = new File(jarFolder, "upload");
        File uploadDependenciesDir = new File(uploadDir, ".dependencies");
        File resourceFolder = new File(jarFolder, "resource");
        uploadDir.mkdir();
        uploadDependenciesDir.mkdirs();
        resourceFolder.mkdir();
        KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "user.jar");
        KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "file1.jar");
        KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "file2.jar");

        String userJar = new File(resourceFolder, "user.jar").toString();
        String file1 = new File(resourceFolder, "file1.jar").toString();
        String file2 = new File(resourceFolder, "file2.jar").toString();
        config.setString(PipelineOptions.JARS.key(), userJar);
        config.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList(file1, "hdfs:///path/file2.jar", "hdfs:///path/file3.jar"));
        config.set(
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                Arrays.asList(file2, "hdfs:///path/of/file1.jar", "hdfs:///path/of/file2.jar"));
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_RESOURCES,
                uploadDir.getPath(),
                PipelineOptions.EXTERNAL_RESOURCES,
                PipelineOptions.JARS);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                uploadDependenciesDir.getPath(),
                PipelineOptions.EXTERNAL_DEPENDENCIES);
        Assertions.assertEquals(
                4,
                config.get(PipelineOptions.EXTERNAL_RESOURCES).size(),
                "all remote files need to be added in external-resource list");
        Assertions.assertFalse(
                config.get(PipelineOptions.EXTERNAL_RESOURCES).contains(userJar),
                "disk file should be uploaded");
        Assertions.assertFalse(
                config.get(PipelineOptions.EXTERNAL_RESOURCES).contains(file1),
                "disk file should be uploaded");
        Assertions.assertEquals(
                3,
                config.get(PipelineOptions.EXTERNAL_DEPENDENCIES).size(),
                "all remote files need to be added in external-dependencies list");
        Assertions.assertFalse(
                config.get(PipelineOptions.EXTERNAL_DEPENDENCIES).contains(file2),
                "disk dependencies file should be uploaded");
    }

    @Test
    public void testUploadDiskFilesOnlyContainsUserJarInDisk() throws IOException {
        final Configuration config = new Configuration();
        File uploadDir = new File(jarFolder, "upload");
        File uploadDependenciesDir = new File(uploadDir, ".dependencies");
        File resourceFolder = new File(jarFolder, "resource");
        uploadDir.mkdir();
        uploadDependenciesDir.mkdirs();
        resourceFolder.mkdir();
        KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "user.jar");
        String userJar = new File(resourceFolder, "user.jar").toString();

        config.setString(PipelineOptions.JARS.key(), userJar);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_RESOURCES,
                uploadDir.getPath(),
                PipelineOptions.EXTERNAL_RESOURCES,
                PipelineOptions.JARS);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                uploadDependenciesDir.getPath(),
                PipelineOptions.EXTERNAL_DEPENDENCIES);
        Assertions.assertEquals(
                1,
                config.get(PipelineOptions.EXTERNAL_RESOURCES).size(),
                "all remote files need to be added in external-resource list");
        Assertions.assertFalse(
                config.get(PipelineOptions.EXTERNAL_RESOURCES).contains(userJar),
                "disk file should be uploaded");
    }

    @Test
    public void testUploadDiskFilesOnlyContainsUserJarInRemote() throws IOException {
        final Configuration config = new Configuration();

        config.setString(PipelineOptions.JARS.key(), "hdfs:///path/of/user.jar");
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_RESOURCES,
                "hdfs:///upload",
                PipelineOptions.EXTERNAL_RESOURCES,
                PipelineOptions.JARS);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                "hdfs:///upload/.dependencies",
                PipelineOptions.EXTERNAL_DEPENDENCIES);
        Assertions.assertEquals(
                1,
                config.get(PipelineOptions.EXTERNAL_RESOURCES).size(),
                "all remote files need to be added in external-resource list");
    }

    @Test
    public void testUploadFolder() throws IOException {
        final Configuration config = new Configuration();
        File uploadDir = new File(jarFolder, "upload");
        File uploadDependenciesDir = new File(uploadDir, ".dependencies");
        File resourceFolder = new File(jarFolder, "resource");
        uploadDir.mkdir();
        uploadDependenciesDir.mkdirs();
        resourceFolder.mkdir();
        config.setString(PipelineOptions.JARS.key(), "hdfs:///path/of/user.jar");

        // test the path with file scheme
        config.setString(
                PipelineOptions.EXTERNAL_RESOURCES.key(),
                new File("file://" + resourceFolder.getPath()).getPath());
        config.setString(
                PipelineOptions.EXTERNAL_DEPENDENCIES.key(),
                new File("file://" + resourceFolder.getPath()).getPath());
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_RESOURCES,
                uploadDir.getPath(),
                PipelineOptions.EXTERNAL_RESOURCES,
                PipelineOptions.JARS);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                uploadDependenciesDir.getPath(),
                PipelineOptions.EXTERNAL_DEPENDENCIES);
        // should ignore uploading a folder
        assertThat(config.get(PipelineOptions.EXTERNAL_RESOURCES))
                .isEqualTo(Collections.singletonList("hdfs:///path/of/user.jar"));
        Assertions.assertEquals(
                0,
                config.get(PipelineOptions.EXTERNAL_DEPENDENCIES).size(),
                "folder should be ignored when uploading");
        // test the path without indicating scheme
        config.setString(PipelineOptions.EXTERNAL_RESOURCES.key(), resourceFolder.getPath());
        config.setString(PipelineOptions.EXTERNAL_DEPENDENCIES.key(), resourceFolder.getPath());
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_RESOURCES,
                uploadDir.getPath(),
                PipelineOptions.EXTERNAL_RESOURCES,
                PipelineOptions.JARS);
        KubernetesUtils.uploadLocalDiskFilesToRemote(
                config,
                PipelineOptions.EXTERNAL_DEPENDENCIES,
                uploadDependenciesDir.getPath(),
                PipelineOptions.EXTERNAL_DEPENDENCIES);
        // should ignore uploading a folder
        assertThat(config.get(PipelineOptions.EXTERNAL_RESOURCES))
                .isEqualTo(Collections.singletonList("hdfs:///path/of/user.jar"));
        Assertions.assertEquals(
                0,
                config.get(PipelineOptions.EXTERNAL_DEPENDENCIES).size(),
                "folder should be ignored when uploading");
    }

    @Test
    public void testGetExternalFiles() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
        flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("hdfs://job/user.jar"));
        flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList("hdfs://job/file1.jar", "hdfs://job/file2.jar"));
        List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
        String[] expectedPath =
                new String[] {"/opt/tiger/workdir/file1.jar", "/opt/tiger/workdir/file2.jar"};
        assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
    }

    @Test
    public void testGetExternalFilesWithEmptyParameter() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///opt/usrlib/user.jar"));
        List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
        String[] expectedPath = new String[] {};
        assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
    }

    @Test
    public void testGetExternalFilesWithRepeatedUserJar() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
        flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("hdfs://job/user.jar"));
        flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList(
                        "hdfs://job/file1.jar", "hdfs://job/file2.jar", "hdfs://job/user.jar"));
        List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
        String[] expectedPath =
                new String[] {"/opt/tiger/workdir/file1.jar", "/opt/tiger/workdir/file2.jar"};
        assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
    }

    @Test
    public void testGetExternalFilesWithSameNameFiles() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
        flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList("hdfs://job/file1.jar", "hdfs://job/flink/file1.jar"));

        Map<String, String> pathToFileName = new HashMap<>();
        pathToFileName.put("hdfs://job/file1.jar", "file1.jar");
        pathToFileName.put("hdfs://job/flink/file1.jar", "0_file1.jar");
        flinkConfig.set(ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING, pathToFileName);
        List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
        String[] expectedPath =
                new String[] {"/opt/tiger/workdir/file1.jar", "/opt/tiger/workdir/0_file1.jar"};
        assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
    }

    @Test
    public void testGetExternalFilesWithNonJarFiles() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
        flinkConfig.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList(
                        "hdfs://job/file1.jar", "hdfs://job/file2.jar", "hdfs://job/sqlFile"));

        Map<String, String> pathToFileName = new HashMap<>();
        pathToFileName.put("hdfs://job/file1.jar", "file1.jar");
        flinkConfig.set(ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING, pathToFileName);
        List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
        String[] expectedPath =
                new String[] {"/opt/tiger/workdir/file1.jar", "/opt/tiger/workdir/file2.jar"};
        assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
    }
}
