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

package org.apache.flink.kubernetes.kubeclient.classpath;

import org.apache.flink.client.deployment.application.classpath.UserClasspathConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Test for KubernetesUserClasspathConstructor. */
public class KubernetesUserClasspathConstructorTest extends TestCase {

    @Test
    public void testGetFlinkUserClasspathWithoutAnyJar() throws IOException {
        Configuration configuration = new Configuration();
        List<URL> userClasspath =
                UserClasspathConstructor.getFlinkUserClasspath(
                        KubernetesUserClasspathConstructor.INSTANCE, configuration, null, null);
        Assert.assertArrayEquals(new URL[0], userClasspath.toArray());
    }

    @Test
    public void testGetFlinkUserClasspathWithEmptyJar() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.JARS, Collections.emptyList());
        configuration.set(PipelineOptions.EXTERNAL_RESOURCES, Collections.emptyList());
        configuration.set(PipelineOptions.CLASSPATHS, Collections.emptyList());
        List<URL> userClasspath =
                UserClasspathConstructor.getFlinkUserClasspath(
                        KubernetesUserClasspathConstructor.INSTANCE, configuration, null, null);
        Assert.assertArrayEquals(new URL[0], userClasspath.toArray());
    }

    @Test
    public void testGetFlinkUserClasspathWithExternalFiles() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList("local:///ext1.jar", "hdfs:///ext2.jar"));
        List<URL> userClasspath =
                UserClasspathConstructor.getFlinkUserClasspath(
                        KubernetesUserClasspathConstructor.INSTANCE, configuration, null, null);
        Assert.assertArrayEquals(
                Arrays.asList(
                                new URL("file:/ext1.jar"),
                                new URL("file:/opt/tiger/workdir/ext2.jar"))
                        .toArray(),
                userClasspath.toArray());
    }

    @Test
    public void testGetFlinkUserClasspathWithConnector() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(
                PipelineOptions.CLASSPATHS,
                Arrays.asList("file:///connector1.jar", "file:///connector2.jar"));
        List<URL> userClasspath =
                UserClasspathConstructor.getFlinkUserClasspath(
                        KubernetesUserClasspathConstructor.INSTANCE, configuration, null, null);
        Assert.assertArrayEquals(
                Arrays.asList(new URL("file:/connector1.jar"), new URL("file:/connector2.jar"))
                        .toArray(),
                userClasspath.toArray());
    }

    @Test
    public void testGetFlinkUserClasspathInOrder() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(
                PipelineOptions.EXTERNAL_RESOURCES,
                Arrays.asList("local:///ext1.jar", "hdfs:///ext2.jar"));
        configuration.set(
                PipelineOptions.CLASSPATHS,
                Arrays.asList("file:///connector1.jar", "file:///connector2.jar"));
        List<URL> userClasspath =
                UserClasspathConstructor.getFlinkUserClasspath(
                        KubernetesUserClasspathConstructor.INSTANCE, configuration, null, null);
        Assert.assertArrayEquals(
                Arrays.asList(
                                new URL("file:/ext1.jar"),
                                new URL("file:/opt/tiger/workdir/ext2.jar"),
                                new URL("file:/connector1.jar"),
                                new URL("file:/connector2.jar"))
                        .toArray(),
                userClasspath.toArray());
    }
}
