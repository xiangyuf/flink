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

package org.apache.flink.client.deployment.application.classpath;

import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** The interface used to construct user classpath. */
public interface UserClasspathConstructor {

    List<URL> getUserJar(Configuration flinkConfiguration);

    List<URL> getExternalJars(Configuration flinkConfiguration);

    default List<URL> getUserLibFiles(@Nullable File jobDir) throws IOException {
        if (jobDir == null) {
            return Collections.emptyList();
        }
        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        return FileUtils.listFilesInDirectory(jobDir.toPath(), FileUtils::isJarFile).stream()
                .map(path -> FileUtils.relativizePath(workingDirectory, path))
                .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                .collect(Collectors.toList());
    }

    default List<URL> getClasspathInConfig(
            Configuration flinkConfiguration, @Nullable String flinkHome) {
        // get classpath in configurations from pipeline.classpath.
        // usually the connector & format jar is set in this parameter
        if (flinkConfiguration == null) {
            return Collections.emptyList();
        }
        try {
            if (StringUtils.isNullOrWhitespaceOnly(flinkHome)) {
                return ConfigUtils.decodeListFromConfig(
                        flinkConfiguration, PipelineOptions.CLASSPATHS, URL::new);
            }
            return ConfigUtils.decodeListFromConfig(
                            flinkConfiguration, PipelineOptions.CLASSPATHS, URL::new)
                    .stream()
                    .map(url -> url.toString().replace("%FLINK_HOME%", flinkHome))
                    .map(FunctionUtils.uncheckedFunction(URL::new))
                    .collect(Collectors.toList());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get flink user classpath from the configuration. The classpath will follow the order: user
     * jar, files in usr lib, external jar, pipeline.classpath.
     *
     * @param userClasspathConstructor the constructor used to get user jar, external jar, user lib,
     *     and files in pipeline.classpath
     * @param flinkConfig The flink configuration
     * @param jobDir The user lib directory in container.
     * @param flinkHome The flink home path
     * @return The constructed user classpath.
     */
    static List<URL> getFlinkUserClasspath(
            UserClasspathConstructor userClasspathConstructor,
            Configuration flinkConfig,
            @Nullable File jobDir,
            @Nullable String flinkHome)
            throws IOException {
        List<URL> userClasspathList = new LinkedList<>();
        // get user jar
        userClasspathList.addAll(userClasspathConstructor.getUserJar(flinkConfig));
        // get user lib jar
        userClasspathList.addAll(userClasspathConstructor.getUserLibFiles(jobDir));
        // get external files
        userClasspathList.addAll(userClasspathConstructor.getExternalJars(flinkConfig));
        // get pipeline.classpath in config
        userClasspathList.addAll(
                userClasspathConstructor.getClasspathInConfig(flinkConfig, flinkHome));
        return userClasspathList;
    }
}
