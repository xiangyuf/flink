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

package org.apache.flink.client.cli;

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** test download options for CliFronted. */
public class CliFrontedDownloadTest extends CliFrontendTestBase {

    @Rule public TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    public static void init() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterClass
    public static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @Test(expected = IOException.class)
    public void testDownload() throws Exception {
        String[] parameters = {
            "-src", "hdfs:///file1;hdfs:///file2", "-dest", "/opt/tiger/workdir"
        };
        Configuration configuration = new Configuration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        try {
            testFrontend.download(parameters);
        } catch (IOException e) {
            assertEquals("Hadoop is not in the classpath/dependencies.", e.getCause().getMessage());
            throw new IOException();
        }
    }

    @Test(expected = CliArgsException.class)
    public void testDownloadSavepath() throws Exception {
        // the destination directory should not end with "/"
        String[] parameters = {
            "-src", "hdfs:///file1;hdfs:///file2", "-dest", "/opt/tiger/workdir/"
        };
        Configuration configuration = new Configuration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.download(parameters);
    }

    @Test(expected = CliArgsException.class)
    public void testNoSavepath() throws Exception {
        String[] parameters = {"-src", "hdfs:///file1;hdfs:///file2", "-dest"};
        Configuration configuration = new Configuration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.download(parameters);
    }

    @Test
    public void testDownloadSameNameFiles() throws Exception {
        File file1 = testFolder.newFile("file1");
        File subFolder = testFolder.newFolder("sub-folder");
        File file2 = new File(subFolder, "file1");
        file2.createNewFile();
        File downloadDir = testFolder.newFolder("download");
        String[] parameters = {
            "-src", file1.getPath() + ";" + file2.getPath(), "-dest", downloadDir.getPath()
        };
        Configuration configuration = new Configuration();
        Map<String, String> fileNameMapping = new HashMap<>();
        fileNameMapping.put(file1.toURI().toString(), "file1");
        fileNameMapping.put(file2.toURI().toString(), "0_file1");
        configuration.set(
                ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING, fileNameMapping);
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.download(parameters);
        assertTrue(new File(downloadDir, "file1").exists());
        assertTrue(new File(downloadDir, "0_file1").exists());
    }
}
