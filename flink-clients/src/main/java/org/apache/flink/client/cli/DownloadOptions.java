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

import org.apache.flink.util.StringUtils;

import org.apache.commons.cli.CommandLine;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendParser.DOWNLOAD_DEST_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.DOWNLOAD_SRC_OPTION;

/** Class for command line options that refer to download files. */
public class DownloadOptions extends CommandLineOptions {

    private List<String> remoteFiles;
    private String savePath;

    protected DownloadOptions(CommandLine line) {
        super(line);
        String remoteFiles =
                line.hasOption(DOWNLOAD_SRC_OPTION.getOpt())
                        ? line.getOptionValue(DOWNLOAD_SRC_OPTION.getOpt())
                        : null;
        this.remoteFiles =
                StringUtils.isNullOrWhitespaceOnly(remoteFiles)
                        ? Collections.emptyList()
                        : Arrays.asList(remoteFiles.split(";"));
        this.savePath =
                line.hasOption(DOWNLOAD_DEST_OPTION.getOpt())
                        ? line.getOptionValue(DOWNLOAD_DEST_OPTION.getOpt())
                        : null;
    }

    public void validate() throws CliArgsException {
        if (remoteFiles == null || remoteFiles.isEmpty()) {
            throw new CliArgsException("Should specify the files you want to download.");
        }
        if (StringUtils.isNullOrWhitespaceOnly(savePath)) {
            throw new CliArgsException("Should specify the save path.");
        }
        if (savePath.endsWith("/")) {
            throw new CliArgsException("The save path don't need to end with '/'");
        }
    }

    public List<String> getRemoteFiles() {
        return remoteFiles;
    }

    public String getSavePath() {
        return savePath;
    }
}
