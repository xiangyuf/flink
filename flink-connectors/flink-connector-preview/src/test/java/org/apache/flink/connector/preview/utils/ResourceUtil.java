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

package org.apache.flink.connector.preview.utils;

import org.apache.flink.shaded.guava30.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava30.com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/** Utilities of read/write resources. */
public class ResourceUtil {

    public static String readFileContent(String filePath) throws IOException {
        return Files.toString(readFile(filePath), Charsets.UTF_8);
    }

    private static File readFile(String filePath) {
        return new File(
                Objects.requireNonNull(ResourceUtil.class.getClassLoader().getResource(filePath))
                        .getFile());
    }
}
