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

package org.apache.flink.metrics.opentsdb;

import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Tag structure for Metrics. */
public class TagKv {
    String name;
    String value;

    public TagKv() {}

    public TagKv(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public static Tags compositeTags(List<TagKv> tagKvList) {
        return compositeTags(null, tagKvList);
    }

    public static Tags compositeTags(@Nullable Tags tags, List<TagKv> tagKvList) {
        Tags result = Tags.empty();
        if (tags != null) {
            result = Tags.merge(result, tags);
        }
        for (TagKv tagKv : tagKvList) {
            result = Tags.merge(result, Tags.keyValues(tagKv.name, tagKv.value));
        }
        return result;
    }

    public static String compositeTags(@Nullable String tagString, Map<String, String> tags) {
        StringBuilder sb;
        if (tagString == null) {
            sb = new StringBuilder();
        } else {
            sb = new StringBuilder(tagString + "|");
        }

        for (Map.Entry<String, String> entry : tags.entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            sb.append("|");
        }
        if (sb.length() == 0) {
            return "";
        }

        return sb.substring(0, sb.length() - 1);
    }

    @Override
    public String toString() {
        return name + "=" + value;
    }
}
