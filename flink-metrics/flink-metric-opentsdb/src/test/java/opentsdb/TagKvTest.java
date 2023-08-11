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

package opentsdb;

import org.apache.flink.metrics.opentsdb.TagKv;

import org.apache.flink.shaded.byted.com.bytedance.metrics2.api.Tags;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** UT for TagKv. */
public class TagKvTest {
    @Test
    public void testCompositeTags1() {
        List<TagKv> tags = new ArrayList<>();
        Assert.assertEquals(Tags.empty(), TagKv.compositeTags(tags));
    }

    @Test
    public void testCompositeTags2() {
        List<TagKv> tags = new ArrayList<>();
        tags.add(new TagKv("host", "n8-131-216"));
        Assert.assertEquals(Tags.keyValues("host", "n8-131-216"), TagKv.compositeTags(tags));
    }

    @Test
    public void testCompositeTags3() {
        List<TagKv> tags = new ArrayList<>();
        tags.add(new TagKv("host", "n8-131-216"));
        tags.add(new TagKv("jobname", "HelloWorld"));
        Assert.assertEquals(
                Tags.keyValues("host", "n8-131-216", "jobname", "HelloWorld"),
                TagKv.compositeTags(tags));
    }

    @Test
    public void testCompositeTags4() {
        String tag = "host=n8-131-216|jobname=HelloWorld";
        Map<String, String> tagMap = new TreeMap<>();
        tagMap.put("key1", "val1");
        tagMap.put("key2", "val2");
        tagMap.put("key3", "val3");
        tag = TagKv.compositeTags(tag, tagMap);
        Assert.assertEquals(
                "host=n8-131-216|jobname=HelloWorld|key1=val1|key2=val2|key3=val3", tag);
    }
}
