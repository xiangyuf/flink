/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.preview;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.preview.utils.ResourceUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link PreviewTableSinkFactory}. */
public class PreviewTableSinkITTest {
    private static final String PATH = "preview/";

    private static final int PREVIEW_PARALLELISM = 1;

    private final RowTypeInfo testTypeInfo1 =
            new RowTypeInfo(
                    new TypeInformation[] {Types.STRING, Types.INT},
                    new String[] {"word", "frequency"});

    private List<Row> initData() {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(Row.of("word", 1));
        }
        return data;
    }

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @Before
    public void setStreams() {
        System.setOut(new PrintStream(out));
    }

    @After
    public void restoreInitialStreams() {
        System.setOut(originalOut);
    }

    @Test
    public void testPreviewSinkChangeLogOnly() throws Exception {
        EnvironmentSettings streamSettings;
        List<Row> testData = initData();
        streamSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(PREVIEW_PARALLELISM);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        DataStream<Row> ds = execEnv.fromCollection(testData).returns(testTypeInfo1);
        tEnv.createTemporaryView("src", ds);
        String sinkDDL =
                " CREATE  TABLE preview_test (name VARCHAR PRIMARY KEY NOT ENFORCED, cnt INT)\n"
                        + "    WITH (\n"
                        + "    'connector' = 'preview', \n"
                        + "    'changelog-mode.enable' = 'true', \n"
                        + "    'table-mode.enable' = 'false', \n"
                        + "    'internal-test.enable' = 'true', \n"
                        + "    'format' = 'json') ";
        tEnv.executeSql(sinkDDL);
        String query =
                "INSERT INTO preview_test SELECT word, sum(frequency) FROM src group by word";
        TableResult tableResult = tEnv.executeSql(query);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        assertEquals(ResourceUtil.readFileContent(PATH + "changelog_only.out"), out.toString());
    }

    @Test
    public void testPreviewSinkTableResultOnly() throws Exception {
        EnvironmentSettings streamSettings;
        List<Row> testData = initData();
        streamSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(PREVIEW_PARALLELISM);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        DataStream<Row> ds = execEnv.fromCollection(testData).returns(testTypeInfo1);
        tEnv.createTemporaryView("src", ds);
        String sinkDDL =
                " CREATE  TABLE preview_test (name VARCHAR PRIMARY KEY NOT ENFORCED, cnt INT)\n"
                        + "    WITH (\n"
                        + "    'connector' = 'preview', \n"
                        + "    'table-mode.enable' = 'true', \n"
                        + "    'changelog-mode.enable' = 'false', \n"
                        + "    'internal-test.enable' = 'true', \n"
                        + "    'format' = 'json') ";
        tEnv.executeSql(sinkDDL);
        String query =
                "INSERT INTO preview_test SELECT word, sum(frequency) FROM src group by word";
        TableResult tableResult = tEnv.executeSql(query);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        assertEquals(ResourceUtil.readFileContent(PATH + "table_only.out"), out.toString());
    }

    @Test
    public void testPreviewSinkChangeAndTableResult() throws Exception {
        EnvironmentSettings streamSettings;
        List<Row> testData = initData();
        streamSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(PREVIEW_PARALLELISM);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        DataStream<Row> ds = execEnv.fromCollection(testData).returns(testTypeInfo1);
        tEnv.createTemporaryView("src", ds);
        String sinkDDL =
                " CREATE  TABLE preview_test (name VARCHAR PRIMARY KEY NOT ENFORCED, cnt INT)\n"
                        + "    WITH (\n"
                        + "    'connector' = 'preview', \n"
                        + "    'internal-test.enable' = 'true', \n"
                        + "    'format' = 'json') ";
        tEnv.executeSql(sinkDDL);
        String query =
                "INSERT INTO preview_test SELECT word, sum(frequency) FROM src group by word";
        TableResult tableResult = tEnv.executeSql(query);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        assertEquals(ResourceUtil.readFileContent(PATH + "changelog_table.out"), out.toString());
    }

    @Test
    public void testPreviewSinkRowMax() throws Exception {
        EnvironmentSettings streamSettings;
        List<Row> testData = initData();
        streamSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(PREVIEW_PARALLELISM);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

        DataStream<Row> ds = execEnv.fromCollection(testData).returns(testTypeInfo1);
        tEnv.createTemporaryView("src", ds);
        String sinkDDL =
                " CREATE  TABLE preview_test (name VARCHAR PRIMARY KEY NOT ENFORCED, cnt INT)\n"
                        + "    WITH (\n"
                        + "    'connector' = 'preview', \n"
                        + "    'internal-test.enable' = 'true', \n"
                        + "    'changelog-mode.result-rows.max' = '5', \n"
                        + "    'table-mode.result-rows.max' = '5', \n"
                        + "    'format' = 'json') ";
        tEnv.executeSql(sinkDDL);
        String query = "INSERT INTO preview_test SELECT word, frequency FROM src ";
        TableResult tableResult = tEnv.executeSql(query);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        assertEquals(
                ResourceUtil.readFileContent(PATH + "changelog_table_row_max.out"), out.toString());
    }
}
