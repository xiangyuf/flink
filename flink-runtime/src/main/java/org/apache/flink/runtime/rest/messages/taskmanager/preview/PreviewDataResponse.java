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

package org.apache.flink.runtime.rest.messages.taskmanager.preview;

import org.apache.flink.annotation.Public;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Base Preview Data. */
@Public
public class PreviewDataResponse implements Serializable, ResponseBody {
    private static final long serialVersionUID = 1L;

    private static final String CHANGE_LOG_RESULT = "changeLogResult";
    private static final String TABLE_RESULT = "tableResult";

    public PreviewDataResponse() {}

    @JsonCreator
    public PreviewDataResponse(
            @JsonProperty(CHANGE_LOG_RESULT) List<String> changeLogResult,
            @JsonProperty(TABLE_RESULT) List<String> tableResult) {
        this.changeLogResult = changeLogResult;
        this.tableResult = tableResult;
    }

    @JsonProperty(CHANGE_LOG_RESULT)
    private List<String> changeLogResult = new ArrayList<>();

    @JsonProperty(TABLE_RESULT)
    private List<String> tableResult = new ArrayList<>();

    public List<String> getChangeLogResult() {
        return changeLogResult;
    }

    public void setChangeLogResult(List<String> changeLogResult) {
        this.changeLogResult = changeLogResult;
    }

    public List<String> getTableResult() {
        return tableResult;
    }

    public void setTableResult(List<String> tableResult) {
        this.tableResult = tableResult;
    }
}
