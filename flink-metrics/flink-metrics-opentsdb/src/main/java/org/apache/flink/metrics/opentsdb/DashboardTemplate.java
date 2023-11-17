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

package org.apache.flink.metrics.opentsdb;

/** Dashboard template path, associated Grafana JSON template file location. */
public class DashboardTemplate {

    /** Templates for checkpoint overview metrics. */
    public static final String CHECKPOINT_OVERVIEW_TEMPLATE =
            "checkpoint/checkpoint_overview_template.txt";
    /** Templates for jvm metrics. */
    public static final String JM_GC_TEMPLATE = "jvm/jm_gc_template.txt";

    public static final String JM_MEMORY_TEMPLATE = "jvm/jm_memory_template.txt";
    public static final String TM_GC_TEMPLATE = "jvm/tm_gc_template.txt";
    public static final String TM_MEMORY_TEMPLATE = "jvm/tm_memory_template.txt";
    public static final String JVM_ROW_TEMPLATE = "jvm/jvm_row_template.txt";
    public static final String JM_THREAD_TEMPLATE = "jvm/jm_thread_template.txt";
    public static final String TM_THREAD_TEMPLATE = "jvm/tm_thread_template.txt";

    /** Templates for kafka metrics. */
    public static final String KAFKA_LAG_SIZE_TARGET_TEMPLATE =
            "kafka/kafka_lag_size_target_template.txt";

    public static final String KAFKA_LAG_SIZE_TEMPLATE = "kafka/kafka_lag_size_template.txt";

    public static final String KAFKA_LAG_LINK_TEMPLATE = "kafka/kafka_lag_link_template.txt";

    /** Templates for network metrics. */
    public static final String NETWORK_ROW_TEMPLATE = "network/network_row_template.txt";

    public static final String NETWORK_MEMORY_TEMPLATE = "network/network_memory_template.txt";
    public static final String POOL_USAGE_TEMPLATE = "network/pool_usage_template.txt";
    public static final String RECORD_NUM_TEMPLATE = "network/record_num_template.txt";
    public static final String PENDING_RECORD_TEMPLATE = "pending_record_template.txt";

    /** Templates for Streaming warehouse. */
    public static final String STREAMINGWAREHOUSE_ROW_TEMPLATE =
            "streamingwarehouse/streamingwarehouse_row_template.txt";

    public static final String RECORD_OUT_TEMPLATE = "streamingwarehouse/record_out_template.txt";
    public static final String BYTES_OUT_TEMPLATE = "streamingwarehouse/bytes_out_template.txt";
    public static final String CURRENT_FETCH_EVENT_TIME_LAG_TEMPLATE =
            "streamingwarehouse/current_fetch_event_time_Lag_template.txt";

    /** Templates for schedule related metrics. */
    public static final String SCHEDULE_INFO_ROW_TEMPLATE =
            "scheduleinfo/schedule_info_row_template.txt";

    public static final String TASK_MANAGER_SLOT_TEMPLATE =
            "scheduleinfo/taskmanager_slot_template.txt";
    public static final String COMPLETED_CONTAINER_TEMPLATE =
            "scheduleinfo/completed_container_template.txt";

    /** Templates for watermark related metrics. */
    public static final String WATERMARK_ROW_TEMPLATE = "watermark/watermark_row_template.txt";

    public static final String LATE_RECORDS_DROPPED_TEMPLATE =
            "watermark/late_records_dropped_template.txt";

    /** Templates for overview. */
    public static final String OVERVIEW_ROW_TEMPLATE = "overview_row_template.txt";

    public static final String OVERVIEW_TEMPLATE = "overview_template.txt";
    public static final String JOB_INFO_TEMPLATE = "job_info_template.txt";
    public static final String TASK_STATUS_TEMPLATE = "task_status_template.txt";

    public static final String DASHBOARD_TEMPLATE = "dashboard_template.txt";
}
