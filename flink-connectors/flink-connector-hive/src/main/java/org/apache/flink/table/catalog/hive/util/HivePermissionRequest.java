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

package org.apache.flink.table.catalog.hive.util;

import java.util.List;

/** Hive permission request pojo. */
public class HivePermissionRequest {
    private List<String> username;
    private List<DataSource> datasources;

    public List<String> getUsername() {
        return username;
    }

    public void setUsername(List<String> username) {
        this.username = username;
    }

    public List<DataSource> getDatasources() {
        return datasources;
    }

    public void setDatasources(List<DataSource> datasources) {
        this.datasources = datasources;
    }

    @Override
    public String toString() {
        return "HivePermissionRequest{"
                + "username="
                + username
                + ", datasources="
                + datasources
                + '}';
    }

    /** datasource. */
    public static class DataSource {
        private String datasource;
        private String database;
        private String table;
        private String column;
        private String priType;

        public String getDatasource() {
            return datasource;
        }

        public void setDatasource(String datasource) {
            this.datasource = datasource;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getPriType() {
            return priType;
        }

        public void setPriType(String priType) {
            this.priType = priType;
        }

        @Override
        public String toString() {
            return "DataSource{"
                    + "datasource='"
                    + datasource
                    + '\''
                    + ", database='"
                    + database
                    + '\''
                    + ", table='"
                    + table
                    + '\''
                    + ", column='"
                    + column
                    + '\''
                    + ", priType='"
                    + priType
                    + '\''
                    + '}';
        }
    }
}
