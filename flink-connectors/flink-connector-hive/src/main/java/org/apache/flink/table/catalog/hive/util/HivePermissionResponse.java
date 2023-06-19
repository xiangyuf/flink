/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contriutor license agreements.  See the NOTICE file
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

/** Hive row permission response pojo. */
public class HivePermissionResponse {
    private int code;

    // Permission list of multiple users.
    private List<Permission> pri;

    private String message;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public List<Permission> getPri() {
        return pri;
    }

    public void setPri(List<Permission> pri) {
        this.pri = pri;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "HiveRowPermissionResponse{"
                + "code="
                + code
                + ", pri="
                + pri
                + ", message='"
                + message
                + '\''
                + '}';
    }

    /** Permission of one user. */
    public static class Permission {
        private List<PermissionInfo> pri;
        private String user;

        public List<PermissionInfo> getPri() {
            return pri;
        }

        public void setPri(List<PermissionInfo> pri) {
            this.pri = pri;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        @Override
        public String toString() {
            return "Permission{" + "pri=" + pri + ", user='" + user + '\'' + '}';
        }
    }

    /** The real permission info. */
    public static class PermissionInfo {
        private boolean authorized;

        // columns without permissions.
        private String columns;

        public boolean isAuthorized() {
            return authorized;
        }

        public void setAuthorized(boolean authorized) {
            this.authorized = authorized;
        }

        public String getColumns() {
            return columns;
        }

        public void setColumns(String columns) {
            this.columns = columns;
        }

        @Override
        public String toString() {
            return "PrmissionInfo{"
                    + "authorized="
                    + authorized
                    + ", columns='"
                    + columns
                    + '\''
                    + '}';
        }
    }
}
