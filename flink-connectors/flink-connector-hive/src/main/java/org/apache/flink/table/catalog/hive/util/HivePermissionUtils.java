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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.HiveOptions;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.byted.com.bytedance.commons.consul.Discovery;
import org.apache.flink.shaded.byted.com.bytedance.commons.consul.ServiceNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.byted.security.ztijwthelper.LegacyIdentity;
import org.byted.security.ztijwthelper.ZTIJwtHelper;
import org.byted.security.ztijwthelper.ZtiJwtException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

/** Hive permission utils. */
public class HivePermissionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HivePermissionUtils.class);
    private static final String AUTHENTICATION_SERVER_PSM =
            "data.olap.gemini_readonly.service.lf.byted.org";
    private static final String AUTHENTICATION_SERVER_URL_TEMPLATE =
            "http://%s/api/query/verifyUsersPrivilege";
    private static final Map<String, String> HTTP_HEADER = getHeader();
    private static final String PSM_PREFIX = "psm_";
    private static final String DATASOURCE_HIVE = "hive";
    private static final String COLUMN_DELIMITER = "|";
    private static final String AUTHORIZATION_KEY = "Authorization";
    private static final String AUTHORIZATION_VAL = "YXBwX25hbWU6aW5mX2ZsaW5r";
    private static final String CONTENT_TYPE_KEY = "Content-Type";
    private static final String CONTENT_TYPE_VAL = "application/json";

    /**
     * Check whether user or psm has hive permission.
     *
     * @param user user used to check permission
     * @param psm psm used to check permission
     * @param database hive database
     * @param table hive table
     * @param columns columns in table
     * @param permissionType permission type
     * @param failOnPermissionDenied whether fail on permission denied. If true, this method will
     *     throw an exception on permission denied, otherwise just write a error log.
     * @return return whether user or psm has hive permission.
     */
    @Deprecated
    public static boolean checkPermission(
            String user,
            String psm,
            String database,
            String table,
            List<String> columns,
            PermissionType permissionType,
            boolean failOnPermissionDenied) {
        return checkPermission(
                user,
                psm,
                database,
                table,
                columns,
                permissionType,
                failOnPermissionDenied,
                getRequestUrl());
    }

    /**
     * Check whether user or psm has hive permission.
     *
     * @param user user used to check permission
     * @param psm psm used to check permission
     * @param database hive database
     * @param table hive table
     * @param columns columns in table
     * @param permissionType permission type
     * @param failOnPermissionDenied whether fail on permission denied. If true, this method will
     *     throw an exception on permission denied, otherwise just write a error log.
     * @param geminiServerUrl gemini server url
     * @return return whether user or psm has hive permission.
     */
    public static boolean checkPermission(
            String user,
            String psm,
            String database,
            String table,
            List<String> columns,
            PermissionType permissionType,
            boolean failOnPermissionDenied,
            String geminiServerUrl) {
        String postData = buildPostData(user, psm, database, table, columns, permissionType);
        try {
            LOG.info(
                    "Hive permission check geminiServerUrl = {}, postData = {}.",
                    geminiServerUrl,
                    postData);
            HttpUtil.HttpResponse response =
                    HttpUtil.sendPost(geminiServerUrl, postData, HTTP_HEADER);
            if (response.getStatusCode() >= 300) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Error response from hive catalog "
                                        + "server. url = %s, postData = %s, response_code = %s, response_data = %s.",
                                geminiServerUrl,
                                postData,
                                response.getStatusCode(),
                                response.getContent()));
            }
            LOG.info(
                    "Hive permission check responseStatusCode = {}, responseData = {}.",
                    response.getStatusCode(),
                    response.getContent());
            Tuple2<Boolean, String> permissionResult =
                    parsePermissionResultAccordingToResp(response.getContent());
            boolean hasPermission = permissionResult.f0;
            String columnsWithoutPermission = permissionResult.f1;
            if (hasPermission) {
                LOG.info(
                        "User: '{}' or psm: '{}' has {} permission for database: '{}', "
                                + "table: '{}', columns: '{}'.",
                        user,
                        psm,
                        permissionType,
                        database,
                        table,
                        columns);
            } else {
                String errorMsg =
                        String.format(
                                "Neither user: '%s' nor psm: '%s' "
                                        + "has %s permission for database: '%s', table: '%s', columns: '%s'.",
                                user, psm, permissionType, database, table, columns);
                if (columnsWithoutPermission != null && !columnsWithoutPermission.isEmpty()) {
                    errorMsg +=
                            String.format(
                                    " Columns without permission: %s.", columnsWithoutPermission);
                }
                if (failOnPermissionDenied) {
                    throw new FlinkRuntimeException(errorMsg);
                }
                LOG.error(errorMsg);
            }
            return hasPermission;
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to send request to hive catalog server. url = %s, postData = %s.",
                            geminiServerUrl, postData),
                    e);
        }
    }

    private static String getRequestUrl() {
        Discovery discovery = new Discovery();
        List<ServiceNode> serviceNodes = discovery.translateOne(AUTHENTICATION_SERVER_PSM);
        Preconditions.checkArgument(
                serviceNodes != null && !serviceNodes.isEmpty(),
                String.format(
                        "serviceNodes of psm: '%s' is null or empty.", AUTHENTICATION_SERVER_PSM));
        Collections.shuffle(serviceNodes);
        ServiceNode serviceNode =
                serviceNodes.get(ThreadLocalRandom.current().nextInt(serviceNodes.size()));
        String authenticationServer = serviceNode.getHost() + ":" + serviceNode.getPort();
        return String.format(AUTHENTICATION_SERVER_URL_TEMPLATE, authenticationServer);
    }

    /**
     * Parse the response data.
     *
     * @param jsonStr response json string.
     * @return permission result in form Tuple2: Tuple2.f0 is whether user/psm has permission,
     *     Tuple2.f1 is columns without permission.
     */
    private static Tuple2<Boolean, String> parsePermissionResultAccordingToResp(String jsonStr) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            HivePermissionResponse response =
                    objectMapper.readValue(jsonStr, HivePermissionResponse.class);
            List<HivePermissionResponse.Permission> permissionOfMultiUsers = response.getPri();

            String columnsWithoutPermission = null;
            // Permission for all users(we treat psm as a user with a prefix PSM_REFIX).
            for (HivePermissionResponse.Permission permission : permissionOfMultiUsers) {
                List<HivePermissionResponse.PermissionInfo> permissionInfos = permission.getPri();
                // Size of permissionInfos greater than one means current user/psm do not have
                // permissions in all columns or just part of the columns under new permission
                // validation.
                if (permissionInfos.size() > 1) {
                    StringJoiner colsWithoutPermission = new StringJoiner(",");
                    for (HivePermissionResponse.PermissionInfo perInfo : permissionInfos) {
                        if (!perInfo.isAuthorized()) {
                            colsWithoutPermission.add(perInfo.getColumns());
                        }
                        columnsWithoutPermission = colsWithoutPermission.toString();
                    }
                } else if (permissionInfos.size() == 1) {
                    // Size of permissionInfos is one means its the old permission validation or
                    // under new permission validation current user/psm have all permissions in all
                    // columns.
                    HivePermissionResponse.PermissionInfo permissionInfo = permissionInfos.get(0);
                    if (permissionInfo.isAuthorized()) {
                        return Tuple2.of(true, null);
                    } else {
                        columnsWithoutPermission = permissionInfo.getColumns();
                    }
                } else {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "The size of permissionInfos "
                                            + "must be greater than zero, but we get %s, "
                                            + "and the whole response is %s",
                                    permissionInfos.size(), response));
                }
            }
            return Tuple2.of(false, columnsWithoutPermission);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to parse hive permission response, jsonStr = %s.", jsonStr),
                    e);
        }
    }

    private static Map<String, String> getHeader() {
        Map<String, String> header = new HashMap<>();
        header.put(AUTHORIZATION_KEY, AUTHORIZATION_VAL);
        header.put(CONTENT_TYPE_KEY, CONTENT_TYPE_VAL);
        return header;
    }

    private static String buildPostData(
            String user,
            String psm,
            String database,
            String table,
            List<String> columns,
            PermissionType permissionType) {

        HivePermissionRequest request = new HivePermissionRequest();
        List<String> usernameList = Arrays.asList(user, PSM_PREFIX + psm);
        HivePermissionRequest.DataSource datasource = new HivePermissionRequest.DataSource();
        datasource.setDatasource(DATASOURCE_HIVE);
        datasource.setDatabase(database);
        datasource.setTable(table);
        datasource.setColumn(String.join(COLUMN_DELIMITER, columns));
        datasource.setPriType(permissionType.name().toLowerCase());
        request.setUsername(usernameList);
        request.setDatasources(Collections.singletonList(datasource));

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(request);
        } catch (JsonProcessingException e) {
            throw new FlinkRuntimeException(
                    "Fail to parse request to json string, request: " + request, e);
        }
    }

    public static LegacyIdentity getIdentityFromToken() {
        try {
            String token = ZTIJwtHelper.getJwtSVID();
            return ZTIJwtHelper.decodeGDPRorJwtSVID(token);
        } catch (ZtiJwtException e) {
            throw new FlinkRuntimeException("Failed to get token!", e);
        }
    }

    /** permission type. */
    public enum PermissionType {
        SELECT,
        ALL
    }

    public static boolean isPermissionCheckDisabled(ReadableConfig flinkConf) {
        return !flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_PERMISSION_CHECK_ENABLED);
    }
}
