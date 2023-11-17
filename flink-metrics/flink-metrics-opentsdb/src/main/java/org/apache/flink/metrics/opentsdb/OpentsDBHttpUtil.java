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

import org.apache.flink.shaded.httpclient.org.apache.http.client.config.RequestConfig;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpGet;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpPost;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpUriRequest;
import org.apache.flink.shaded.httpclient.org.apache.http.entity.StringEntity;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.HttpClients;
import org.apache.flink.shaded.httpclient.org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

/** Provides http methods. */
public class OpentsDBHttpUtil {
    private static final RequestConfig requestConfig =
            RequestConfig.custom()
                    .setSocketTimeout(5000)
                    .setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .build();

    public static HttpResponsePojo sendPost(String url, String jsonStr, Map<String, String> headers)
            throws IOException {
        HttpPost httpPost = new HttpPost(url);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            httpPost.addHeader(entry.getKey(), entry.getValue());
        }
        StringEntity entity = new StringEntity(jsonStr, "UTF-8");
        httpPost.setEntity(entity);
        httpPost.setConfig(RequestConfig.copy(requestConfig).build());
        return sendRequest(httpPost);
    }

    public static HttpResponsePojo sendGet(String url) throws IOException {
        return sendGet(url, null);
    }

    public static HttpResponsePojo sendGet(String url, Map<String, String> headers)
            throws IOException {
        HttpGet httpGet = new HttpGet(url);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpGet.addHeader(entry.getKey(), entry.getValue());
            }
        }
        httpGet.setConfig(RequestConfig.copy(requestConfig).build());
        return sendRequest(httpGet);
    }

    public static HttpResponsePojo sendRequest(HttpUriRequest request) throws IOException {
        try (CloseableHttpClient closeableHttpClient =
                        HttpClients.custom()
                                .setDefaultRequestConfig(RequestConfig.copy(requestConfig).build())
                                .build();
                CloseableHttpResponse response = closeableHttpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            String resStr = EntityUtils.toString(response.getEntity(), "UTF-8");
            return new HttpResponsePojo(statusCode, resStr);
        }
    }

    /** Http response pojo, which contains status code and data. */
    public static class HttpResponsePojo {

        private int statusCode;
        private String content;

        public HttpResponsePojo(int statusCode, String content) {
            this.statusCode = statusCode;
            this.content = content;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public void setStatusCode(int statusCode) {
            this.statusCode = statusCode;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        @Override
        public String toString() {
            return "HttpResponsePojo{"
                    + "statusCode="
                    + statusCode
                    + ", content='"
                    + content
                    + '\''
                    + '}';
        }
    }
}
