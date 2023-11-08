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

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** Preview Table options. */
public class PreviewTableOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private boolean changelogModeEnable;
    private int changelogRowsMax;
    private boolean tableModeEnable;
    private int tableRowsMax;
    private FlinkConnectorRateLimiter rateLimiter;
    private boolean internalTestEnable;

    public PreviewTableOptions(
            boolean changelogModeEnable,
            int changelogRowsMax,
            boolean tableModeEnable,
            int tableRowsMax,
            FlinkConnectorRateLimiter rateLimiter,
            boolean internalTestEnable) {
        this.changelogModeEnable = changelogModeEnable;
        this.changelogRowsMax = changelogRowsMax;
        this.tableModeEnable = tableModeEnable;
        this.tableRowsMax = tableRowsMax;
        this.rateLimiter = rateLimiter;
        this.internalTestEnable = internalTestEnable;
    }

    public boolean isChangelogModeEnable() {
        return changelogModeEnable;
    }

    public void setChangelogModeEnable(boolean changelogModeEnable) {
        this.changelogModeEnable = changelogModeEnable;
    }

    public boolean isTableModeEnable() {
        return tableModeEnable;
    }

    public void setTableModeEnable(boolean tableModeEnable) {
        this.tableModeEnable = tableModeEnable;
    }

    public int getChangelogRowsMax() {
        return changelogRowsMax;
    }

    public void setChangelogRowsMax(int changelogRowsMax) {
        this.changelogRowsMax = changelogRowsMax;
    }

    public int getTableRowsMax() {
        return tableRowsMax;
    }

    public void setTableRowsMax(int tableRowsMax) {
        this.tableRowsMax = tableRowsMax;
    }

    public FlinkConnectorRateLimiter getRateLimiter() {
        return rateLimiter;
    }

    public void setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public boolean isInternalTestEnable() {
        return internalTestEnable;
    }

    public void setInternalTestEnable(boolean internalTestEnable) {
        this.internalTestEnable = internalTestEnable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PreviewTableOptions)) {
            return false;
        }
        PreviewTableOptions that = (PreviewTableOptions) o;
        return Objects.equals(changelogModeEnable, that.changelogModeEnable)
                && Objects.equals(changelogRowsMax, that.changelogRowsMax)
                && Objects.equals(tableModeEnable, that.tableModeEnable)
                && Objects.equals(tableRowsMax, that.tableRowsMax)
                && Objects.equals(rateLimiter, that.rateLimiter)
                && Objects.equals(internalTestEnable, that.internalTestEnable);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for Preview table. */
    public static class Builder {
        private boolean changelogModeEnable;
        private boolean tableModeEnable;
        private int changelogRowsMax;
        private int tableRowsMax;
        private FlinkConnectorRateLimiter rateLimiter;
        private boolean internalTestEnable;

        public Builder setChangelogModeEnable(boolean changelogModeEnable) {
            this.changelogModeEnable = changelogModeEnable;
            return this;
        }

        public Builder setTableModeEnable(boolean tableModeEnable) {
            this.tableModeEnable = tableModeEnable;
            return this;
        }

        public Builder setChangelogRowsMax(int changelogRowsMax) {
            this.changelogRowsMax = changelogRowsMax;
            return this;
        }

        public Builder setTableRowsMax(int tableRowsMax) {
            this.tableRowsMax = tableRowsMax;
            return this;
        }

        public Builder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
            this.rateLimiter = rateLimiter;
            return this;
        }

        public Builder setInternalTestEnable(boolean internalTestEnable) {
            this.internalTestEnable = internalTestEnable;
            return this;
        }

        public PreviewTableOptions build() {
            Preconditions.checkNotNull(changelogModeEnable, "changelogModeEnable can not be null");
            Preconditions.checkNotNull(tableModeEnable, "tableModeEnable can not be null");
            Preconditions.checkArgument(
                    changelogModeEnable ? changelogRowsMax > 0 : true,
                    "changelogRowsMax  must >= 0");
            Preconditions.checkArgument(
                    tableModeEnable ? tableRowsMax > 0 : true, "tableRowsMax must >= 0");
            return new PreviewTableOptions(
                    changelogModeEnable,
                    changelogRowsMax,
                    tableModeEnable,
                    tableRowsMax,
                    rateLimiter,
                    internalTestEnable);
        }
    }
}
