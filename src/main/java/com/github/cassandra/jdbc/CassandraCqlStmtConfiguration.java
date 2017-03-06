/**
 * Copyright (C) 2015-2017, Zhichun Wu
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.cassandra.jdbc;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;

import java.util.Map;
import java.util.Properties;

public class CassandraCqlStmtConfiguration {
    private static final String FLD_STATEMENT_TYPE = "StatementType";
    private static final String FLD_SERIAL_CONSISTENCY_LEVEL = "SerialConsistencyLevel";

    private static final String KEY_CONSISTENCY_LEVEL = "consistency_level";
    private static final String KEY_FETCH_SIZE = "fetch_size";
    private static final String KEY_NO_LIMIT = "no_limit";
    private static final String KEY_NO_WAIT = "no_wait";
    private static final String KEY_READ_TIMEOUT = "read_timeout";
    private static final String KEY_REPLACE_NULL_VALUE = "replace_null_value";
    private static final String KEY_SQL_PARSER = "sql_parser";
    private static final String KEY_TRACING = "tracing";

    private final CassandraStatementType stmtType;
    private final CassandraConfiguration connectionConfig;

    private final String consistencyLevel;
    private final String serialConsistencyLevel;
    private final int fetchSize;
    private final boolean noLimit;
    private final boolean noWait;
    private final int readTimeout; // in seconds
    private final boolean replaceNullValue;
    private final boolean sqlParser;
    private final boolean tracing;

    public CassandraCqlStmtConfiguration(CassandraConfiguration connectionConfig, CassandraStatementType stmtType,
                                         Map<String, String> stmtOptions) {
        this.connectionConfig = connectionConfig;
        this.stmtType = stmtType;

        Properties options = new Properties();
        if (stmtOptions != null) {
            options.putAll(stmtOptions);
        }

        CassandraEnums.ConsistencyLevel preferredCL = connectionConfig.getConsistencyLevel();
        if (stmtType.isQuery()) {
            preferredCL = connectionConfig.getReadConsistencyLevel();
        } else if (stmtType.isUpdate()) {
            preferredCL = connectionConfig.getWriteConsistencyLevel();
        }

        consistencyLevel = options.getProperty(KEY_CONSISTENCY_LEVEL, preferredCL.name()).trim().toUpperCase();
        // TODO better to check if there's IF condition in the update CQL before doing so
        serialConsistencyLevel = stmtType.isUpdate()
                && (preferredCL == CassandraEnums.ConsistencyLevel.LOCAL_SERIAL
                || preferredCL == CassandraEnums.ConsistencyLevel.SERIAL) ? preferredCL.name() : null;

        String value = options.getProperty(KEY_FETCH_SIZE);
        // -1 implies using the one defined in Statement / PreparedStatement
        fetchSize = Strings.isNullOrEmpty(value) ? -1 : Ints.tryParse(value);
        noLimit = Boolean.valueOf(options.getProperty(KEY_NO_LIMIT, null));
        noWait = Boolean.valueOf(options.getProperty(KEY_NO_WAIT, null));
        tracing = Boolean.valueOf(options.getProperty(KEY_TRACING, String.valueOf(connectionConfig.isTracingEnabled())));
        value = options.getProperty(KEY_READ_TIMEOUT);
        // convert second to millisecond
        readTimeout = Strings.isNullOrEmpty(value) ? connectionConfig.getReadTimeout() : Ints.tryParse(value) * 1000;
        replaceNullValue = Boolean.valueOf(options.getProperty(KEY_REPLACE_NULL_VALUE, null));
        sqlParser = Boolean.valueOf(options.getProperty(KEY_SQL_PARSER, String.valueOf(connectionConfig.isSqlFriendly())));
    }

    public CassandraConfiguration getConnectionConfig() {
        return this.connectionConfig;
    }

    public boolean hasSetFetchSize() {
        return fetchSize > 0;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public CassandraStatementType getStatementType() {
        return this.stmtType;
    }

    /**
     * Get read timeout(read_timeout) from magic comment.
     *
     * @return read timeout in seconds
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Get consistency level(consistency_level) from magic comment.
     *
     * @return consistency level
     */
    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    public String getSerialConsistencyLevel() {
        return serialConsistencyLevel;
    }

    public boolean noLimit() {
        return noLimit;
    }

    public boolean noWait() {
        return noWait;
    }

    public boolean tracingEnabled() {
        return tracing;
    }

    public boolean sqlParserEnabled() {
        return sqlParser;
    }

    public boolean replaceNullValue() {
        return replaceNullValue;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(stmtType, consistencyLevel, serialConsistencyLevel, fetchSize, noLimit, noWait, tracing,
                readTimeout, replaceNullValue, sqlParser);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CassandraCqlStmtConfiguration other = (CassandraCqlStmtConfiguration) obj;

        return Objects.equal(this.stmtType, other.stmtType)
                && Objects.equal(this.consistencyLevel, other.consistencyLevel)
                && Objects.equal(this.serialConsistencyLevel, other.serialConsistencyLevel)
                && Objects.equal(this.fetchSize, other.fetchSize)
                && Objects.equal(this.noLimit, other.noLimit)
                && Objects.equal(this.noWait, other.noWait)
                && Objects.equal(this.tracing, other.tracing)
                && Objects.equal(this.readTimeout, other.readTimeout)
                && Objects.equal(this.replaceNullValue, other.replaceNullValue)
                && Objects.equal(this.sqlParser, other.sqlParser);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add(FLD_STATEMENT_TYPE, this.stmtType)
                .add(KEY_CONSISTENCY_LEVEL, this.consistencyLevel)
                .add(FLD_SERIAL_CONSISTENCY_LEVEL, this.serialConsistencyLevel)
                .add(KEY_FETCH_SIZE, this.fetchSize)
                .add(KEY_NO_LIMIT, this.noLimit)
                .add(KEY_NO_WAIT, this.noWait)
                .add(KEY_TRACING, this.tracing)
                .add(KEY_READ_TIMEOUT, this.readTimeout)
                .add(KEY_REPLACE_NULL_VALUE, this.replaceNullValue)
                .add(KEY_SQL_PARSER, this.sqlParser)
                .toString();
    }
}
