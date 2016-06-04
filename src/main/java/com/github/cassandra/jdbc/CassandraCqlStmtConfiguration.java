/*
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
 *
 */
package com.github.cassandra.jdbc;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;

import java.util.Map;
import java.util.Properties;

public class CassandraCqlStmtConfiguration {
    static final String KEY_CONSISTENCY_LEVEL = "consistency_level";
    static final String KEY_EXEC_ASYNC = "exec_async";
    static final String KEY_FETCH_SIZE = "fetch_size";
    static final String KEY_NO_LIMIT = "no_limit";
    static final String KEY_READ_TIMEOUT = "read_timeout";
    static final String KEY_REPLACE_NULL_VALUE = "replace_null_value";
    static final String KEY_SQL_PARSER = "sql_parser";
    static final String KEY_TRACING = "tracing";

    private final CassandraStatementType type;
    private final CassandraConfiguration config;

    private final String consistencyLevel;
    private final boolean execAsync;
    private final int fetchSize;
    private final boolean noLimit;
    private final int readTimeout; // in seconds
    private final boolean replaceNullValue;
    private final boolean sqlParser;
    private final boolean tracing;

    public CassandraCqlStmtConfiguration(CassandraConfiguration config, CassandraStatementType type,
                                         Map<String, String> hints) {
        this.config = config;
        this.type = type;

        Properties props = new Properties();
        if (hints != null) {
            props.putAll(hints);
        }

        CassandraEnums.ConsistencyLevel preferredCL = config.getConsistencyLevel();
        if (type.isQuery()) {
            preferredCL = config.getReadConsistencyLevel();
        } else if (type.isUpdate()) {
            preferredCL = config.getWriteConsistencyLevel();
        }

        consistencyLevel = props.getProperty(KEY_CONSISTENCY_LEVEL, preferredCL.name()).trim().toUpperCase();

        String value = props.getProperty(KEY_FETCH_SIZE);
        execAsync = Boolean.valueOf(props.getProperty(KEY_EXEC_ASYNC, null));
        // -1 implies using the one defined in Statement / PreparedStatement
        fetchSize = Strings.isNullOrEmpty(value) ? -1 : Ints.tryParse(value);
        noLimit = Boolean.valueOf(props.getProperty(KEY_NO_LIMIT, null));
        tracing = Boolean.valueOf(props.getProperty(KEY_TRACING, String.valueOf(config.isTracingEnabled())));
        value = props.getProperty(KEY_READ_TIMEOUT);
        // convert second to millisecond
        readTimeout = Strings.isNullOrEmpty(value) ? config.getReadTimeout() : Ints.tryParse(value) * 1000;
        replaceNullValue = Boolean.valueOf(props.getProperty(KEY_REPLACE_NULL_VALUE, null));
        sqlParser = Boolean.valueOf(props.getProperty(KEY_SQL_PARSER, String.valueOf(config.isSqlFriendly())));
    }

    public boolean executeAsync() {
        return execAsync;
    }

    public boolean hasSetFetchSize() {
        return fetchSize > 0;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public CassandraStatementType getStatementType() {
        return this.type;
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

    public boolean noLimit() {
        return noLimit;
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
        return Objects.hashCode(consistencyLevel, execAsync, fetchSize, noLimit, tracing,
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

        return Objects.equal(this.consistencyLevel, other.consistencyLevel)
                && Objects.equal(this.execAsync, other.execAsync)
                && Objects.equal(this.fetchSize, other.fetchSize)
                && Objects.equal(this.noLimit, other.noLimit)
                && Objects.equal(this.tracing, other.tracing)
                && Objects.equal(this.readTimeout, other.readTimeout)
                && Objects.equal(this.replaceNullValue, other.replaceNullValue)
                && Objects.equal(this.sqlParser, other.sqlParser);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(this.consistencyLevel)
                .addValue(this.execAsync)
                .addValue(this.fetchSize)
                .addValue(this.noLimit)
                .addValue(this.tracing)
                .addValue(this.readTimeout)
                .addValue(this.replaceNullValue)
                .addValue(this.sqlParser)
                .toString();
    }
}
