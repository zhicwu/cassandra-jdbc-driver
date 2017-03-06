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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.github.cassandra.jdbc.CassandraUtils.CURSOR_PREFIX;

/**
 * This is the base class for all Cassandra statements.
 *
 * @author Zhichun Wu
 */
public abstract class BaseCassandraStatement extends BaseJdbcObject implements Statement {
    private boolean _closeOnCompletion;
    private BaseCassandraConnection _connection;
    private String _cursorName;

    protected final List<CassandraCqlStatement> batch = new ArrayList<CassandraCqlStatement>();
    protected final int concurrency = ResultSet.CONCUR_READ_ONLY;
    protected boolean escapeProcessing = true;
    protected int fetchDirection = ResultSet.FETCH_FORWARD;
    protected int fetchSize = 100;
    protected final int hodability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    protected int maxFieldSize = 0; // unlimited
    protected int maxRows = 0; // unlimited
    protected boolean poolable = false;
    protected int queryTimeout = 0; // unlimited
    protected final int resultType = ResultSet.TYPE_FORWARD_ONLY;

    protected BaseCassandraStatement(BaseCassandraConnection conn) {
        super(conn == null || conn.quiet);

        _closeOnCompletion = false;
        _connection = conn;
        _cursorName = new StringBuilder().append(CURSOR_PREFIX)
                .append(Objects.hashCode(conn)).append('/').append(hashCode())
                .toString();

        if (conn != null) {
            fetchSize = conn.getConfiguration().getFetchSize();
        }
    }

    protected CassandraConfiguration getConfiguration() {
        return _connection.getConfiguration();
    }

    protected abstract CassandraDataTypeMappings getDataTypeMappings();

    protected abstract CassandraDataTypeConverters getDataTypeConverters();

    /**
     * Gets cursor name set in statement.
     *
     * @return cursor name
     */
    protected String getCursorName() {
        return _cursorName;
    }

    public void addBatch(String sql) throws SQLException {
        validateState();

        batch.add(CassandraCqlParser.parse(getConfiguration(), sql));
    }

    public void cancel() throws SQLException {
        validateState();
    }

    public void clearBatch() throws SQLException {
        validateState();

        batch.clear();
    }

    public void close() throws SQLException {
        _connection = null;
        super.close();
    }

    public void closeOnCompletion() throws SQLException {
        validateState();

        this._closeOnCompletion = true;
    }

    public Connection getConnection() throws SQLException {
        validateState();

        return _connection;
    }

    public int getFetchDirection() throws SQLException {
        validateState();

        return ResultSet.FETCH_FORWARD;
    }

    public int getFetchSize() throws SQLException {
        validateState();

        return getConfiguration().getFetchSize();
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        validateState();

        return null;
    }

    public int getMaxFieldSize() throws SQLException {
        validateState();

        return maxFieldSize;
    }

    public int getMaxRows() throws SQLException {
        validateState();

        return maxRows;
    }

    public boolean getMoreResults() throws SQLException {
        validateState();

        return false;
    }

    public boolean getMoreResults(int current) throws SQLException {
        validateState();

        return false;
    }

    public int getQueryTimeout() throws SQLException {
        validateState();

        return queryTimeout;
    }

    public int getResultSetConcurrency() throws SQLException {
        validateState();

        return concurrency;
    }

    public int getResultSetHoldability() throws SQLException {
        validateState();

        return hodability;
    }

    public int getResultSetType() throws SQLException {
        validateState();

        return resultType;
    }

    public boolean isCloseOnCompletion() throws SQLException {
        validateState();

        return _closeOnCompletion;
    }

    public boolean isPoolable() throws SQLException {
        validateState();

        return this.poolable;
    }

    public void setCursorName(String name) throws SQLException {
        validateState();

        this._cursorName = name;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        validateState();

        escapeProcessing = enable;
    }

    public void setFetchDirection(int direction) throws SQLException {
        validateState();

        if (direction != ResultSet.FETCH_FORWARD) {
            if (!quiet) {
                throw CassandraErrors.notSupportedException();
            }
            // this.fetchDirection = direction;
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        validateState();

        fetchSize = rows;
    }

    public void setMaxFieldSize(int max) throws SQLException {
        validateState();

        maxFieldSize = max;
    }

    public void setMaxRows(int max) throws SQLException {
        validateState();

        maxRows = max;
    }

    public void setPoolable(boolean poolable) throws SQLException {
        validateState();

        this.poolable = poolable;
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        validateState();

        queryTimeout = seconds;
    }
}
