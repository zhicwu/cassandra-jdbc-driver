/**
 * Copyright (C) 2015-2016, Zhichun Wu
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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.github.cassandra.jdbc.provider.datastax.CassandraResultSet;
import com.github.cassandra.jdbc.provider.datastax.CassandraStatement;
import com.github.cassandra.jdbc.provider.datastax.DataStaxSessionWrapper;
import com.google.common.base.Strings;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;

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
public abstract class BaseCassandraStatement extends BaseJdbcObject implements
        Statement {
    private boolean _closeOnCompletion;
    private BaseCassandraConnection _connection;
    private String _cursorName;

    private static final Level LOG_LEVEL = Logger.getLevel(CassandraStatement.class);

    protected CassandraResultSet currentResultSet;

    protected DataStaxSessionWrapper session;


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

    protected CassandraCqlStatement cqlStmt;


    protected BaseCassandraStatement(BaseCassandraConnection conn) {
        super(conn == null || conn.quiet);

        _closeOnCompletion = false;
        _connection = conn;
        _cursorName = new StringBuilder().append(CURSOR_PREFIX)
                .append(conn.hashCode()).append('/').append(hashCode())
                .toString();

        fetchSize = conn.getConfiguration().getFetchSize();
    }

    protected void configureStatement(com.datastax.driver.core.Statement stmt, CassandraCqlStmtConfiguration config)
            throws SQLException {
        stmt.setConsistencyLevel(ConsistencyLevel.valueOf(config.getConsistencyLevel()));
        String scl = config.getSerialConsistencyLevel();
        if (!Strings.isNullOrEmpty(scl)) {
            stmt.setSerialConsistencyLevel(ConsistencyLevel.valueOf(scl));
        }
        stmt.setFetchSize(config.hasSetFetchSize() ? config.getFetchSize() : this.getFetchSize());

        if (config.tracingEnabled()) {
            stmt.enableTracing();
        }

        stmt.setReadTimeoutMillis(config.getReadTimeout());

        // TODO: for prepared statement, we'd better set routing key as hints for token-aware load-balancing policy
        // http://www.cyanicautomation.com/cassandra-routing-keys-datastax-c-driver/
    }

    protected String getExecutionInfoAsString(ExecutionInfo info, int index,
                                              int size) {
        StringBuilder builder = new StringBuilder();

        if (info != null) {
            builder.append("Execution Info ").append(index).append(" of ")
                    .append(size).append(":\n* schema agreement: ")
                    .append(info.isSchemaInAgreement())
                    .append("\n* achieved consistency level: ")
                    .append(info.getAchievedConsistencyLevel())
                    .append("\n* queried host: ").append(info.getQueriedHost())
                    .append("\n* tried hosts: ").append(info.getTriedHosts())
                    .append("\n* paging state: ").append(info.getPagingState());
        }

        return builder.toString();
    }

    protected void postStatementExecution(CassandraCqlStatement parsedStmt, com.datastax.driver.core.ResultSet rs) {
        if (LOG_LEVEL.compareTo(Level.DEBUG) >= 0 && rs != null) {
            List<ExecutionInfo> list = rs.getAllExecutionInfo();
            int size = list == null ? 0 : list.size();

            if (size > 0) {
                int index = 1;

                for (ExecutionInfo info : rs.getAllExecutionInfo()) {
                    Logger.debug(getExecutionInfoAsString(info, index, size));

                    QueryTrace q = info.getQueryTrace();
                    if (parsedStmt.getConfiguration().tracingEnabled() && q != null) {
                        Logger.debug(getQueryTraceAsString(q, index, size));
                    }

                    index++;
                }

                Logger.debug("Executed successfully with results: {}", !rs.isExhausted());
            }
        }

        replaceCurrentResultSet(parsedStmt, rs);
    }


    protected void replaceCurrentResultSet(CassandraCqlStatement parsedStmt, com.datastax.driver.core.ResultSet resultSet) {
        this.cqlStmt = parsedStmt;

        if (currentResultSet != null) {
            try {
                if (!currentResultSet.isClosed()) {
                    currentResultSet.close();
                }
            } catch (Throwable t) {
                Logger.warn(t, "Not able to close the old result set: {}", currentResultSet);
            }
        }

        currentResultSet = new CassandraResultSet(this, parsedStmt, resultSet);
    }

    protected String getQueryTraceAsString(QueryTrace q, int index, int size) {
        StringBuilder trace = new StringBuilder();

        if (q != null) {
            trace.append("Query Trace ").append(index).append(" of ")
                    .append(size).append(": \n[ id=").append(q.getTraceId())
                    .append(", coordinator=").append(q.getCoordinator())
                    .append(", requestType=").append(q.getRequestType())
                    .append(", startAt=")
                    .append(new java.sql.Timestamp(q.getStartedAt()))
                    .append(", duration=").append(q.getDurationMicros())
                    .append("(microseconds), params=")
                    .append(q.getParameters()).append(" ]");

            for (QueryTrace.Event e : q.getEvents()) {
                trace.append("\n* event=[").append(e.getDescription())
                        .append("], location=[").append(e.getThreadName())
                        .append("@").append(e.getSource()).append("], time=[")
                        .append(new java.sql.Timestamp(e.getTimestamp()))
                        .append("], elapsed=[")
                        .append(e.getSourceElapsedMicros())
                        .append("(microseconds)]");
            }
        }

        return trace.toString();
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

    protected SQLException tryClose() {
        SQLException e = null;

        try {
            if (currentResultSet != null && !currentResultSet.isClosed()) {
                currentResultSet.close();
            }
        } catch (Throwable t) {
            Logger.warn(t, "Not able to close the current result set: {}", currentResultSet);
            e = new SQLException(t);
        } finally {
            currentResultSet = null;
        }

        return e;
    }

    @Override
    protected Object unwrap() {
        return session;
    }


}
