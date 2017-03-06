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
package com.github.cassandra.jdbc.provider.datastax;

import com.datastax.driver.core.*;
import com.github.cassandra.jdbc.*;
import com.google.common.base.Strings;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;

/**
 * This is a statement implementation built on top of DataStax Java driver.
 *
 * @author Zhichun Wu
 */
public class CassandraStatement extends BaseCassandraPreparedStatement {
    private static final Level LOG_LEVEL = Logger.getLevel(CassandraStatement.class);

    protected CassandraResultSet currentResultSet;
    protected DataStaxSessionWrapper session;

    protected CassandraStatement(CassandraConnection conn,
                                 DataStaxSessionWrapper session) {
        this(conn, session, EMPTY_STRING);
    }

    protected CassandraStatement(CassandraConnection conn,
                                 DataStaxSessionWrapper session,
                                 String cql) {
        super(conn, cql);
        this.session = session;
    }

    @Override
    protected CassandraDataTypeMappings getDataTypeMappings() {
        return DataStaxDataTypes.mappings;
    }

    @Override
    protected CassandraDataTypeConverters getDataTypeConverters() {
        return DataStaxDataTypes.converters;
    }

    protected void configureStatement(Statement stmt, CassandraCqlStmtConfiguration config) throws SQLException {
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

    protected void postStatementExecution(CassandraCqlStatement parsedStmt, ResultSet rs) {
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

    protected ResultSet executeCql(String cql) throws SQLException {
        Logger.debug("Trying to execute the following CQL:\n{}", cql);

        CassandraCqlStatement parsedStmt = CassandraCqlParser.parse(getConfiguration(), cql);
        CassandraCqlStmtConfiguration stmtConf = parsedStmt.getConfiguration();

        Logger.debug("Statement Configuration:\n{}", stmtConf);

        SimpleStatement ss = new SimpleStatement(parsedStmt.getCql());

        configureStatement(ss, stmtConf);

        ResultSet rs = null;
        if (stmtConf.noWait()) {
            session.executeAsync(ss);
        } else {
            rs = session.execute(ss);
        }

        postStatementExecution(parsedStmt, rs);

        return rs;
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

    protected void replaceCurrentResultSet(CassandraCqlStatement parsedStmt, ResultSet resultSet) {
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

    @Override
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

    @Override
    protected void validateState() throws SQLException {
        super.validateState();

        if (session == null || session.isClosed()) {
            session = null;
            throw CassandraErrors.statementClosedException();
        }
    }

    public int[] executeBatch() throws SQLException {
        CassandraEnums.Batch mode = getConfiguration().getBatch();
        BatchStatement batchStmt = new BatchStatement(
                mode == CassandraEnums.Batch.LOGGED ? BatchStatement.Type.LOGGED : BatchStatement.Type.UNLOGGED);

        for (CassandraCqlStatement stmt : batch) {
            batchStmt.add(new SimpleStatement(stmt.getCql()));
        }

        session.execute(batchStmt);

        int[] results = new int[batch.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = SUCCESS_NO_INFO;
        }

        return results;
    }

    public boolean execute(String sql) throws SQLException {
        validateState();

        executeCql(sql);

        return cqlStmt.getConfiguration().getStatementType().isQuery();
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public java.sql.ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw CassandraErrors.invalidQueryException(sql);
        }

        return currentResultSet;
    }

    public int executeUpdate(String sql) throws SQLException {
        validateState();

        executeCql(sql);

        return cqlStmt.getConfiguration().getStatementType().isUpdate() ? 1 : 0;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public java.sql.ResultSet getResultSet() throws SQLException {
        return cqlStmt.getConfiguration().getStatementType().isQuery() ? currentResultSet : null;
    }

    public int getUpdateCount() throws SQLException {
        CassandraStatementType stmtType = cqlStmt.getConfiguration().getStatementType();

        return stmtType.isQuery() ? -1 : (stmtType.isUpdate() ? 1 : 0);
    }

    public java.sql.ResultSet executeQuery() throws SQLException {
        // method inherited from BaseCassandraPreparedStatement
        throw CassandraErrors.notSupportedException();
    }

    public int executeUpdate() throws SQLException {
        // method inherited from BaseCassandraPreparedStatement
        throw CassandraErrors.notSupportedException();
    }

    public boolean execute() throws SQLException {
        // method inherited from BaseCassandraPreparedStatement
        throw CassandraErrors.notSupportedException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw CassandraErrors.notSupportedException();
    }
}
