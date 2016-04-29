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
package com.github.cassandra.jdbc.provider.datastax;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.github.cassandra.jdbc.BaseCassandraPreparedStatement;
import com.github.cassandra.jdbc.CassandraErrors;
import org.pmw.tinylog.Logger;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * This is a statement implementation built on top of DataStax Java driver.
 *
 * @author Zhichun Wu
 */
public class CassandraStatement extends BaseCassandraPreparedStatement {
    protected CassandraResultSet currentResultSet;
    protected DataStaxSessionWrapper session;

    protected CassandraStatement(CassandraConnection conn,
                                 DataStaxSessionWrapper session) {
        super(conn);
        this.session = session;
    }

    protected ResultSet executeCql(String cql) throws SQLException {
        Logger.debug(new StringBuilder(
                "Trying to execute the following CQL:\n").append(cql)
                .toString());

        Connection c = this.getConnection();
        if (c != null) {
            cql = c.nativeSQL(cql);
        }

        boolean queryTrace = false;
        SimpleStatement ss = new SimpleStatement(cql);
        ss.setFetchSize(this.getFetchSize());
        if (c instanceof CassandraConnection) {
            CassandraConnection cc = (CassandraConnection) c;

            if (cc.getConfiguration().isQueryTrace()) {
                queryTrace = true;
                ss.enableTracing();
            }
        }

        ResultSet rs = session.execute(ss);

        List<ExecutionInfo> list = rs.getAllExecutionInfo();
        int size = list == null ? 0 : list.size();

        if (size > 0) {
            int index = 1;

            for (ExecutionInfo info : rs.getAllExecutionInfo()) {
                Logger.debug(getExecutionInfoAsString(info, index, size));

                QueryTrace q = info.getQueryTrace();
                if (queryTrace && q != null) {
                    Logger.debug(getQueryTraceAsString(q, index, size));
                }

                index++;
            }

            Logger.debug(new StringBuilder(
                    "Executed successfully with results: ").append(
                    !rs.isExhausted()).toString());
        }

        return rs;
    }

    protected String getExecutionInfoAsString(ExecutionInfo info, int index,
                                              int size) {
        StringBuilder builder = new StringBuilder();

        if (info != null) {
            builder.append("Execution Info ").append(index).append(" of ")
                    .append(size).append(":\n* schema aggrement: ")
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

            for (Event e : q.getEvents()) {
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

    protected void replaceCurrentResultSet(ResultSet resultSet) {
        if (currentResultSet != null) {
            try {
                if (!currentResultSet.isClosed()) {
                    currentResultSet.close();
                }
            } catch (Throwable t) {
                Logger.warn(t, "Not able to close the old result set: ", currentResultSet);
            }
        }

        currentResultSet = new CassandraResultSet(this, resultSet);
    }

    @Override
    protected SQLException tryClose() {
        SQLException e = null;

        try {
            if (currentResultSet != null && !currentResultSet.isClosed()) {
                currentResultSet.close();
            }
        } catch (Throwable t) {
            Logger.warn(t, "Not able to close the current result set: ", currentResultSet);
            e = new SQLException(t);
        } finally {
            currentResultSet = null;
        }

        /*
        try {
            if (session != null && !session.isClosed()) {
                session.close();
                session = null;
            }
        } catch (Throwable t) {
            Logger.warn("Not able to close this statement: " + this, t);

            e = new SQLException(t);
        } finally {
            session = null;
        }
        */

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

    public boolean execute(String sql) throws SQLException {
        validateState();

        ResultSet rs = executeCql(sql);
        replaceCurrentResultSet(rs);

        return true;
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
        validateState();

        ResultSet resultSet = executeCql(sql);
        replaceCurrentResultSet(resultSet);
        return currentResultSet;
    }

    public int executeUpdate(String sql) throws SQLException {
        validateState();

        ResultSet resultSet = executeCql(sql);
        replaceCurrentResultSet(resultSet);
        return 0;
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
        return currentResultSet;
    }

    public int getUpdateCount() throws SQLException {
        return -1;
    }

    public java.sql.ResultSet executeQuery() throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public int executeUpdate() throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public boolean execute() throws SQLException {
        throw CassandraErrors.notSupportedException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw CassandraErrors.notSupportedException();
    }
}
