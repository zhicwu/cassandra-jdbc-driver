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
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.pmw.tinylog.Logger;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * This is a prepared statement implementation built on top of DataStax Java
 * driver.
 *
 * @author Zhichun Wu
 */
public class CassandraPreparedStatement extends CassandraStatement
        implements java.sql.PreparedStatement {
    protected final Cache<String, PreparedStatement> preparedStmtCache;

    protected CassandraPreparedStatement(CassandraConnection conn,
                                         DataStaxSessionWrapper session,
                                         String sql) throws SQLException {
        super(conn, session, sql);

        preparedStmtCache = CacheBuilder.newBuilder().maximumSize(50).build();

        // FIXME convert given string(sql or cql) to CassandraCqlStatement and put in a cache for further usage
        if (conn != null) { // connection might be null in unit tests
            updateParameterMetaData(this.cqlStmt, true);
        }
    }


    protected PreparedStatement getInnerPreparedStatement(final String cql) throws SQLException {
        PreparedStatement preparedStmt = null;

        try {
            preparedStmt = preparedStmtCache.get(cql, new Callable<PreparedStatement>() {
                public PreparedStatement call() throws Exception {
                    return session.prepare(cql);
                }
            });
        } catch (ExecutionException e) {
            throw new SQLException(e);
        }

        return preparedStmt;
    }

    protected void updateParameterMetaData(CassandraCqlStatement cql, boolean force) throws SQLException {
        if (force || !Objects.equal(this.cqlStmt.getCql(), cql.getCql())) {
            this.cqlStmt = cql;
            PreparedStatement preparedStmt = getInnerPreparedStatement(cql.getCql());
            parameterMetaData.clear();
            for (ColumnDefinitions.Definition def : preparedStmt.getVariables().asList()) {
                parameterMetaData.addParameterDefinition(new CassandraColumnDefinition(
                        def.getKeyspace(), def.getTable(), def.getName(), def.getName(),
                        def.getType().toString(), false, false));
            }
        }
    }

    @Override
    protected void setParameter(int paramIndex, Object paramValue) throws SQLException {
        String typeName = parameterMetaData.getParameterTypeName(paramIndex);
        Class javaClass = getDataTypeMappings().javaTypeFor(typeName);

        boolean replaceNullValue = this.cqlStmt.getConfiguration().replaceNullValue();

        if (javaClass != null) {
            paramValue = getDataTypeConverters().convert(paramValue, javaClass, replaceNullValue);
            // time is mapped by the driver to a primitive long, representing the number of nanoseconds since midnight
            if (CassandraDataType.TIME.getTypeName().equals(typeName) && paramValue instanceof Time) {
                Time time = (Time) paramValue;
                paramValue = new LocalTime(time).getMillisOfDay() * 1000000L;
            } else if (CassandraDataType.DATE.getTypeName().equals(typeName)
                    && paramValue instanceof Date) {
                LocalDate localDate = LocalDate.fromDateFields((Date) paramValue);
                paramValue = com.datastax.driver.core.LocalDate.fromYearMonthDay(
                        localDate.getYear(), localDate.getMonthOfYear(), localDate.getDayOfMonth());
            } else if (CassandraDataType.BLOB.getTypeName().equals(typeName)
                    && paramValue instanceof byte[]) {
                paramValue = ByteBuffer.wrap((byte[]) paramValue);
            }

            parameters.put(paramIndex, paramValue);
        } else {
            super.setParameter(paramIndex, paramValue);
        }
    }

    protected com.datastax.driver.core.ResultSet executePreparedCql(final String cql, Object... params)
            throws SQLException {
        Logger.debug("Trying to execute the following CQL:\n{}", cql);

        CassandraCqlStatement parsedStmt = cqlStmt.getCql().equals(cql)
                ? cqlStmt : CassandraCqlParser.parse(getConfiguration(), cql);
        CassandraCqlStmtConfiguration stmtConf = parsedStmt.getConfiguration();

        Logger.debug("Statement Configuration:\n{}", stmtConf);

        updateParameterMetaData(CassandraCqlParser.parse(getConfiguration(), cql), false);

        PreparedStatement preparedStmt = getInnerPreparedStatement(cql);
        BoundStatement boundStatement = preparedStmt.bind(params);

        configureStatement(boundStatement, stmtConf);

        com.datastax.driver.core.ResultSet rs = null;
        if (stmtConf.noWait()) {
            session.executeAsync(boundStatement);
        } else {
            rs = session.execute(boundStatement);
        }

        postStatementExecution(parsedStmt, rs);

        return rs;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        CassandraEnums.Batch mode = getConfiguration().getBatch();
        BatchStatement batchStmt = new BatchStatement(
                mode == CassandraEnums.Batch.LOGGED ? BatchStatement.Type.LOGGED : BatchStatement.Type.UNLOGGED);


        for (CassandraCqlStatement stmt : batch) {
            String cql = stmt.getCql();
            if (stmt.hasParameter()) {
                batchStmt.add(getInnerPreparedStatement(cql).bind(stmt.getParameters()));
            } else {
                batchStmt.add(new SimpleStatement(cql));
            }
        }

        session.execute(batchStmt);

        int[] results = new int[batch.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = SUCCESS_NO_INFO;
        }

        return results;
    }

    public boolean execute() throws SQLException {
        return this.execute(this.cqlStmt.getCql());
    }

    public java.sql.ResultSet executeQuery() throws SQLException {
        return this.executeQuery(this.cqlStmt.getCql());
    }

    public int executeUpdate() throws SQLException {
        return this.executeUpdate(this.cqlStmt.getCql());
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        if (currentResultSet == null) {
            throw CassandraErrors.databaseMetaDataNotAvailableException();
        }

        return currentResultSet.getMetaData();
    }

    public boolean execute(String sql) throws SQLException {
        validateState();

        Object[] params = new Object[parameters.size()];
        int i = 0;
        for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
            params[i++] = entry.getValue();
        }

        executePreparedCql(sql, params);


        return cqlStmt.getConfiguration().getStatementType().isQuery();
    }

    public java.sql.ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw CassandraErrors.invalidQueryException(sql);
        }

        return currentResultSet;
    }

    public int executeUpdate(String sql) throws SQLException {
        execute(sql);

        return cqlStmt.getConfiguration().getStatementType().isUpdate() ? 1 : 0;
    }
}
