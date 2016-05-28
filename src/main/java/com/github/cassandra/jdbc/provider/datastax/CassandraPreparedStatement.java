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

import com.datastax.driver.core.*;
import com.github.cassandra.jdbc.CassandraColumnDefinition;
import com.github.cassandra.jdbc.CassandraCqlStatement;
import com.github.cassandra.jdbc.CassandraErrors;
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.pmw.tinylog.Logger;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.github.cassandra.jdbc.CassandraDataTypeMappings.*;

/**
 * This is a prepared statement implementation built on top of DataStax Java
 * driver.
 *
 * @author Zhichun Wu
 */
public class CassandraPreparedStatement extends CassandraStatement {
    protected final Cache<String, PreparedStatement> preparedStmtCache;

    protected CassandraPreparedStatement(CassandraConnection conn,
                                         DataStaxSessionWrapper session,
                                         String sql) throws SQLException {
        super(conn, session, sql);

        preparedStmtCache = CacheBuilder.newBuilder().maximumSize(5).build();

        // FIXME convert given string(sql or cql) to CassandraCqlStatement and put in a cache for further usage
        updateParameterMetaData(this.cql, true);
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

    protected void updateParameterMetaData(String cql, boolean force) throws SQLException {
        if (force || !Objects.equal(this.cql, cql)) {
            this.cql = cql;
            PreparedStatement preparedStmt = getInnerPreparedStatement(cql);
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

        if (INET.equals(typeName) && paramValue instanceof String) {
            try {
                parameters.put(paramIndex, InetAddress.getByName(String.valueOf(paramValue)));
            } catch (UnknownHostException e) {
                throw new SQLException(e);
            }
        } else if (BLOB.equals(typeName) && paramValue instanceof byte[]) {
            parameters.put(paramIndex, ByteBuffer.wrap((byte[]) paramValue));
        } else if (TEXT.equals(typeName)) {
            parameters.put(paramIndex, String.valueOf(paramValue));
        } else if (UUID.equals(typeName)) {
            parameters.put(paramIndex, java.util.UUID.fromString(String.valueOf(paramValue)));
        } else if (INT.equals(typeName)) {
            parameters.put(paramIndex, paramValue instanceof Number
                    ? ((Number) paramValue).intValue() : Integer.valueOf(String.valueOf(paramValue)));
        } else if (BIGINT.equals(typeName)) {
            parameters.put(paramIndex, paramValue instanceof Number
                    ? ((Number) paramValue).longValue() : Long.valueOf(String.valueOf(paramValue)));
        } else if (FLOAT.equals(typeName)) {
            parameters.put(paramIndex, paramValue instanceof Number
                    ? ((Number) paramValue).floatValue() : Float.valueOf(String.valueOf(paramValue)));
        } else if (DOUBLE.equals(typeName)) {
            parameters.put(paramIndex, paramValue instanceof Number
                    ? ((Number) paramValue).doubleValue() : Double.valueOf(String.valueOf(paramValue)));
        } else if (DECIMAL.equals(typeName)) {
            parameters.put(paramIndex, paramValue instanceof BigDecimal
                    ? (BigDecimal) paramValue : new BigDecimal(String.valueOf(paramValue)));
        } else {
            super.setParameter(paramIndex, paramValue);
        }
    }

    protected com.datastax.driver.core.ResultSet executePreparedCql(final String cql, Object... params) throws SQLException {
        Logger.debug(new StringBuilder(
                "Trying to execute the following CQL:\n").append(cql)
                .toString());

        boolean queryTrace = false;
        updateParameterMetaData(cql, false);

        PreparedStatement preparedStmt = getInnerPreparedStatement(cql);
        if (getConnection() instanceof CassandraConnection) {
            CassandraConnection cc = (CassandraConnection) getConnection();

            if (cc.getConfiguration().isQueryTrace()) {
                preparedStmt.enableTracing();
            }
        }

        com.datastax.driver.core.ResultSet rs = session.execute(preparedStmt.bind(params));

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

    @Override
    public int[] executeBatch() throws SQLException {
        BatchStatement batchStmt = new BatchStatement(BatchStatement.Type.UNLOGGED);


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
            results[i] = 0;
        }

        return results;
    }

    public boolean execute() throws SQLException {
        return this.execute(this.cql);
    }

    public ResultSet executeQuery() throws SQLException {
        return this.executeQuery(this.cql);
    }

    public int executeUpdate() throws SQLException {
        return this.executeUpdate(this.cql);
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

        com.datastax.driver.core.ResultSet rs = executePreparedCql(getConnection().nativeSQL(sql), params);
        replaceCurrentResultSet(rs);

        return true;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return execute(sql) ? currentResultSet : null;
    }

    public int executeUpdate(String sql) throws SQLException {
        // FIXME
        return execute(sql) ? 1 : 0;
    }
}
