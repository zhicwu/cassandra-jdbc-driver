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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.github.cassandra.jdbc.CassandraColumnDefinition;
import com.github.cassandra.jdbc.CassandraErrors;
import org.pmw.tinylog.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.github.cassandra.jdbc.CassandraDataTypeMappings.BLOB;
import static com.github.cassandra.jdbc.CassandraDataTypeMappings.INET;

/**
 * This is a prepared statement implementation built on top of DataStax Java
 * driver.
 *
 * @author Zhichun Wu
 */
public class CassandraPreparedStatement extends CassandraStatement {
    private String _cql;
    private PreparedStatement _ps;

    protected CassandraPreparedStatement(CassandraConnection conn,
                                         DataStaxSessionWrapper session,
                                         String sql) throws SQLException {
        super(conn, session);

        updateParameterMetaData(sql, false);
    }

    protected void updateParameterMetaData(String sql, boolean isCql) throws SQLException {
        String cql = isCql ? sql : getConnection().nativeSQL(sql);

        if (_cql != cql) {
            _cql = cql;
            _ps = session.prepare(_cql);
            parameterMetaData.clear();
            for (ColumnDefinitions.Definition def : _ps.getVariables().asList()) {
                parameterMetaData.addParameterDefinition(new CassandraColumnDefinition(
                        def.getKeyspace(), def.getTable(), def.getName(), def.getName(), def.getType().toString(), false, false));
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
        } else {
            super.setParameter(paramIndex, paramValue);
        }
    }

    protected com.datastax.driver.core.ResultSet executePreparedCql(String cql, Object... params) throws SQLException {
        Logger.debug(new StringBuilder(
                "Trying to execute the following CQL:\n").append(cql)
                .toString());

        boolean queryTrace = false;
        updateParameterMetaData(cql, true);

        if (getConnection() instanceof CassandraConnection) {
            CassandraConnection cc = (CassandraConnection) getConnection();

            if (cc.getConfiguration().isQueryTrace()) {
                _ps.enableTracing();
            }
        }

        com.datastax.driver.core.ResultSet rs = session.execute(_ps.bind(params));

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

    public boolean execute() throws SQLException {
        return this.execute(_cql);
    }

    public ResultSet executeQuery() throws SQLException {
        return this.executeQuery(_cql);
    }

    public int executeUpdate() throws SQLException {
        return this.executeUpdate(_cql);
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
