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

import com.google.common.base.Strings;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static com.github.cassandra.jdbc.CassandraUtils.*;

/**
 * This is the base class for implementing Cassandra connection.
 *
 * @author Zhichun Wu
 */
public abstract class BaseCassandraConnection extends BaseJdbcObject implements
        Connection {
    private final Properties _clientInfo = new Properties();
    private int _txIsolationLevel;
    private final Map<String, Class<?>> _typeMap = new HashMap<String, Class<?>>();

    protected final CassandraConfiguration config;
    protected final CassandraDatabaseMetaData metaData;

    public BaseCassandraConnection(CassandraConfiguration driverConfig) {
        super(driverConfig.isQuiet());
        _txIsolationLevel = TRANSACTION_NONE;

        this.config = driverConfig;

        metaData = new CassandraDatabaseMetaData(this);
        metaData.setProperty(CassandraConfiguration.KEY_CONNECTION_URL, driverConfig.getConnectionUrl());
        metaData.setProperty(CassandraConfiguration.KEY_USERNAME, driverConfig.getUserName());
    }

    protected abstract <T> T createObject(Class<T> clazz) throws SQLException;

    protected ResultSet getObjectMetaData(CassandraObjectType objectType,
                                          Properties queryPatterns, Object... additionalHints)
            throws SQLException {
        ResultSet rs;

        switch (objectType) {
            case TABLE_TYPE:
                rs = new DummyCassandraResultSet(TABLE_TYPE_COLUMNS,
                        TABLE_TYPE_DATA);
                break;
            case TYPE:
                rs = new DummyCassandraResultSet(TYPE_COLUMNS,
                        CassandraDataTypeMappings.instance.getTypeMetaData());
                break;

            default:
                throw CassandraErrors.notSupportedException();
        }

        return rs;
    }

    public CassandraConfiguration getConfiguration() {
        return config;
    }

    public void abort(Executor executor) throws SQLException {
        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void commit() throws SQLException {
        validateState();

        // better to be quiet and do nothing as we always commit in Cassandra
        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        validateState();

        // FIXME incomplete
        Array result = createObject(Array.class);

        return result;
    }

    public Blob createBlob() throws SQLException {
        validateState();

        return createObject(Blob.class);
    }

    public Clob createClob() throws SQLException {
        validateState();

        return createObject(Clob.class);
    }

    public NClob createNClob() throws SQLException {
        validateState();

        return createObject(NClob.class);
    }

    public SQLXML createSQLXML() throws SQLException {
        validateState();

        return createObject(SQLXML.class);
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        validateState();

        // FIXME incomplete
        Struct result = createObject(Struct.class);

        return result;
    }

    public boolean getAutoCommit() throws SQLException {
        validateState();

        return config.isAutoCommit();
    }

    public Properties getClientInfo() throws SQLException {
        validateState();

        Properties props = new Properties();
        props.putAll(_clientInfo);
        return props;
    }

    public String getClientInfo(String name) throws SQLException {
        validateState();

        return _clientInfo.getProperty(name);
    }

    public int getHoldability() throws SQLException {
        validateState();

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        validateState();

        if (!quiet && metaData == null) {
            throw CassandraErrors.databaseMetaDataNotAvailableException();
        }

        return metaData;
    }

    public int getNetworkTimeout() throws SQLException {
        validateState();

        return config.getConnectionTimeout();
    }

    public String getCatalog() throws SQLException {
        validateState();

        return null;
    }

    public int getTransactionIsolation() throws SQLException {
        validateState();

        return _txIsolationLevel;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        validateState();

        Map<String, Class<?>> map = new HashMap<String, Class<?>>();
        map.putAll(_typeMap);
        return map;
    }

    public boolean isReadOnly() throws SQLException {
        validateState();

        return config.isReadOnly();
    }

    public boolean isValid(int timeout) throws SQLException {
        validateState();

        // FIXME incomplete
        return !closed;
    }

    public String nativeSQL(String sql) throws SQLException {
        validateState();

        if (config.isSqlFriendly()) {
            CassandraCqlStatement stmt = CassandraCqlParser.parse(config, sql);
            sql = stmt.getCql();
        }

        return Strings.nullToEmpty(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }

        return null;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        // FIXME incomplete
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        // FIXME incomplete
        return prepareStatement(sql);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void rollback() throws SQLException {
        validateState();

        // better to be quiet and do nothing as we always commit in Cassandra
        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        validateState();

        if (!quiet && !autoCommit) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        // FIXME incomplete
        _clientInfo.clear();

        if (properties != null) {
            _clientInfo.putAll(properties);
        }
    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        // FIXME incomplete
        _clientInfo.setProperty(name, value);
    }

    public void setHoldability(int holdability) throws SQLException {
        validateState();

        if (!quiet && holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        validateState();

        // FIXME incomplete
        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        validateState();

        // FIXME incomplete
        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public Savepoint setSavepoint() throws SQLException {
        return setSavepoint(null);
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }

        return null;
    }

    public void setCatalog(String catalog) throws SQLException {
        validateState();

        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        validateState();

        // ignore isolation level and always use TRANSACTION_READ_COMMITTED
        if (!quiet && level != TRANSACTION_NONE) {
            throw CassandraErrors.notSupportedException();
        }
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        validateState();

        _typeMap.clear();

        if (map != null) {
            _typeMap.putAll(map);
        }
    }
}
