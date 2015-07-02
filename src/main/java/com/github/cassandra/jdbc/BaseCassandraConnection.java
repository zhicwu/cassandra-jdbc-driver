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

import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_CONNECT_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_SQL_FRIENDLY;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_USERNAME;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONNECTION_URL;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONNECT_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_QUIET;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_SQL_FRIENDLY;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;
import static com.github.cassandra.jdbc.CassandraUtils.TABLE_TYPE_COLUMNS;
import static com.github.cassandra.jdbc.CassandraUtils.TABLE_TYPE_DATA;
import static com.github.cassandra.jdbc.CassandraUtils.TYPE_COLUMNS;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * This is the base class for implementing Cassandra connection.
 *
 * @author Zhichun Wu
 */
public abstract class BaseCassandraConnection extends BaseJdbcObject implements
		Connection {
	protected final CassandraDatabaseMetaData metaData;

	private final Properties _clientInfo = new Properties();
	private final Map<String, Class<?>> _typeMap = new HashMap<String, Class<?>>();

	private boolean _autoCommit;
	private boolean _connReadOnly;
	private boolean _sqlFriendly;
	private int _timeout;
	private int _txIsolationLevel;

	public BaseCassandraConnection(Properties props) {
		super(Boolean.valueOf(CassandraUtils.getPropertyValue(props, KEY_QUIET,
				Boolean.TRUE.toString())));

		metaData = new CassandraDatabaseMetaData(this);
		metaData.setProperty(KEY_CONNECTION_URL,
				CassandraUtils.buildSimplifiedConnectionUrl(props));
		metaData.setProperty(KEY_USERNAME, CassandraUtils.getPropertyValue(
				props, KEY_USERNAME, DEFAULT_USERNAME));

		_autoCommit = true;
		_connReadOnly = false;
		_sqlFriendly = Boolean.valueOf(CassandraUtils.getPropertyValue(props,
				KEY_SQL_FRIENDLY, Boolean.toString(DEFAULT_SQL_FRIENDLY)));
		_timeout = Integer.parseInt(CassandraUtils.getPropertyValue(props,
				KEY_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT));
		_txIsolationLevel = TRANSACTION_NONE;
	}

	protected ResultSet getObjectMetaData(CassandraObjectType objectType,
			Properties queryPatterns, Object... additionalHints) {
		ResultSet rs = new DummyCassandraResultSet();

		switch (objectType) {
		case TABLE_TYPE:
			rs = new DummyCassandraResultSet(TABLE_TYPE_COLUMNS,
					TABLE_TYPE_DATA);
			break;
		case TYPE:
			rs = new DummyCassandraResultSet(TYPE_COLUMNS,
					CassandraDataTypeMappings.TYPE_META_DATA);
			break;

		default:
			break;
		}

		return rs;
	}

	protected abstract <T> T createObject(Class<T> clazz) throws SQLException;

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		validateState();

		// FIXME incomplete
		Array result = createObject(Array.class);

		return result;
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		validateState();

		// FIXME incomplete
		Struct result = createObject(Struct.class);

		return result;
	}

	public Clob createClob() throws SQLException {
		validateState();

		return createObject(Clob.class);
	}

	public Blob createBlob() throws SQLException {
		validateState();

		return createObject(Blob.class);
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

	public String nativeSQL(String sql) throws SQLException {
		validateState();

		return CassandraUtils.normalizeSql(sql, _sqlFriendly, quiet);
	}

	public boolean getAutoCommit() throws SQLException {
		validateState();

		return _autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		validateState();

		if (!quiet && !autoCommit) {
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

	public void rollback() throws SQLException {
		validateState();

		// better to be quiet and do nothing as we always commit in Cassandra
		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		validateState();

		if (!quiet && metaData == null) {
			throw CassandraErrors.databaseMetaDataNotAvailableException();
		}

		return metaData;
	}

	public boolean isReadOnly() throws SQLException {
		validateState();

		return _connReadOnly;
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		validateState();

		_connReadOnly = readOnly;
	}

	public String getSchema() throws SQLException {
		validateState();

		return null;
	}

	public void setSchema(String schema) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}
	}

	public int getTransactionIsolation() throws SQLException {
		validateState();

		return _txIsolationLevel;
	}

	public void setTransactionIsolation(int level) throws SQLException {
		validateState();

		// ignore isolation level and always use TRANSACTION_READ_COMMITTED
		if (!quiet && level != TRANSACTION_NONE) {
			throw CassandraErrors.notSupportedException();
		}
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		validateState();

		Map<String, Class<?>> map = new HashMap<String, Class<?>>();
		map.putAll(_typeMap);
		return map;
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		validateState();

		_typeMap.clear();

		if (map != null) {
			_typeMap.putAll(map);
		}
	}

	public int getHoldability() throws SQLException {
		validateState();

		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	public void setHoldability(int holdability) throws SQLException {
		validateState();

		if (!quiet && holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
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

	public void rollback(Savepoint savepoint) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}
	}

	public boolean isValid(int timeout) throws SQLException {
		validateState();

		// FIXME incomplete
		return !closed;
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		// FIXME incomplete
		_clientInfo.setProperty(name, value);
	}

	public String getClientInfo(String name) throws SQLException {
		validateState();

		return _clientInfo.getProperty(name);
	}

	public Properties getClientInfo() throws SQLException {
		validateState();

		Properties props = new Properties();
		props.putAll(_clientInfo);
		return props;
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		// FIXME incomplete
		_clientInfo.clear();

		if (properties != null) {
			_clientInfo.putAll(properties);
		}
	}

	public void abort(Executor executor) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		validateState();

		// FIXME incomplete
		if (quiet) {
			_timeout = milliseconds;
		} else {
			throw CassandraErrors.notSupportedException();
		}
	}

	public int getNetworkTimeout() throws SQLException {
		validateState();

		return _timeout;
	}
}
