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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Wrapper;

/**
 * Base class for JDBC implementations of Connection, Statement and ResultSet.
 *
 * @author Zhichun Wu
 */
public abstract class BaseJdbcObject implements AutoCloseable, Wrapper {
	/**
	 * Quiet mode suggests if the driver should throw SQLException for
	 * unsupported operation. By default it is true but can be customized by
	 * passing "quiet=false" in connection properties.
	 */
	protected final boolean quiet;

	/**
	 * This indicates whether this has been closed.
	 */
	protected boolean closed;

	protected BaseJdbcObject(boolean quiet) {
		this.closed = false;
		this.quiet = quiet;
	}

	/**
	 * Validate instance state - mainly checking if this has been closed.
	 *
	 * @throws SQLException
	 *             when state is invalid(e.g. closed
	 *             connection/statement/resultset)
	 */
	protected void validateState() throws SQLException {
		if (closed) {
			throw CassandraErrors.resourceClosedException(this);
		}
	}

	/**
	 * This returns the underlying object actually doing the work.
	 *
	 * @return underlying object
	 */
	protected abstract Object unwrap();

	/**
	 * Idempotent close method.
	 *
	 * @return SQLException if any
	 */
	protected abstract SQLException tryClose();

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		Object innerObj = unwrap();

		return innerObj != null && innerObj.getClass().isAssignableFrom(iface);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return iface.cast(unwrap());
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public void clearWarnings() throws SQLException {
		// be quiet
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public void close() throws SQLException {
		closed = true;

		SQLException exception = tryClose();
		if (!quiet) {
			throw exception;
		}
	}
}
