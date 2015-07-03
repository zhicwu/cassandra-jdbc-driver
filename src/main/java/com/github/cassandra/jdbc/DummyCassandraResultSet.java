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

import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;

import java.sql.SQLException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy result set that only supports String. It's mainly be used to return
 * meta data.
 *
 * @author Zhichun Wu
 */
public class DummyCassandraResultSet extends BaseCassandraResultSet {
	private static final Logger logger = LoggerFactory
			.getLogger(DummyCassandraResultSet.class);

	private Object[] _currentRow;
	private final Object[][] _data;

	public DummyCassandraResultSet() {
		this(new String[0][], null);
	}

	public DummyCassandraResultSet(String[][] columns, Object[][] data) {
		super(null);

		if (logger.isTraceEnabled()) {
			logger.trace(new StringBuilder("Consutructing dummary result set [")
					.append(this.hashCode()).append("]...").toString());
		}

		if (columns != null && columns.length > 0 && columns[0].length > 1) {
			for (String[] column : columns) {
				if (logger.isTraceEnabled()) {
					logger.trace(new StringBuffer("* Column: {name=")
							.append(column[0]).append(", cqlType=")
							.append(column[1]).append("}").toString());
				}
				metadata.addColumnDefinition(new CassandraColumnDefinition(
						EMPTY_STRING, EMPTY_STRING, column[0], column[1], false));
			}
		}

		if (data != null) {
			_data = data;
		} else {
			_data = new String[0][];
		}

		if (logger.isTraceEnabled()) {
			for (Object[] objs : _data) {
				logger.trace(new StringBuffer("* Row: {")
						.append(Arrays.toString(objs)).append("}").toString());
			}
		}

		if (logger.isTraceEnabled()) {
			logger.trace(new StringBuilder("Dummary result set [")
					.append(this.hashCode()).append("] is ready for use")
					.toString());
		}
	}

	public int getColumnCount() {
		return _data != null && _data.length > 0 && _data[0] != null ? _data[0].length
				: 0;
	}

	public int getRowCount() {
		return _data == null || _data.length == 0 ? 0 : _data.length;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T> T getValue(int columnIndex, Class<T> clazz)
			throws SQLException {
		if (logger.isTraceEnabled()) {
			logger.trace(new StringBuilder(
					"Trying to get value with inputs: line=").append(getRow())
					.append(", column=").append(columnIndex).append(", type=")
					.append(clazz).toString());
		}

		Object obj = _currentRow[columnIndex - 1];
		T result = null;

		if (obj != null) {
			wasNull = false;

			if (logger.isTraceEnabled()) {
				logger.trace(new StringBuilder("Got raw value [").append(obj)
						.append("] from line #").append(getRow())
						.append(" of ").append(_data.length).toString());
			}

			if (String.class == clazz) {
				result = (T) String.valueOf(obj);
			} else if (Object.class == clazz) {
				result = (T) obj;
			} else {
				result = clazz.cast(obj);
			}
		} else {
			wasNull = true;
		}

		if (logger.isTraceEnabled()) {
			logger.trace(new StringBuilder("Return value: raw=").append(obj)
					.append(", converted=").append(result).toString());
		}
		return result;
	}

	@Override
	protected boolean hasMore() {
		return getCurrentRowIndex() < _data.length;
	}

	@Override
	protected <T> void setValue(int columnIndex, T value) throws SQLException {
		throw CassandraErrors.notSupportedException();
	}

	@Override
	protected SQLException tryClose() {
		return null;
	}

	@Override
	protected boolean tryIterate() throws SQLException {
		int row = getCurrentRowIndex();
		boolean success = row < _data.length;
		if (success) {
			_currentRow = _data[row];
		}

		return success;
	}

	@Override
	protected boolean tryMoveTo(int rows, boolean relativeIndex)
			throws SQLException {
		throw CassandraErrors.notSupportedException();
	}

	@Override
	protected Object unwrap() {
		return this;
	}
}
