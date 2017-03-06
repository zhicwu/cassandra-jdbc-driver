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

import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * Dummy result set that is read-only. It's mainly used to wrap meta data.
 *
 * @author Zhichun Wu
 */
public class DummyCassandraResultSet extends BaseCassandraResultSet {
    private static final Level LOG_LEVEL = Logger.getLevel(DummyCassandraResultSet.class);

    private Object[] currentRow;
    private final Object[][] data;

    /**
     * This creates an empty result set.
     */
    public DummyCassandraResultSet() {
        this(new String[0][], null);
    }

    /**
     * This creates a result set based on given data and column definitions.
     *
     * @param columns column definitions, name and its Cql type
     * @param data    rows
     */
    public DummyCassandraResultSet(String[][] columns, Object[][] data) {
        super(null, null);

        Logger.trace("Constructing dummy result set @{}...", hashCode());

        if (columns != null && columns.length > 0 && columns[0].length > 1) {
            for (String[] column : columns) {
                Logger.trace("* Column: {name={}, cqlType={}}", column[0], column[1]);

                metadata.addColumnDefinition(new CassandraColumnDefinition(
                        null, null, column[0], column[1], false));
            }
        }

        this.data = data == null ? new String[0][] : data;

        if (LOG_LEVEL.compareTo(Level.TRACE) >= 0) {
            for (Object[] row : this.data) {
                Logger.trace("* Row: {}", Arrays.toString(row));
            }
        }

        Logger.trace("Dummy result set @{} is ready for use", hashCode());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T getValue(int columnIndex, Class<T> clazz)
            throws SQLException {
        Logger.trace("Trying to get value with inputs: line={}, column={}, type={}", getRow(), columnIndex, clazz);

        Object obj = currentRow[columnIndex - 1];
        T result = null;

        Logger.trace("Got raw value [{}] from line {} of {}", obj, getRow(), data.length);
        wasNull = obj == null;
        result = this.getDataTypeConverters().convert(obj, clazz, false);

        Logger.trace("Return value: raw={}, converted={}", obj, result);

        return result;
    }

    @Override
    protected boolean hasMore() {
        return getCurrentRowIndex() < data.length;
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
        boolean success = row < data.length;
        if (success) {
            currentRow = data[row];
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

    public int getColumnCount() {
        return data.length > 0 && data[0] != null ? data[0].length
                : 0;
    }

    public int getRowCount() {
        return data.length == 0 ? 0 : data.length;
    }
}
