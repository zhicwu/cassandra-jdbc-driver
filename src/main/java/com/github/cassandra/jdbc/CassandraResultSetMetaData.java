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

import org.pmw.tinylog.Logger;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This represents meta data of a {@link java.sql.ResultSet}.
 *
 * @author Zhichun Wu
 */
public class CassandraResultSetMetaData extends BaseJdbcObject implements
        ResultSetMetaData {
    protected final List<CassandraColumnDefinition> columnDefinitions = new ArrayList<CassandraColumnDefinition>();
    protected final Map<String, Integer> columnNameIndices = new HashMap<String, Integer>();

    public CassandraResultSetMetaData() {
        super(true);
    }

    @Override
    protected SQLException tryClose() {
        return null;
    }

    @Override
    protected Object unwrap() {
        return this;
    }

    public void addColumnDefinition(CassandraColumnDefinition def) {
        if (columnDefinitions.add(def)) {
            columnNameIndices.put(def.getColumnName(),
                    columnDefinitions.size() - 1);
        }
    }

    public void clear() {
        columnNameIndices.clear();
        columnDefinitions.clear();
    }

    public String getCatalogName(int column) throws SQLException {
        return getColumnDefinition(column).getCatalogName();
    }

    public String getColumnClassName(int column) throws SQLException {
        return getColumnDefinition(column).getColumnClassName();
    }

    public int getColumnCount() throws SQLException {
        return columnDefinitions.size();
    }

    public CassandraColumnDefinition getColumnDefinition(int column)
            throws SQLException {
        if (column > 0 && column <= columnDefinitions.size()) {
            return columnDefinitions.get(column - 1);
        }

        Logger.trace("Columns for your reference: " + columnNameIndices);

        throw new SQLException("Column " + column + " does not exists!");
    }

    public CassandraColumnDefinition getColumnDefinition(String columnName)
            throws SQLException {
        int column = columnNameIndices.containsKey(columnName) ? columnNameIndices
                .get(columnName) : -1;
        if (column >= 0 && column < columnDefinitions.size()) {
            return columnDefinitions.get(column);
        }

        Logger.trace("Columns for your reference: " + columnNameIndices);

        throw new SQLException("Column " + columnName + " does not exists!");
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumnDefinition(column).getColumnDisplaySize();
    }

    public int getColumnIndex(String columnLabel) throws SQLException {
        if (!columnNameIndices.containsKey(columnLabel)) {
            throw new SQLException("Column label \"" + columnLabel
                    + "\" does not exists");
        }

        return columnNameIndices.get(columnLabel);
    }

    public String getColumnLabel(int column) throws SQLException {
        return getColumnDefinition(column).getColumnLabel();
    }

    public String getColumnName(int column) throws SQLException {
        return getColumnDefinition(column).getColumnName();
    }

    public int getColumnType(int column) throws SQLException {
        return getColumnDefinition(column).getColumnType();
    }

    public String getColumnTypeName(int column) throws SQLException {
        return getColumnDefinition(column).getColumnTypeName();
    }

    public int getPrecision(int column) throws SQLException {
        return getColumnDefinition(column).getPrecision();
    }

    public int getScale(int column) throws SQLException {
        return getColumnDefinition(column).getScale();
    }

    public String getSchemaName(int column) throws SQLException {
        return getColumnDefinition(column).getSchemaName();
    }

    public String getTableName(int column) throws SQLException {
        return getColumnDefinition(column).getTableName();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumnDefinition(column).isAutoIncrement();
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumnDefinition(column).isCaseSensitive();
    }

    public boolean isCurrency(int column) throws SQLException {
        return getColumnDefinition(column).isCurrency();
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumnDefinition(column).isDefinitelyWritable();
    }

    public int isNullable(int column) throws SQLException {
        return getColumnDefinition(column).isNullable();
    }

    public boolean isReadOnly(int column) throws SQLException {
        return getColumnDefinition(column).isReadOnly();
    }

    public boolean isSearchable(int column) throws SQLException {
        return getColumnDefinition(column).isSearchable();
    }

    public boolean isSigned(int column) throws SQLException {
        return getColumnDefinition(column).isSigned();
    }

    public boolean isWritable(int column) throws SQLException {
        return getColumnDefinition(column).isWritable();
    }
}
