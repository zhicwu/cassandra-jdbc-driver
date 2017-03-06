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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CassandraParameterMetaData extends BaseJdbcObject implements ParameterMetaData {
    private final BaseCassandraConnection _conn;
    private final List<CassandraColumnDefinition> _columns;

    protected CassandraParameterMetaData(BaseCassandraConnection conn) {
        super(conn == null || conn.quiet);

        _conn = conn;
        _columns = new ArrayList<CassandraColumnDefinition>();
    }

    @Override
    protected SQLException tryClose() {
        return null;
    }

    @Override
    protected Object unwrap() {
        return this;
    }

    protected CassandraColumnDefinition getParameterDefinition(int index) {
        return _columns.get(index - 1);
    }

    public void addParameterDefinition(CassandraColumnDefinition def) {
        _columns.add(def);
    }

    public void clear() {
        _columns.clear();
    }

    public int getParameterCount() throws SQLException {
        return _columns.size();
    }

    public int isNullable(int param) throws SQLException {
        return getParameterDefinition(param).isNullable();
    }

    public boolean isSigned(int param) throws SQLException {
        return getParameterDefinition(param).isSigned();
    }

    public int getPrecision(int param) throws SQLException {
        return getParameterDefinition(param).getPrecision();
    }

    public int getScale(int param) throws SQLException {
        return getParameterDefinition(param).getScale();
    }

    public int getParameterType(int param) throws SQLException {
        return getParameterDefinition(param).getColumnType();
    }

    public String getParameterTypeName(int param) throws SQLException {
        return getParameterDefinition(param).getColumnTypeName();
    }

    public Class getParameterClass(int param) throws SQLException {
        return getParameterDefinition(param).getColumnClass();
    }

    public String getParameterClassName(int param) throws SQLException {
        return getParameterDefinition(param).getColumnClassName();
    }

    public int getParameterMode(int param) throws SQLException {
        getParameterDefinition(param);

        return ParameterMetaData.parameterModeIn;
    }
}
