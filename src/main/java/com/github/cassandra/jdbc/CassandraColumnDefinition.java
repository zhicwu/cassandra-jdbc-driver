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

import java.sql.ResultSetMetaData;

import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;

/**
 * This defines a column in Cassandra.
 *
 * @author Zhichun Wu
 */
public class CassandraColumnDefinition {
    protected final String catalog;
    protected final String cqlType;
    protected final Class<?> javaType;
    protected final String label;
    protected final String name;
    protected final int precision;
    protected final int scale;
    protected final String schema;
    protected final boolean searchable;
    protected final int sqlType;
    protected final String table;
    protected final boolean writable;

    public CassandraColumnDefinition(String schema, String table, String name,
                                     String type, boolean searchable) {
        this(schema, table, name, name, type, searchable, false);
    }

    public CassandraColumnDefinition(String schema, String table, String name,
                                     String label, String type, boolean searchable, boolean writable) {
        this.catalog = EMPTY_STRING;
        this.schema = Strings.nullToEmpty(schema);
        this.table = Strings.nullToEmpty(table);
        this.name = Strings.nullToEmpty(name);
        this.label = Strings.isNullOrEmpty(label) ? name : label;
        this.cqlType = CassandraDataTypeMappings.instance.cqlTypeFor(type);
        this.sqlType = CassandraDataTypeMappings.instance.sqlTypeFor(this.cqlType);
        this.javaType = CassandraDataTypeMappings.instance.javaTypeFor(this.cqlType);
        this.searchable = searchable;
        this.writable = writable;
        this.precision = CassandraDataTypeMappings.instance.precisionFor(this.cqlType);
        this.scale = CassandraDataTypeMappings.instance.scaleFor(this.cqlType);
    }

    public String getCatalogName() {
        return catalog;
    }

    public Class getColumnClass() {
        return javaType;
    }

    public String getColumnClassName() {
        return javaType.getName();
    }

    public int getColumnDisplaySize() {
        return 0;
    }

    public String getColumnLabel() {
        return label;
    }

    public String getColumnName() {
        return name;
    }

    public int getColumnType() {
        return sqlType;
    }

    public String getColumnTypeName() {
        return cqlType;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public String getSchemaName() {
        return schema;
    }

    public String getTableName() {
        return table;
    }

    public boolean isAutoIncrement() {
        return false;
    }

    public boolean isCaseSensitive() {
        return true;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isDefinitelyWritable() {
        return writable;
    }

    public int isNullable() {
        return ResultSetMetaData.columnNullable;
    }

    public boolean isReadOnly() {
        return !writable;
    }

    public boolean isSearchable() {
        return searchable;
    }

    public boolean isSigned() {
        return false;
    }

    public boolean isWritable() {
        return writable;
    }
}
