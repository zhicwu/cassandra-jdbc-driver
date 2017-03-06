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
package com.github.cassandra.jdbc.provider.datastax;

import com.datastax.driver.core.*;
import com.github.cassandra.jdbc.*;
import com.google.common.base.Strings;
import org.pmw.tinylog.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraUtils.*;

/**
 * This is a connection implementation built on top of DataStax Java driver.
 *
 * @author Zhichun Wu
 */
public class CassandraConnection extends BaseCassandraConnection {
    static final Statement CQL_TO_GET_VERSION = new SimpleStatement(
            "select release_version from system.local where key='local' limit 1");

    static final String PROVIDER_NAME = "DataStax Java Driver";
    static final String DRIVER_NAME = CassandraConfiguration.DRIVER_NAME + " (using " + PROVIDER_NAME + ")";

    static {
        CQL_TO_GET_VERSION.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    }

    private DataStaxSessionWrapper _session;

    private String _keyspace;

    public CassandraConnection(CassandraConfiguration driverConfig) {
        super(driverConfig);

        _keyspace = driverConfig.getKeyspace();
        _session = DataStaxSessionFactory.getSession(driverConfig);

        // populate meta data
        metaData.setProperty(KEY_DRIVER_NAME, DRIVER_NAME);

        metaData.setProperty(KEY_DRIVER_VERSION, new StringBuilder()
                .append(CassandraConfiguration.DRIVER_VERSION)
                .append(' ')
                .append('(')
                .append(PROVIDER_NAME)
                .append(' ')
                .append(Cluster.getDriverVersion())
                .append(')')
                .toString());
    }

    private ResultSet buildResultSet(String[][] columns, List<Object[]> list) {
        Object[][] data = new Object[list.size()][];
        int index = 0;
        for (Object[] objs : list) {
            data[index++] = objs;
        }

        return new DummyCassandraResultSet(columns, data);
    }

    private Object[] populateColumnMetaData(KeyspaceMetadata ks,
                                            TableMetadata t, ColumnMetadata c, int colIndex) {
        String cqlType = c.getType().toString().toLowerCase();
        boolean isKey = t.getPrimaryKey().contains(c);
        return new Object[]{
                null, // TABLE_CAT
                ks.getName(), // TABLE_SCHEM
                t.getName(), // TABLE_NAME
                c.getName(), // COLUMN_NAME
                DataStaxDataTypes.mappings.sqlTypeFor(cqlType), // DATA_TYPE
                cqlType, // TYPE_NAME
                0, // COLUMN_SIZE
                65535, // BUFFER_LENGTH
                0, // DECIMAL_DIGITS
                10, // NUM_PREC_RADIX
                isKey ? java.sql.DatabaseMetaData.typeNoNulls : java.sql.DatabaseMetaData.typeNullable, // NULLABLE
                EMPTY_STRING, // REMARKS
                null, // COLUMN_DEF, default value for the column
                0, // SQL_DATA_TYPE
                0, // SQL_DATETIME_SUB
                0, // CHAR_OCTET_LENGTH
                colIndex, // ORDINAL_POSITION
                isKey ? "NO" : "YES", // IS_NULLABLE
                null, // SCOPE_CATALOG
                null, // SCOPE_SCHEMA
                null, // SCOPE_TABLE
                null, // SOURCE_DATA_TYPE
                "NO", // IS_AUTOINCREMENT
                "NO" // IS_GENERATEDCOLUMN
        };
    }

    private Object[] populateIndexMetaData(KeyspaceMetadata ks,
                                           TableMetadata t, ColumnMetadata pk, int colIndex, boolean unique) {
        return new Object[]{
                null, // TABLE_CAT
                ks.getName(), // TABLE_SCHEM
                t.getName(), // TABLE_NAME
                unique, // NON_UNIQUE
                EMPTY_STRING, // INDEX_QUALIFIER
                "PRIMARY", // INDEX_NAME
                java.sql.DatabaseMetaData.tableIndexOther, // TYPE
                colIndex, // ORDINAL_POSITION
                pk.getName(), // COLUMN_NAME
                // column sort sequence, "A" => ascending, "D" => descending,
                // may be null if sort sequence is not supported; null when TYPE
                // is tableIndexStatistic
                null, // ASC_OR_DESC
                100, // FIXME CARDINALITY
                0, // PAGES
                null // FILTER_CONDITION
        };
    }

    private Object[] populateIndexMetaData(KeyspaceMetadata ks,
                                           TableMetadata t, IndexMetadata i) {
        return new Object[]{
                null, // TABLE_CAT
                ks.getName(), // TABLE_SCHEM
                t.getName(), // TABLE_NAME
                false, // NON_UNIQUE
                EMPTY_STRING, // INDEX_QUALIFIER
                i.getName(), // INDEX_NAME
                java.sql.DatabaseMetaData.tableIndexOther, // TYPE
                1, // ORDINAL_POSITION
                i.getTarget(), //.getIndexedColumn().getName(), // COLUMN_NAME
                // column sort sequence, "A" => ascending, "D" => descending,
                // may be null if sort sequence is not supported; null when TYPE
                // is tableIndexStatistic
                null, // FIXME ASC_OR_DESC
                100, // FIXME CARDINALITY
                0, // PAGES
                null // FILTER_CONDITION
        };
    }

    private Object[] populatePrimaryKeyMetaData(KeyspaceMetadata ks,
                                                TableMetadata t, ColumnMetadata c, int colIndex) {
        return new Object[]{
                null, // TABLE_CAT
                ks.getName(), // TABLE_SCHEM
                t.getName(), // TABLE_NAME
                c.getName(), // COLUMN_NAME
                colIndex, // KEY_SEQ
                "PRIMARY" // PK_NAME
        };
    }

    private Object[] populateTableMetaData(KeyspaceMetadata ks, TableMetadata t) {
        return new Object[]{
                null, // TABLE_CAT
                ks.getName(), // TABLE_SCHEM
                t.getName(), // TABLE_NAME
                "TABLE", // TABLE_TYPE
                new StringBuilder().append("ID: [").append(t.getId())
                        .append("] \nDDL: \n").append(t.exportAsString())
                        .toString(), // REMARKS
                null, // TYPE_CAT
                null, // TYPE_SCHEM
                null, // TYPE_NAME
                null, // SELF_REFERENCING_COL_NAME
                "USER" // REF_GENERATION
        };
    }

    private Object[] populateUdtMetaData(KeyspaceMetadata ks, UserType t) {
        return new Object[]{
                null, // TYPE_CAT
                ks.getName(), // TABLE_SCHEM
                t.getTypeName(), // TYPE_NAME
                t.getTypeArguments(), //.getCustomTypeClassName(), // CLASS_NAME
                java.sql.Types.JAVA_OBJECT, // DATA_TYPE
                t.getName().toString(), // REMARKS
                null // BASE_TYPE
        };
    }

    @Override
    protected <T> T createObject(Class<T> clazz) throws SQLException {
        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }

        return null;
    }

    @SuppressWarnings("resource")
    @Override
    protected ResultSet getObjectMetaData(CassandraObjectType objectType,
                                          Properties queryPatterns, Object... additionalHints)
            throws SQLException {
        Logger.trace("Trying to get meta data with the following parameters:\nobjectType: {}\nqueryPatterns:\n{}\nadditionalHints: {}",
                objectType.name(), queryPatterns, additionalHints);

        ResultSet rs = new DummyCassandraResultSet();

        Metadata m = _session.getClusterMetaData();
        switch (objectType) {
            case KEYSPACE: {
                List<KeyspaceMetadata> keyspaces = m.getKeyspaces();
                String[][] data = new String[keyspaces.size()][1];
                int index = 0;
                for (KeyspaceMetadata km : keyspaces) {
                    data[index++][0] = km.getName();
                }

                rs = new DummyCassandraResultSet(SCHEMA_COLUMNS, data);
                break;
            }

            case TABLE: {
                String schemaPattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_SCHEMA_PATTERN);
                String tablePattern = CassandraUtils.getPropertyValue(
                        queryPatterns, KEY_TABLE_PATTERN);

                boolean queryTable = false;
                if (additionalHints != null) {
                    for (Object type : additionalHints) {
                        if (CassandraObjectType.TABLE.toString().equals(type)) {
                            queryTable = true;
                            break;
                        }
                    }
                } else {
                    queryTable = true;
                }

                List<Object[]> list = new ArrayList<Object[]>();
                if (queryTable) {
                    for (KeyspaceMetadata ks : m.getKeyspaces()) {
                        if (CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                            for (TableMetadata t : ks.getTables()) {
                                if (CassandraUtils.matchesPattern(t.getName(),
                                        tablePattern)) {
                                    list.add(populateTableMetaData(ks, t));
                                }
                            }
                        }
                    }
                }

                rs = buildResultSet(TABLE_COLUMNS, list);
                break;
            }

            case COLUMN: {
                String schemaPattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_SCHEMA_PATTERN);
                String tablePattern = CassandraUtils.getPropertyValue(
                        queryPatterns, KEY_TABLE_PATTERN);
                String columnPattern = CassandraUtils.getPropertyValue(
                        queryPatterns, KEY_COLUMN_PATTERN);

                List<Object[]> list = new ArrayList<Object[]>();
                for (KeyspaceMetadata ks : m.getKeyspaces()) {
                    if (CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                        for (TableMetadata t : ks.getTables()) {
                            if (CassandraUtils.matchesPattern(t.getName(),
                                    tablePattern)) {
                                int colIndex = 0;
                                for (ColumnMetadata c : t.getColumns()) {
                                    colIndex++;
                                    // Why DataStax Java driver returned additional
                                    // blob column?
                                    // Try system.IndexInfo...
                                    if (!CassandraUtils.isNullOrEmptyString(c
                                            .getName())
                                            && CassandraUtils.matchesPattern(
                                            c.getName(), columnPattern)) {
                                        list.add(populateColumnMetaData(ks, t, c,
                                                colIndex));
                                    }
                                }
                            }
                        }
                    }
                }

                rs = buildResultSet(COLUMN_COLUMNS, list);
                break;
            }

            case INDEX: {
                String schemaPattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_SCHEMA_PATTERN);
                String tablePattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_TABLE_PATTERN);
                boolean uniqueIndexOnly = Boolean.valueOf(CassandraUtils
                        .getPropertyValue(queryPatterns, KEY_UNIQUE_INDEX,
                                Boolean.FALSE.toString()));
                // boolean approximateIndex = Boolean.valueOf(CassandraUtils
                // .getPropertyValue(queryPatterns, KEY_APPROXIMATE_INDEX,
                // Boolean.TRUE.toString()));

                List<Object[]> list = new ArrayList<Object[]>();
                for (KeyspaceMetadata ks : m.getKeyspaces()) {
                    if (CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                        for (TableMetadata t : ks.getTables()) {
                            if (CassandraUtils.matchesPattern(t.getName(), tablePattern)) {
                                int colIndex = 0;
                                List<ColumnMetadata> primaryKeys = t
                                        .getPrimaryKey();
                                boolean uniquePk = primaryKeys.size() == 1;
                                if (!uniqueIndexOnly || uniquePk) {
                                    for (ColumnMetadata c : primaryKeys) {
                                        list.add(populateIndexMetaData(ks, t, c,
                                                ++colIndex, uniquePk));
                                    }
                                }


                                for (IndexMetadata i : t.getIndexes()) {
                                    if (i != null && i.getKind() != IndexMetadata.Kind.KEYS) {
                                        list.add(populateIndexMetaData(ks, t, i));
                                    }
                                }
                            }
                        }
                    }
                }

                rs = buildResultSet(INDEX_COLUMNS, list);
                break;
            }

            case PRIMARY_KEY: {
                String schemaPattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_SCHEMA_PATTERN);
                String tablePattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_TABLE_PATTERN);

                List<Object[]> list = new ArrayList<Object[]>();
                for (KeyspaceMetadata ks : m.getKeyspaces()) {
                    if (CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                        for (TableMetadata t : ks.getTables()) {
                            if (CassandraUtils.matchesPattern(t.getName(), tablePattern)) {
                                int colIndex = 0;
                                for (ColumnMetadata c : t.getPrimaryKey()) {
                                    list.add(populatePrimaryKeyMetaData(ks, t, c,
                                            ++colIndex));
                                }
                            }
                        }
                    }
                }

                rs = buildResultSet(PK_COLUMNS, list);
                break;
            }

            case UDT: {
                String schemaPattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_SCHEMA_PATTERN);
                String typePattern = CassandraUtils.getPropertyValue(queryPatterns,
                        KEY_TYPE_PATTERN);

                // FIXME deal with additionalHints

                List<Object[]> list = new ArrayList<Object[]>();
                for (KeyspaceMetadata ks : m.getKeyspaces()) {
                    if (CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                        for (UserType t : ks.getUserTypes()) {
                            if (CassandraUtils.matchesPattern(t.getTypeName(),
                                    typePattern)) {
                                list.add(populateUdtMetaData(ks, t));
                            }
                        }
                    }
                }

                rs = buildResultSet(UDT_COLUMNS, list);
                break;
            }

            default: {
                rs = super.getObjectMetaData(objectType, queryPatterns,
                        additionalHints);
                break;

            }
        }

        int rowCount = 0;
        int colCount = 0;

        if (rs instanceof DummyCassandraResultSet) {
            DummyCassandraResultSet drs = (DummyCassandraResultSet) rs;

            rowCount = drs.getRowCount();
            colCount = drs.getColumnCount();
        }

        Logger.trace("Returning results with {} x {}  two-dimensional array", rowCount, colCount);

        return rs;
    }

    @Override
    protected SQLException tryClose() {
        SQLException e = CassandraUtils.tryClose(_session);
        _session = null;

        return e;
    }

    @Override
    protected Object unwrap() {
        return _session;
    }

    @Override
    protected void validateState() throws SQLException {
        super.validateState();

        // this might happen if someone close the connection directly without
        // touching JDBC API
        if (_session == null || _session.isClosed()) {
            _session = null;
            throw CassandraErrors.connectionClosedException();
        }
    }

    public java.sql.Statement createStatement(int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        validateState();

        return new CassandraStatement(this, _session);
    }

    public String getSchema() throws SQLException {
        validateState();

        return _keyspace;
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType,
                                                       int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        validateState();

        return new CassandraPreparedStatement(this, _session, sql);
    }

    public void setSchema(String schema) throws SQLException {
        validateState();

        if (!quiet) {
            throw CassandraErrors.notSupportedException();
        }

        if (Strings.isNullOrEmpty(schema)
                || schema.equals(_keyspace)) {
            return;
        }

        try {
            // _session.closeAsync().force();
            _session = DataStaxSessionFactory.getSession(getConfiguration(), schema);

            Logger.debug(new StringBuilder(
                    "Current keyspace changed from \"").append(_keyspace)
                    .append("\" to \"").append(schema)
                    .append("\" successfully").toString());

            _keyspace = schema;
        } catch (Exception e) {
            throw CassandraErrors.failedToChangeKeyspaceException(schema, e);
        }
    }
}
