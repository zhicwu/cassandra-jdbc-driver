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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;

/**
 * This is a utility class.
 *
 * @author Zhichun Wu
 */
public class CassandraUtils {
    static final String BUNDLE_NAME = CassandraUtils.class.getPackage()
            .getName() + ".messages";
    static final ResultSet DUMMY_RESULT_SET = new DummyCassandraResultSet();

    static final ResourceBundle RESOURCE_BUNDLE;

    public static final String KEY_DB_MAJOR_VERSION = "dbMajorVersion";
    public static final String KEY_DB_MINOR_VERSION = "dbMinorVersion";

    public static final String KEY_DRIVER_NAME = "driverName";
    public static final String KEY_DRIVER_VERSION = "driverVersion";

    public static final String EMPTY_STRING = "";
    public static final String[][] SCHEMA_COLUMNS = new String[][]{{
            "TABLE_CAT", "text"}};
    public static final String[][] COLUMN_COLUMNS = new String[][]{
            {"TABLE_CAT", "text"}, {"TABLE_SCHEM", "text"},
            {"TABLE_NAME", "text"}, {"COLUMN_NAME", "text"},
            {"DATA_TYPE", "int"}, {"TYPE_NAME", "text"},
            {"COLUMN_SIZE", "int"}, {"BUFFER_LENGTH", "int"},
            {"DECIMAL_DIGITS", "int"}, {"NUM_PREC_RADIX", "int"},
            {"NULLABLE", "int"}, {"REMARKS", "text"},
            {"COLUMN_DEF", "text"}, {"SQL_DATA_TYPE", "int"},
            {"SQL_DATETIME_SUB", "int"}, {"CHAR_OCTET_LENGTH", "int"},
            {"ORDINAL_POSITION", "int"}, {"IS_NULLABLE", "text"},
            {"SCOPE_CATALOG", "text"}, {"SCOPE_SCHEMA", "text"},
            {"SCOPE_TABLE", "text"}, {"SOURCE_DATA_TYPE", "short"},
            {"IS_AUTOINCREMENT", "text"}, {"IS_GENERATEDCOLUMN", "text"}};
    public static final String CURSOR_PREFIX = "cursor@";
    public static final String DEFAULT_DB_MAJOR_VERSION = "2";
    public static final String DEFAULT_DB_MINOR_VERSION = "0";

    public static final String[][] INDEX_COLUMNS = new String[][]{
            {"TABLE_CAT", "text"}, {"TABLE_SCHEM", "text"},
            {"TABLE_NAME", "text"}, {"NON_UNIQUE", "boolean"},
            {"INDEX_QUALIFIER", "text"}, {"INDEX_NAME", "text"},
            {"TYPE", "int"}, {"ORDINAL_POSITION", "int"},
            {"COLUMN_NAME", "text"}, {"ASC_OR_DESC", "text"},
            {"CARDINALITY", "int"}, {"PAGES", "int"},
            {"FILTER_CONDITION", "text"}};

    public static final String KEY_APPROXIMATE_INDEX = "approximateIndexInfo";

    public static final String KEY_CATALOG = "catalog";

    public static final String KEY_COLUMN_PATTERN = "columnNamePattern";


    public static final String KEY_NUMERIC_FUNCTIONS = "numericFunctions";

    public static final String KEY_PRODUCT_NAME = "productName";
    public static final String KEY_PRODUCT_VERSION = "productVersion";

    public static final String KEY_SQL_KEYWORDS = "keywords";

    public static final String KEY_STRING_FUNCTIONS = "stringFunctions";

    public static final String KEY_SYSTEM_FUNCTIONS = "systemFunctions";
    public static final String KEY_SCHEMA_PATTERN = "schemaPattern";
    public static final String KEY_TABLE_PATTERN = "tableNamePattern";

    public static final String KEY_TIMEDATE_FUNCTIONS = "timeDateFunctions";
    public static final String KEY_TYPE_PATTERN = "typeNamePattern";
    public static final String KEY_UNIQUE_INDEX = "uniqueIndexOnly";

    public static final String[][] PK_COLUMNS = new String[][]{
            {"TABLE_CAT", "text"}, {"TABLE_SCHEM", "text"},
            {"TABLE_NAME", "text"}, {"COLUMN_NAME", "text"},
            {"KEY_SEQ", "int"}, {"PK_NAME", "text"}};
    public static final String[][] TABLE_COLUMNS = new String[][]{
            {"TABLE_CAT", "text"}, {"TABLE_SCHEM", "text"},
            {"TABLE_NAME", "text"}, {"TABLE_TYPE", "text"},
            {"REMARKS", "text"}, {"TYPE_CAT", "text"},
            {"TYPE_SCHEM", "text"}, {"TYPE_NAME", "text"},
            {"SELF_REFERENCING_COL_NAME", "text"},
            {"REF_GENERATION", "text"}};

    // meta data
    public static final String[][] TABLE_TYPE_COLUMNS = new String[][]{{
            "TABLE_TYPE", "text"}};

    public static final Object[][] TABLE_TYPE_DATA = new Object[][]{new Object[]{"TABLE"}};

    public static final String[][] TYPE_COLUMNS = new String[][]{
            {"TYPE_NAME", "text"}, {"DATA_TYPE", "int"},
            {"PRECISION", "int"}, {"LITERAL_PREFIX", "text"},
            {"LITERAL_SUFFIX", "text"}, {"CREATE_PARAMS", "text"},
            {"NULLABLE", "int"}, {"CASE_SENSITIVE", "boolean"},
            {"SEARCHABLE", "int"}, {"UNSIGNED_ATTRIBUTE", "boolean"},
            {"FIXED_PREC_SCALE", "boolean"}, {"AUTO_INCREMENT", "boolean"},
            {"LOCAL_TYPE_NAME", "text"}, {"MINIMUM_SCALE", "int"},
            {"MAXIMUM_SCALE", "int"}, {"SQL_DATA_TYPE", "int"},
            {"SQL_DATETIME_SUB", "int"}, {"NUM_PREC_RADIX", "int"}};

    public static final String[][] UDT_COLUMNS = new String[][]{
            {"TYPE_CAT", "text"}, {"TYPE_SCHEM", "text"},
            {"TYPE_NAME", "text"}, {"CLASS_NAME", "text"},
            {"DATA_TYPE", "int"}, {"REMARKS", "text"},
            {"BASE_TYPE", "int"}};

    static {
        ResourceBundle bundle = null;

        try {
            bundle = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(),
                    CassandraUtils.class.getClassLoader());
        } catch (Throwable t) {
            try {
                bundle = ResourceBundle.getBundle(BUNDLE_NAME, Locale.US);
            } catch (Throwable e) {
                throw new RuntimeException(
                        "Failed to load resource bundle due to underlying exception: "
                                + t.toString(), e);
            }
        } finally {
            RESOURCE_BUNDLE = bundle;
        }
    }

    public static Object[][] getAllData(ResultSet rs) throws SQLException {
        return getAllData(rs, true);
    }

    public static Object[][] getAllData(ResultSet rs, boolean closeResultSet)
            throws SQLException {
        List<Object[]> list = new ArrayList<Object[]>();

        if (rs != null) {
            try {
                if (!rs.isBeforeFirst()) {
                    throw new IllegalStateException("We need a fresh rs");
                }

                Object[] columns = null;
                while (rs.next()) {
                    if (rs.isFirst()) {
                        ResultSetMetaData metaData = rs.getMetaData();
                        columns = new Object[metaData.getColumnCount()];
                    }

                    for (int i = 0; i < columns.length; i++) {
                        columns[i] = rs.getObject(i + 1);
                    }

                    list.add(columns);
                    columns = new Object[columns.length];
                }
            } catch (SQLException e) {
                throw e;
            } finally {
                if (closeResultSet) {
                    rs.close();
                }
            }
        }

        Object[][] data = new Object[list.size()][];
        int index = 0;
        for (Object[] ss : list) {
            data[index++] = ss;
        }

        return data;
    }

    public static String[] getColumnNames(ResultSet rs) throws SQLException {
        return getColumnNames(rs, false);
    }

    public static String[] getColumnNames(ResultSet rs, boolean closeResultSet)
            throws SQLException {
        String[] columns = new String[0];

        if (rs != null) {
            try {
                ResultSetMetaData metaData = rs.getMetaData();

                columns = new String[metaData.getColumnCount()];
                for (int i = 0; i < columns.length; i++) {
                    columns[i] = metaData.getColumnName(i + 1);
                }
            } catch (SQLException e) {
                throw e;
            } finally {
                if (closeResultSet) {
                    rs.close();
                }
            }
        }

        return columns;
    }

    public static String getPropertyValue(Properties props, String key) {
        return getPropertyValue(props, key, EMPTY_STRING);
    }

    /**
     * Get string value from given properties.
     *
     * @param props        properties
     * @param key          key for looking up the value
     * @param defaultValue default value
     * @return property value as string
     */
    public static String getPropertyValue(Properties props, String key,
                                          String defaultValue) {
        return props == null || !props.containsKey(key) ? defaultValue : props
                .getProperty(key, defaultValue);
    }

    /**
     * Get integer value from given properties.
     *
     * @param props        properties
     * @param key          key for looking up the value
     * @param defaultValue default value
     * @return property value as integer
     */
    public static int getPropertyValueAsInt(Properties props, String key,
                                            int defaultValue) {
        return props == null || !props.containsKey(key) ? defaultValue
                : Integer.parseInt(props.getProperty(key));
    }

    /**
     * Returns the localized message based on the given message key.
     *
     * @param key the message key
     * @return The localized message for the key
     */
    public static String getString(String key) {
        if (RESOURCE_BUNDLE == null) {
            throw new RuntimeException("Messages from resource bundle '"
                    + BUNDLE_NAME
                    + "' not loaded during initialization of driver.");
        }

        try {
            if (key == null) {
                throw new IllegalArgumentException(
                        "Message key can not be null");
            }

            String message = RESOURCE_BUNDLE.getString(key);

            if (message == null) {
                message = "Missing message for key '" + key + "'";
            }

            return message;
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    public static String getString(String key, Object... args) {
        return MessageFormat.format(getString(key), args);
    }

    public static boolean isNullOrEmptyString(String str) {
        return isNullOrEmptyString(str, true);
    }

    public static boolean isNullOrEmptyString(String str, boolean trimRequired) {
        return str == null || EMPTY_STRING.equals(str)
                || (trimRequired && EMPTY_STRING.equals(str.trim()));
    }

    public static boolean matchesPattern(String name, String pattern) {
        return isNullOrEmptyString(pattern) || pattern.equals("%")
                || pattern.equals(name);
    }

    public static SQLException tryClose(AutoCloseable resource) {
        SQLException exception = null;

        if (resource != null) {
            String resourceName = new StringBuilder(resource.getClass()
                    .getName()).append('@').append(resource.hashCode())
                    .toString();

            Logger.debug(new StringBuilder("Trying to close [")
                    .append(resourceName).append(']').toString());

            try {
                resource.close();
                Logger.debug(new StringBuilder().append("[")
                        .append(resourceName)
                        .append("] closed successfully").toString());
            } catch (Throwable t) {
                exception = CassandraErrors.failedToCloseResourceException(
                        resourceName, t);

                Logger.warn(t,
                        new StringBuilder("Error occurred when closing [")
                                .append(resourceName).append("]")
                                .toString());
            }
        }

        return exception;
    }

    private CassandraUtils() {
    }
}
