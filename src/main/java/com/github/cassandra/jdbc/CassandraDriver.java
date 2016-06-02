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

import com.google.common.collect.Iterables;

import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraConfiguration.*;

/**
 * Cassandra JDBC driver.
 *
 * @author Zhichun Wu
 */
public class CassandraDriver implements Driver {
    public static final int VERSION_MAJOR = 0;
    public static final int VERSION_MINOR = 4;
    public static final int VERSION_PATCH = 0;

    static final String MSG_PREFIX = "MESSAGE_PROP_";
    static final String MSG_SUFFIX = "_DESCRIPTION";

    static final String PROVIDER_PREFIX = CassandraUtils.class.getPackage()
            .getName() + ".provider.";
    static final String PROVIDER_SUFFIX = "CassandraConnection";

    static {
        // Register the CassandraDriver with DriverManager
        try {
            CassandraDriver driver = new CassandraDriver();
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static BaseCassandraConnection createConnection(CassandraConfiguration config)
            throws SQLException {
        BaseCassandraConnection conn;

        try {
            // FIXME needs a better way to isolate base class and implementation
            Class<?> clazz = CassandraUtils.class.getClassLoader().loadClass(
                    new StringBuffer()
                            .append(PROVIDER_PREFIX)
                            .append(config.getProvider()).append('.')
                            .append(PROVIDER_SUFFIX).toString());
            Constructor<?> c = clazz.getConstructor(CassandraConfiguration.class);
            conn = (BaseCassandraConnection) c.newInstance(config);
        } catch (Exception e) {
            throw new SQLException(e.getCause() == null ? e : e.getCause());
        }

        return conn;
    }

    private DriverPropertyInfo createDriverPropertyInfo(String propertyName,
                                                        String propertyValue, boolean required, String[] choices) {
        DriverPropertyInfo info = new DriverPropertyInfo(propertyName,
                propertyValue);
        info.required = required;
        info.description = CassandraUtils.getString(new StringBuilder(
                MSG_PREFIX).append(propertyName.toUpperCase())
                .append(MSG_SUFFIX).toString());

        if (choices != null && choices.length > 0) {
            info.choices = choices;
        }

        return info;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return CassandraConfiguration.isValidUrl(url);
    }

    public Connection connect(String url, Properties props) throws SQLException {
        if (acceptsURL(url)) {
            CassandraConfiguration config = new CassandraConfiguration(url, props);

            // load concrete Cassandra driver based on the properties
            return createConnection(config);
        } else {
            // signal it is the wrong driver for this
            return null;
        }
    }

    public int getMajorVersion() {
        return VERSION_MAJOR;
    }

    public int getMinorVersion() {
        return VERSION_MINOR;
    }

    public java.util.logging.Logger getParentLogger()
            throws SQLFeatureNotSupportedException {
        throw CassandraErrors.notSupportedException();
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
            throws SQLException {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();

        list.add(createDriverPropertyInfo(KEY_USERNAME,
                CassandraUtils.getPropertyValue(props, KEY_USERNAME), true,
                null));
        list.add(createDriverPropertyInfo(KEY_PASSWORD,
                CassandraUtils.getPropertyValue(props, KEY_PASSWORD), true,
                null));
        list.add(createDriverPropertyInfo(KEY_PORT,
                CassandraUtils.getPropertyValue(props, KEY_PORT), false, null));
        list.add(createDriverPropertyInfo(KEY_CONNECT_TIMEOUT, CassandraUtils
                .getPropertyValue(props, KEY_CONNECT_TIMEOUT,
                        DEFAULT_CONNECT_TIMEOUT), false, null));
        list.add(createDriverPropertyInfo(KEY_READ_TIMEOUT,
                CassandraUtils.getPropertyValue(props, KEY_READ_TIMEOUT,
                        DEFAULT_READ_TIMEOUT), false, null));
        list.add(createDriverPropertyInfo(KEY_CONSISTENCY_LEVEL, CassandraUtils
                .getPropertyValue(props, KEY_CONSISTENCY_LEVEL,
                        DEFAULT_CONSISTENCY_LEVEL), false, new String[]{
                "ANY", "ONE", "LOCAL_ONE", "QUORUM", "LOCAL_QUORUM", "EACH_QUORUM", "ALL"}));
        list.add(createDriverPropertyInfo(KEY_COMPRESSION, CassandraUtils
                        .getPropertyValue(props, KEY_COMPRESSION, DEFAULT_COMPRESSION),
                false, new String[]{"NONE", "SNAPPY", "LZ4"}));
        list.add(createDriverPropertyInfo(KEY_FETCH_SIZE, CassandraUtils
                        .getPropertyValue(props, KEY_FETCH_SIZE, DEFAULT_FETCH_SIZE),
                false, null));
        list.add(createDriverPropertyInfo(KEY_LOCAL_DC,
                CassandraUtils.getPropertyValue(props, KEY_LOCAL_DC), false,
                null));
        list.add(createDriverPropertyInfo(
                KEY_SQL_FRIENDLY,
                CassandraUtils.getPropertyValue(props, KEY_SQL_FRIENDLY,
                        String.valueOf(DEFAULT_SQL_FRIENDLY)), false,
                new String[]{"true", "false"}));
        list.add(createDriverPropertyInfo(KEY_QUERY_TRACE, CassandraUtils
                        .getPropertyValue(props, KEY_QUERY_TRACE, DEFAULT_QUERY_TRACE),
                false, new String[]{"true", "false"}));
        list.add(createDriverPropertyInfo(KEY_KEEP_ALIVE, CassandraUtils
                        .getPropertyValue(props, KEY_KEEP_ALIVE, DEFAULT_KEEP_ALIVE),
                false, new String[]{"true", "false"}));

        return Iterables.toArray(list, DriverPropertyInfo.class);
    }

    public boolean jdbcCompliant() {
        return false;
    }
}
