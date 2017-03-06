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

import com.google.common.collect.Iterables;

import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Cassandra JDBC driver.
 *
 * @author Zhichun Wu
 */
public class CassandraDriver implements Driver {
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

    private DriverPropertyInfo createDriverPropertyInfo(String propertyName, Object propertyValue) {
        Class propertyClass = propertyValue == null ? Object.class : propertyValue.getClass();

        DriverPropertyInfo info = new DriverPropertyInfo(propertyName,
                String.valueOf(propertyValue));
        info.required = false;
        info.description = CassandraUtils.getString(new StringBuilder(
                MSG_PREFIX).append(propertyName.toUpperCase())
                .append(MSG_SUFFIX).toString());

        if (propertyClass.isEnum()) {
            Object[] values = propertyClass.getEnumConstants();

            int len = values == null ? 0 : values.length;
            if (len > 0) {
                String[] choices = new String[len];
                for (int i = 0; i < len; i++) {
                    choices[i] = String.valueOf(values[i]);
                }

                info.choices = choices;
            }
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
        return CassandraConfiguration.VERSION_MAJOR;
    }

    public int getMinorVersion() {
        return CassandraConfiguration.VERSION_MINOR;
    }

    public java.util.logging.Logger getParentLogger()
            throws SQLFeatureNotSupportedException {
        throw CassandraErrors.notSupportedException();
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
            throws SQLException {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();

        for (Map.Entry<String, Object> entry : CassandraConfiguration.DEFAULT.toSortedMap().entrySet()) {
            String key = entry.getKey();
            list.add(createDriverPropertyInfo(key, entry.getValue()));
        }

        return Iterables.toArray(list, DriverPropertyInfo.class);
    }

    public boolean jdbcCompliant() {
        return false;
    }
}
