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
package com.github.cassandra.jdbcx;

import com.github.cassandra.jdbc.BaseJdbcObject;
import com.github.cassandra.jdbc.CassandraConfiguration;
import com.github.cassandra.jdbc.CassandraDriver;
import com.github.cassandra.jdbc.CassandraErrors;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraConfiguration.KEY_CONNECTION_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;

public class CassandraDataSource extends BaseJdbcObject implements DataSource {
    private static final CassandraDriver driver = new CassandraDriver();

    private CassandraConfiguration config;
    private boolean configChanged;

    private final Properties connectionProps;

    protected CassandraConfiguration getConfig() throws SQLException {
        if (configChanged || config == null) {
            config = new CassandraConfiguration(EMPTY_STRING, connectionProps);
        }

        return config;
    }

    @Override
    protected SQLException tryClose() {
        return null;
    }

    @Override
    protected Object unwrap() {
        return this;
    }

    public CassandraDataSource() {
        super(true);

        connectionProps = new Properties();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(getConfig().getConnectionUrl(), connectionProps);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return driver.connect(getConfig().getConnectionUrl(), connectionProps);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        if (seconds > 0) {
            connectionProps.setProperty(KEY_CONNECTION_TIMEOUT, String.valueOf(seconds * 1000));
            configChanged = true;
        }
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw CassandraErrors.notSupportedException();
    }
}
