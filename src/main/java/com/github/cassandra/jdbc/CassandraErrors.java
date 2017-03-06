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

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Helper class defines unified Cassandra errors regardless the ones from
 * underlying driver.
 *
 * @author Zhichun Wu
 */
public final class CassandraErrors {
    public static final int ERROR_CODE_GENERAL = -1;

    public static SQLException connectionClosedException() {
        return new SQLException(
                CassandraUtils.getString("EXCEPTION_CONNECTION_CLOSED"), null,
                ERROR_CODE_GENERAL);
    }

    public static SQLException databaseMetaDataNotAvailableException() {
        return new SQLException(
                CassandraUtils
                        .getString("EXCEPTION_DATABASE_METADATA_NOT_AVAILABLE"),
                null, ERROR_CODE_GENERAL);
    }

    public static SQLException failedToChangeKeyspaceException(String keyspace,
                                                               Exception cause) {
        return new SQLException(CassandraUtils.getString(
                "EXCEPTION_FAILED_TO_CHANGE_KEYSPACE",
                new Object[]{keyspace}), null, ERROR_CODE_GENERAL, cause);
    }

    public static SQLException failedToCloseConnectionException(Exception cause) {
        return new SQLException(
                CassandraUtils
                        .getString("EXCEPTION_FAILED_TO_CLOSE_CONNECTION"),
                null, ERROR_CODE_GENERAL, cause);
    }

    public static SQLException failedToCloseResourceException(
            String resourceName, Throwable cause) {
        return new SQLException(CassandraUtils.getString(
                "EXCEPTION_FAILED_TO_CLOSE_RESOURCE", resourceName), null,
                ERROR_CODE_GENERAL, cause);
    }

    public static SQLException invalidKeyspaceException(String keyspace) {
        return new SQLException(CassandraUtils.getString(
                "EXCEPTION_INVALID_KEYSPACE", new Object[]{keyspace}), null,
                ERROR_CODE_GENERAL);
    }

    public static SQLException invalidQueryException(String query) {
        return new SQLException(CassandraUtils.getString(
                "EXCEPTION_INVALID_QUERY", new Object[]{query}), null,
                ERROR_CODE_GENERAL);
    }

    public static SQLFeatureNotSupportedException notSupportedException() {
        return new SQLFeatureNotSupportedException(
                CassandraUtils.getString("EXCEPTION_NOT_SUPPORTED"), null,
                ERROR_CODE_GENERAL);
    }

    public static SQLException resourceClosedException(Object obj) {
        return new SQLException(CassandraUtils.getString(
                "EXCEPTION_RESOURCE_CLOSED", obj), null, ERROR_CODE_GENERAL);
    }

    public static SQLException resultSetClosed() {
        return new SQLException(
                CassandraUtils.getString("EXCEPTION_RESULTSET_CLOSED"), null,
                ERROR_CODE_GENERAL);
    }

    public static SQLException statementClosedException() {
        return new SQLException(
                CassandraUtils.getString("EXCEPTION_STATEMENT_CLOSED"), null,
                ERROR_CODE_GENERAL);
    }

    public static IllegalStateException unexpectedException(Throwable cause) {
        return new IllegalStateException(CassandraUtils.getString("EXCEPTION_UNEXPECTED"), cause);
    }
}
