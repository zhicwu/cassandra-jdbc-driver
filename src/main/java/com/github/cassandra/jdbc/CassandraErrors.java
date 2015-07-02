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

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Helper class defines unified Cassandra errors regardless the ones from
 * underlying driver.
 *
 * @author Zhichun Wu
 */
public final class CassandraErrors {
	public static final int ERROR_CODE_GENERAL = -1;
	static final String BUNDLE_NAME = CassandraErrors.class.getPackage()
			.getName() + ".messages";
	static final ResourceBundle RESOURCE_BUNDLE;

	static {
		ResourceBundle bundle = null;

		//
		// Overly-pedantic here, some appserver and JVM combos don't deal
		// well with the no-args version, others don't deal well with
		// the three-arg version, so we need to try both :(
		//

		try {
			bundle = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(),
					CassandraErrors.class.getClassLoader());
		} catch (Throwable t) {
			try {
				bundle = ResourceBundle.getBundle(BUNDLE_NAME);
			} catch (Throwable e) {
				throw new RuntimeException(
						"Failed to load resource bundle due to underlying exception "
								+ t.toString(), e);
			}
		} finally {
			RESOURCE_BUNDLE = bundle;
		}
	}

	/**
	 * Returns the localized message for the given message key
	 *
	 * @param key
	 *            the message key
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

	public static SQLFeatureNotSupportedException notSupportedException() {
		return new SQLFeatureNotSupportedException(
				getString("EXCEPTION_NOT_SUPPORTED"), null, ERROR_CODE_GENERAL);
	}

	public static SQLException resourceClosedException(Object obj) {
		return new SQLException(getString("EXCEPTION_RESOURCE_CLOSED", obj),
				null, ERROR_CODE_GENERAL);
	}

	public static SQLException connectionClosedException() {
		return new SQLException(getString("EXCEPTION_CONNECTION_CLOSED"), null,
				ERROR_CODE_GENERAL);
	}

	public static SQLException failedToCloseResourceException(
			String resourceName, Throwable cause) {
		return new SQLException(getString("EXCEPTION_FAILED_TO_CLOSE_RESOURCE",
				resourceName), null, ERROR_CODE_GENERAL, cause);
	}

	public static SQLException failedToCloseConnectionException(Exception cause) {
		return new SQLException(
				getString("EXCEPTION_FAILED_TO_CLOSE_CONNECTION"), null,
				ERROR_CODE_GENERAL, cause);
	}

	public static SQLException databaseMetaDataNotAvailableException() {
		return new SQLException(
				getString("EXCEPTION_DATABASE_METADATA_NOT_AVAILABLE"), null,
				ERROR_CODE_GENERAL);
	}

	public static SQLException invalidKeyspaceException(String keyspace) {
		return new SQLException(getString("EXCEPTION_INVALID_KEYSPACE",
				new Object[] { keyspace }), null, ERROR_CODE_GENERAL);
	}

	public static SQLException failedToChangeKeyspaceException(String keyspace,
			Exception cause) {
		return new SQLException(getString(
				"EXCEPTION_FAILED_TO_CHANGE_KEYSPACE",
				new Object[] { keyspace }), null, ERROR_CODE_GENERAL, cause);
	}

	public static SQLException statementClosedException() {
		return new SQLException(getString("EXCEPTION_STATEMENT_CLOSED"), null,
				ERROR_CODE_GENERAL);
	}

	public static SQLException resultSetClosed() {
		return new SQLException(getString("EXCEPTION_RESULTSET_CLOSED"), null,
				ERROR_CODE_GENERAL);
	}
}
