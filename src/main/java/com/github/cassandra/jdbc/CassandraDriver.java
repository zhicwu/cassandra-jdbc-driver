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

import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_COMPRESSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_CONNECT_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_CONSISTENCY_LEVEL;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_FETCH_SIZE;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_QUERY_TRACE;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_READ_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_SQL_FRIENDLY;
import static com.github.cassandra.jdbc.CassandraUtils.DRIVER_PROTOCOL;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_COMPRESSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONNECT_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONSISTENCY_LEVEL;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_FETCH_SIZE;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_LOCAL_DC;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PASSWORD;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PORT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_QUERY_TRACE;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_READ_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_SQL_FRIENDLY;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra JDBC driver.
 *
 * @author Zhichun Wu
 */
public class CassandraDriver implements Driver {
	private static final Logger logger = LoggerFactory
			.getLogger(CassandraDriver.class);
	public static final int VERSION_MAJOR = 0;
	public static final int VERSION_MINOR = 1;

	public static final int VERSION_PATCH = 0;

	static {
		// Register the CassandraDriver with DriverManager
		try {
			CassandraDriver driver = new CassandraDriver();
			DriverManager.registerDriver(driver);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean acceptsURL(String url) throws SQLException {
		return url != null && url.startsWith(DRIVER_PROTOCOL);
	}

	public Connection connect(String url, Properties props) throws SQLException {
		Properties connProps;
		if (acceptsURL(url)) {
			// parse the URL into a set of Properties
			connProps = CassandraUtils.parseConnectionURL(url);

			// override any matching values in connProps with values from props
			connProps.putAll(props);

			if (logger.isDebugEnabled()) {
				logger.debug("Connection Properties: {}", connProps);
			}

			// load concrete Cassandra driver based on the properties
			return CassandraUtils.createConnection(connProps);
		} else {
			// signal it is the wrong driver for this
			return null;
		}
	}

	private DriverPropertyInfo createDriverPropertyInfo(String propertyName,
			String propertyValue, boolean required, String[] choices) {
		DriverPropertyInfo info = new DriverPropertyInfo(propertyName,
				propertyValue);
		info.required = required;
		info.description = CassandraUtils.getString(new StringBuilder(
				"MESSAGE_PROP_").append(propertyName.toUpperCase())
				.append("_DESCRIPTION").toString());

		if (choices != null && choices.length > 0) {
			info.choices = choices;
		}

		return info;
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
						DEFAULT_CONSISTENCY_LEVEL), false, new String[] {
				"ONE", "LOCAL_ONE", "QUORUM", "LOCAL_QUORUM", "EACH_QUORUM",
				"ALL" }));
		list.add(createDriverPropertyInfo(KEY_COMPRESSION, CassandraUtils
				.getPropertyValue(props, KEY_COMPRESSION, DEFAULT_COMPRESSION),
				false, new String[] { "NONE", "SNAPPY", "LZ4" }));
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
				new String[] { "true", "false" }));
		list.add(createDriverPropertyInfo(KEY_QUERY_TRACE, CassandraUtils
				.getPropertyValue(props, KEY_QUERY_TRACE, DEFAULT_QUERY_TRACE),
				false, new String[] { "true", "false" }));

		return list.toArray(new DriverPropertyInfo[0]);
	}

	public boolean jdbcCompliant() {
		return false;
	}
}
