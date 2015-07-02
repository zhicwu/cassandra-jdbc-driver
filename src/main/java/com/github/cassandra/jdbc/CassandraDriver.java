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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraUtils.*;

/**
 * Cassandra JDBC driver.
 *
 * @author Zhichun Wu
 */
public class CassandraDriver implements Driver {
	public static final int VERSION_MAJOR = 0;
	public static final int VERSION_MINOR = 1;
	public static final int VERSION_PATCH = 0;

	private static final Logger logger = LoggerFactory
			.getLogger(CassandraDriver.class);

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

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
			throws SQLException {
		if (props == null) {
			props = new Properties();
		}

		DriverPropertyInfo[] info = new DriverPropertyInfo[2];

		info[0] = new DriverPropertyInfo(KEY_USERNAME,
				props.getProperty(KEY_USERNAME));
		info[0].description = "The 'user' property";

		info[1] = new DriverPropertyInfo(KEY_PASSWORD,
				props.getProperty(KEY_PASSWORD));
		info[1].description = "The 'password' property";

		return info;
	}

	public int getMajorVersion() {
		return VERSION_MAJOR;
	}

	public int getMinorVersion() {
		return VERSION_MINOR;
	}

	public boolean jdbcCompliant() {
		return false;
	}

	public java.util.logging.Logger getParentLogger()
			throws SQLFeatureNotSupportedException {
		throw CassandraErrors.notSupportedException();
	}
}
