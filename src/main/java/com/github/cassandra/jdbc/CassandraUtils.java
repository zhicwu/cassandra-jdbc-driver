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

import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.cassandra.jdbc.parser.SqlToCqlTranslator;

/**
 * This is a utility class.
 *
 * @author Zhichun Wu
 */
public class CassandraUtils {
	static final String BUNDLE_NAME = CassandraUtils.class.getPackage()
			.getName() + ".messages";

	public static final String[][] CATALOG_COLUMNS = new String[][] { {
			"TABLE_CAT", "text" } };
	public static final String[][] COLUMN_COLUMNS = new String[][] {
			{ "TABLE_CAT", "text" }, { "TABLE_SCHEM", "text" },
			{ "TABLE_NAME", "text" }, { "COLUMN_NAME", "text" },
			{ "DATA_TYPE", "int" }, { "TYPE_NAME", "text" },
			{ "COLUMN_SIZE", "int" }, { "BUFFER_LENGTH", "int" },
			{ "DECIMAL_DIGITS", "int" }, { "NUM_PREC_RADIX", "int" },
			{ "NULLABLE", "int" }, { "REMARKS", "text" },
			{ "COLUMN_DEF", "text" }, { "SQL_DATA_TYPE", "int" },
			{ "SQL_DATETIME_SUB", "int" }, { "CHAR_OCTET_LENGTH", "int" },
			{ "ORDINAL_POSITION", "int" }, { "IS_NULLABLE", "text" },
			{ "SCOPE_CATALOG", "text" }, { "SCOPE_SCHEMA", "text" },
			{ "SCOPE_TABLE", "text" }, { "SOURCE_DATA_TYPE", "short" },
			{ "IS_AUTOINCREMENT", "text" }, { "IS_GENERATEDCOLUMN", "text" } };

	public static final String CURSOR_PREFIX = "cursor@";
	public static final String DEFAULT_COMPRESSION = "none";
	public static final String DEFAULT_CONNECT_TIMEOUT = "5000"; // 5 seconds
	public static final String DEFAULT_CONSISTENCY_LEVEL = "ONE";
	public static final String DEFAULT_DB_MAJOR_VERSION = "2";
	public static final String DEFAULT_DB_MINOR_VERSION = "0";
	public static final int DEFAULT_DRIVER_MAJOR_VERSION = 0;

	public static final int DEFAULT_DRIVER_MINOR_VERSION = 2;
	public static final String DEFAULT_DRIVER_NAME = "Cassandra JDBC Driver";

	public static final int DEFAULT_DRIVER_PATCH_VERSION = 0;
	public static final String DEFAULT_DRIVER_VERSION = DEFAULT_DRIVER_MAJOR_VERSION
			+ "."
			+ DEFAULT_DRIVER_MINOR_VERSION
			+ "."
			+ DEFAULT_DRIVER_PATCH_VERSION;
	public static final String DEFAULT_FETCH_SIZE = "100";
	public static final String DEFAULT_HOSTS = "localhost";
	public static final String DEFAULT_KEYSPACE = "system";
	public static final String DEFAULT_PRODUCT_NAME = "Apache Cassandra";
	public static final String DEFAULT_PRODUCT_VERSION = "2.x";
	// default settings
	public static final String DEFAULT_PROVIDER = "datastax";
	public static final String DEFAULT_QUERY_TRACE = "true";
	public static final String DEFAULT_READ_TIMEOUT = "30000"; // 30 seconds
	public static final long DEFAULT_ROW_LIMIT = 10000L;
	public static final boolean DEFAULT_SQL_FRIENDLY = true;

	public static final String DEFAULT_USERNAME = "cassandra";
	public static final String DRIVER_NAME = "Cassandra JDBC Driver";
	public static final String DRIVER_PROTOCOL = "jdbc:c*:";
	static final ResultSet DUMMY_RESULT_SET = new DummyCassandraResultSet();
	// really like String.Empty and String.IsNullOrEmpty() in C#...
	public static final String EMPTY_STRING = "";
	public static final String[][] INDEX_COLUMNS = new String[][] {
			{ "TABLE_CAT", "text" }, { "TABLE_SCHEM", "text" },
			{ "TABLE_NAME", "text" }, { "NON_UNIQUE", "boolean" },
			{ "INDEX_QUALIFIER", "text" }, { "INDEX_NAME", "text" },
			{ "TYPE", "int" }, { "ORDINAL_POSITION", "int" },
			{ "COLUMN_NAME", "text" }, { "ASC_OR_DESC", "text" },
			{ "CARDINALITY", "int" }, { "PAGES", "int" },
			{ "FILTER_CONDITION", "text" } };
	static final String INVALID_URL = "Invalid connection URL";
	public static final String KEY_APPROXIMATE_INDEX = "approximateIndexInfo";

	public static final String KEY_CATALOG = "catalog";
	public static final String KEY_COLUMN_PATTERN = "columnNamePattern";
	public static final String KEY_COMPRESSION = "compression";
	public static final String KEY_CONNECT_TIMEOUT = "connectTimeout";
	public static final String KEY_CONNECTION_URL = "url";
	public static final String KEY_CONSISTENCY_LEVEL = "consistencyLevel";
	public static final String KEY_DB_MAJOR_VERSION = "dbMajorVersion";

	public static final String KEY_DB_MINOR_VERSION = "dbMinorVersion";
	public static final String KEY_DRIVER_MAJOR_VERSION = "driverMajorVersion";

	public static final String KEY_DRIVER_MINOR_VERSION = "driverMinorVersion";

	public static final String KEY_DRIVER_NAME = "driverName";

	public static final String KEY_DRIVER_VERSION = "driverVersion";

	public static final String KEY_FETCH_SIZE = "fetchSize";

	public static final String KEY_HOSTS = "hosts";

	public static final String KEY_KEYSPACE = "keyspace";

	public static final String KEY_LOCAL_DC = "localDc";

	public static final String KEY_NUMERIC_FUNCTIONS = "numericFunctions";
	public static final String KEY_PASSWORD = "password";
	public static final String KEY_PORT = "port";
	public static final String KEY_PRODUCT_NAME = "productName";
	public static final String KEY_PRODUCT_VERSION = "productVersion";
	public static final String KEY_PROVIDER = "provider";
	public static final String KEY_QUERY_TRACE = "queryTrace";
	public static final String KEY_QUIET = "quiet";
	public static final String KEY_READ_TIMEOUT = "readTimeout";
	public static final String KEY_SCHEMA_PATTERN = "schemaPattern";
	public static final String KEY_SQL_FRIENDLY = "sqlFriendly";
	public static final String KEY_SQL_KEYWORDS = "keywords";

	public static final String KEY_STRING_FUNCTIONS = "stringFunctions";
	public static final String KEY_SYSTEM_FUNCTIONS = "systemFunctions";
	public static final String KEY_TABLE_PATTERN = "tableNamePattern";
	public static final String KEY_TIMEDATE_FUNCTIONS = "timeDateFunctions";
	public static final String KEY_TYPE_PATTERN = "typeNamePattern";
	public static final String KEY_UNIQUE_INDEX = "uniqueIndexOnly";
	public static final String KEY_USERNAME = "user";
	private static final Logger logger = LoggerFactory
			.getLogger(CassandraUtils.class);
	public static final String[][] PK_COLUMNS = new String[][] {
			{ "TABLE_CAT", "text" }, { "TABLE_SCHEM", "text" },
			{ "TABLE_NAME", "text" }, { "COLUMN_NAME", "text" },
			{ "KEY_SEQ", "int" }, { "PK_NAME", "text" } };

	static final String PROVIDER_PREFIX = CassandraUtils.class.getPackage()
			.getName() + ".provider.";

	static final String PROVIDER_SUFFIX = "CassandraConnection";

	static final ResourceBundle RESOURCE_BUNDLE;
	static final String SQL_KEYWORD_ESCAPING = ".\"$1\"$2";

	static final Pattern SQL_KEYWORDS_PATTERN = Pattern
			.compile("(?i)\\.(select|insert|update|delete|into|from|where|key|alter|drop|create)([>=<\\.,\\s])");
	public static final String[][] TABLE_COLUMNS = new String[][] {
			{ "TABLE_CAT", "text" }, { "TABLE_SCHEM", "text" },
			{ "TABLE_NAME", "text" }, { "TABLE_TYPE", "text" },
			{ "REMARKS", "text" }, { "TYPE_CAT", "text" },
			{ "TYPE_SCHEM", "text" }, { "TYPE_NAME", "text" },
			{ "SELF_REFERENCING_COL_NAME", "text" },
			{ "REF_GENERATION", "text" } };
	// meta data
	public static final String[][] TABLE_TYPE_COLUMNS = new String[][] { {
			"TABLE_TYPE", "text" } };
	public static final Object[][] TABLE_TYPE_DATA = new Object[][] { new Object[] { "TABLE" } };

	static final String TOKEN_KVP_SEPARATOR = "=";
	static final String TOKEN_PARAM_SEPARATOR = "&";

	static final String TOKEN_PROTO_SEPARATOR = ":";

	static final String TOKEN_URL_SEPARATOR = "//";

	public static final String[][] TYPE_COLUMNS = new String[][] {
			{ "TYPE_NAME", "text" }, { "DATA_TYPE", "int" },
			{ "PRECISION", "int" }, { "LITERAL_PREFIX", "text" },
			{ "LITERAL_SUFFIX", "text" }, { "CREATE_PARAMS", "text" },
			{ "NULLABLE", "int" }, { "CASE_SENSITIVE", "boolean" },
			{ "SEARCHABLE", "int" }, { "UNSIGNED_ATTRIBUTE", "boolean" },
			{ "FIXED_PREC_SCALE", "boolean" }, { "AUTO_INCREMENT", "boolean" },
			{ "LOCAL_TYPE_NAME", "text" }, { "MINIMUM_SCALE", "int" },
			{ "MAXIMUM_SCALE", "int" }, { "SQL_DATA_TYPE", "int" },
			{ "SQL_DATETIME_SUB", "int" }, { "NUM_PREC_RADIX", "int" } };

	public static final String[][] UDT_COLUMNS = new String[][] {
			{ "TYPE_CAT", "text" }, { "TYPE_SCHEM", "text" },
			{ "TYPE_NAME", "text" }, { "CLASS_NAME", "text" },
			{ "DATA_TYPE", "int" }, { "REMARKS", "text" },
			{ "BASE_TYPE", "int" } };

	static {
		ResourceBundle bundle = null;

		try {
			bundle = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(),
					CassandraUtils.class.getClassLoader());
		} catch (Throwable t) {
			try {
				bundle = ResourceBundle.getBundle(BUNDLE_NAME);
			} catch (Throwable e) {
				throw new RuntimeException(
						"Failed to load resource bundle due to underlying exception: "
								+ t.toString(), e);
			}
		} finally {
			RESOURCE_BUNDLE = bundle;
		}
	}

	public static String buildSimplifiedConnectionUrl(Properties props) {
		StringBuilder builder = new StringBuilder(DRIVER_PROTOCOL);
		builder.append(getPropertyValue(props, KEY_PROVIDER, DEFAULT_PROVIDER))
				.append(':')
				.append(TOKEN_URL_SEPARATOR)
				.append(getPropertyValue(props, KEY_HOSTS, DEFAULT_HOSTS))
				.append('/')
				.append(getPropertyValue(props, KEY_KEYSPACE, DEFAULT_KEYSPACE))
				.append('?').append(KEY_USERNAME).append('=')
				.append(getPropertyValue(props, KEY_USERNAME, EMPTY_STRING));

		return builder.toString();
	}

	static BaseCassandraConnection createConnection(Properties props)
			throws SQLException {
		BaseCassandraConnection conn;

		try {
			// FIXME needs a better way to isolate base class and implementation
			Class<?> clazz = CassandraUtils.class.getClassLoader().loadClass(
					new StringBuffer()
							.append(PROVIDER_PREFIX)
							.append(props.getProperty(KEY_PROVIDER,
									DEFAULT_PROVIDER)).append('.')
							.append(PROVIDER_SUFFIX).toString());
			Constructor<?> c = clazz.getConstructor(Properties.class);
			conn = (BaseCassandraConnection) c.newInstance(props);
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return conn;
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
	 * @param props
	 *            properties
	 * @param key
	 *            key for looking up the value
	 * @param defaultValue
	 *            default value
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
	 * @param props
	 *            properties
	 * @param key
	 *            key for looking up the value
	 * @param defaultValue
	 *            default value
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

	public static String normalizeSql(String sql, boolean translate,
			boolean quiet) throws SQLException {
		sql = isNullOrEmptyString(sql) ? EMPTY_STRING : sql.trim();

		if (translate) {
			try {
				// workaround for limitation of JSqlParser
				Matcher m = SQL_KEYWORDS_PATTERN.matcher(sql);

				// go ahead to parse the normalized SQL
				net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil
						.parse(m.replaceAll(SQL_KEYWORD_ESCAPING));
				if (s instanceof Select) {
					Select select = (Select) s;
					select.getSelectBody().accept(new SqlToCqlTranslator());
					sql = select.toString();
				} else {
					sql = s.toString();
				}
			} catch (Exception e) {
				if (!quiet) {
					throw new SQLException(e);
				} else {
					if (logger.isWarnEnabled()) {
						logger.warn(
								"Wasn't able to translate given SQL to CQL", e);
					}
				}
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug(new StringBuilder("Normalized SQL:\n").append(sql)
					.toString());
		}

		return sql;
	}

	/**
	 * Extract properties from given non-null connection URL.
	 *
	 * @param url
	 *            connection URL
	 * @return properties defined in the URL
	 * @throws SQLException
	 *             when the given URL is invalid
	 */
	static Properties parseConnectionURL(String url) throws SQLException {
		Properties props = new Properties();

		// example URL:
		// jdbc:c*:datastax://host1:9160,host2/keyspace1?consistency=LOCAL_ONE
		String[] parts = url.split(TOKEN_URL_SEPARATOR);
		boolean invalidUrl = true;

		if (parts.length == 2) {
			// get provider
			String provider = parts[0].replace(DRIVER_PROTOCOL, EMPTY_STRING);
			if (EMPTY_STRING.equals(provider)) {
				provider = DEFAULT_PROVIDER;
			} else {
				// this will also ignore extra protocol codes like ":a:b:c:d:"
				provider = provider.split(TOKEN_PROTO_SEPARATOR)[0];
			}
			props.setProperty(KEY_PROVIDER, provider);

			String restUrl = parts[1];
			int ksIdx = restUrl.indexOf('/');
			int pIdx = restUrl.indexOf('?');
			if (ksIdx > 0) {
				// get hosts
				String hosts = restUrl.substring(0, ksIdx);
				props.setProperty(KEY_HOSTS, hosts);

				// get keyspace
				String keyspace = restUrl.substring(ksIdx + 1,
						pIdx > ksIdx ? pIdx : restUrl.length());
				if (EMPTY_STRING.equals(keyspace)) {
					keyspace = DEFAULT_KEYSPACE;
				}
				props.setProperty(KEY_KEYSPACE, keyspace);
			} else {
				props.setProperty(KEY_KEYSPACE, DEFAULT_KEYSPACE);
				props.setProperty(KEY_HOSTS,
						pIdx > 0 ? restUrl.substring(0, pIdx) : restUrl);
			}

			invalidUrl = false;

			// now let's see if there's any optional parameters
			if (pIdx > ksIdx) {
				String[] params = restUrl.substring(pIdx + 1, restUrl.length())
						.split(TOKEN_PARAM_SEPARATOR);
				for (String param : params) {
					String[] kvPair = param.split(TOKEN_KVP_SEPARATOR);
					if (kvPair.length == 2) {
						String key = kvPair[0].trim();
						String value = kvPair[1].trim();

						if (!EMPTY_STRING.equals(key)) {
							props.setProperty(key, value);
						}
					}
				}
			}
		}

		if (invalidUrl) {
			throw new SQLException(INVALID_URL);
		}

		return props;
	}

	public static SQLException tryClose(AutoCloseable resource) {
		SQLException exception = null;

		if (resource != null) {
			String resourceName = new StringBuilder(resource.getClass()
					.getName()).append('@').append(resource.hashCode())
					.toString();

			if (logger.isDebugEnabled()) {
				logger.debug(new StringBuilder("Trying to close [")
						.append(resourceName).append(']').toString());
			}

			try {
				resource.close();
				if (logger.isDebugEnabled()) {
					logger.debug(new StringBuilder().append("[")
							.append(resourceName)
							.append("] closed successfully").toString());
				}
			} catch (Throwable t) {
				exception = CassandraErrors.failedToCloseResourceException(
						resourceName, t);

				if (logger.isWarnEnabled()) {
					logger.warn(
							new StringBuilder("Error occurred when closing [")
									.append(resourceName).append("]")
									.toString(), t);
				}
			}
		}

		return exception;
	}

	private CassandraUtils() {
	}
}
