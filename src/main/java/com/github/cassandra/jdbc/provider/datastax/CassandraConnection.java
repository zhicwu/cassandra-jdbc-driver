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
package com.github.cassandra.jdbc.provider.datastax;

import static com.github.cassandra.jdbc.CassandraUtils.CATALOG_COLUMNS;
import static com.github.cassandra.jdbc.CassandraUtils.COLUMN_COLUMNS;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_COMPRESSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_CONNECT_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_CONSISTENCY_LEVEL;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_FETCH_SIZE;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_READ_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;
import static com.github.cassandra.jdbc.CassandraUtils.INDEX_COLUMNS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_APPROXIMATE_INDEX;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CATALOG;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_COLUMN_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_COMPRESSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONNECT_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONSISTENCY_LEVEL;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DB_MAJOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DB_MINOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_MAJOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_MINOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_NAME;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_FETCH_SIZE;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_HOSTS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_KEYSPACE;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_LOCAL_DC;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PASSWORD;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PORT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PRODUCT_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_READ_TIMEOUT;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_TABLE_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_TYPE_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_UNIQUE_INDEX;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;
import static com.github.cassandra.jdbc.CassandraUtils.PK_COLUMNS;
import static com.github.cassandra.jdbc.CassandraUtils.TABLE_COLUMNS;
import static com.github.cassandra.jdbc.CassandraUtils.UDT_COLUMNS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ColumnMetadata.IndexMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.github.cassandra.jdbc.BaseCassandraConnection;
import com.github.cassandra.jdbc.CassandraDataTypeMappings;
import com.github.cassandra.jdbc.CassandraErrors;
import com.github.cassandra.jdbc.CassandraObjectType;
import com.github.cassandra.jdbc.CassandraUtils;
import com.github.cassandra.jdbc.DummyCassandraResultSet;

/**
 * This is a connection implementation built on top of DataStax Java driver.
 *
 * @author Zhichun Wu
 */
public class CassandraConnection extends BaseCassandraConnection {
	static final String DRIVER_NAME = "DataStax Java Driver";
	static final String CQL_TO_GET_VERSION = "select release_version from system.local where key='local' limit 1";

	private static final Logger logger = LoggerFactory
			.getLogger(CassandraConnection.class);

	private Cluster _cluster;
	private String _keyspace;

	public CassandraConnection(Properties props) {
		super(props);

		String url = CassandraUtils.buildSimplifiedConnectionUrl(props);

		if (logger.isDebugEnabled()) {
			logger.debug(new StringBuilder().append("Connecting to [")
					.append(url).append("]...").toString());
		}

		Builder builder = Cluster.builder();

		// add contact points
		String[] hosts = props.getProperty(KEY_HOSTS).split(",");
		for (String host : hosts) {
			builder.addContactPoint(host);
		}

		// set port if specified
		if (props.containsKey(KEY_PORT)) {
			builder.withPort(Integer.parseInt(props.getProperty(KEY_PORT)));
		}

		// set timeouts
		SocketOptions socketOptions = new SocketOptions();
		socketOptions.setConnectTimeoutMillis(Integer.valueOf(props
				.getProperty(KEY_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT)));
		socketOptions.setReadTimeoutMillis(Integer.valueOf(props.getProperty(
				KEY_READ_TIMEOUT, DEFAULT_READ_TIMEOUT)));
		builder.withSocketOptions(socketOptions);

		// set query options
		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setConsistencyLevel(ConsistencyLevel.valueOf(props
				.getProperty(KEY_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL)
				.toUpperCase()));
		queryOptions.setFetchSize(Integer.valueOf(props.getProperty(
				KEY_FETCH_SIZE, DEFAULT_FETCH_SIZE)));
		builder.withQueryOptions(queryOptions);

		String localDc = props.getProperty(KEY_LOCAL_DC, EMPTY_STRING);
		if (!CassandraUtils.isNullOrEmptyString(localDc)) {
			builder.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(localDc));
		}

		// set compression
		builder.withCompression(Compression.valueOf(props.getProperty(
				KEY_COMPRESSION, DEFAULT_COMPRESSION).toUpperCase()));

		// build the cluster
		_cluster = builder.withCredentials(props.getProperty(KEY_USERNAME),
				props.getProperty(KEY_PASSWORD)).build();

		if (logger.isDebugEnabled()) {
			logger.debug(new StringBuilder().append("Connected to [")
					.append(url).append('(').append(_cluster.hashCode())
					.append(')').append("] successfully").toString());
		}

		if (logger.isInfoEnabled()) {
			Metadata metadata = _cluster.getMetadata();

			logger.info(new StringBuilder("Connected to cluster: ").append(
					metadata.getClusterName()).toString());
			for (Host host : metadata.getAllHosts()) {
				logger.info(new StringBuilder("-> Datacenter: ")
						.append(host.getDatacenter()).append("; Host: ")
						.append(host.getAddress()).append("; Rack: ")
						.append(host.getRack()).toString());
			}
		}

		// populate meta data
		metaData.setProperty(KEY_DRIVER_NAME, DRIVER_NAME);

		String driverVersion = Cluster.getDriverVersion();
		String[] versions = driverVersion.split(".");
		metaData.setProperty(KEY_DRIVER_VERSION, driverVersion);
		if (versions != null && versions.length > 1) {
			metaData.setProperty(KEY_DRIVER_MAJOR_VERSION, versions[0]);
			metaData.setProperty(KEY_DRIVER_MINOR_VERSION, versions[1]);
		}

		// try if the given keyspace is valid
		_keyspace = props.getProperty(KEY_KEYSPACE);

		// test if we can access the keyspace and try to get database version
		String dbVersion = touchKeyspace(_keyspace, true);
		if (!CassandraUtils.isNullOrEmptyString(dbVersion)) {
			metaData.setProperty(KEY_PRODUCT_VERSION, dbVersion);
			versions = dbVersion.split(".");
			if (versions != null && versions.length > 1) {
				metaData.setProperty(KEY_DB_MAJOR_VERSION, versions[0]);
				metaData.setProperty(KEY_DB_MINOR_VERSION, versions[1]);
			}
		}
	}

	@Override
	protected Object unwrap() {
		return _cluster;
	}

	@Override
	protected void validateState() throws SQLException {
		super.validateState();

		// this might happen if someone close the connection directly without
		// touching JDBC API
		if (_cluster.isClosed()) {
			_cluster = null;
			throw CassandraErrors.connectionClosedException();
		}
	}

	@Override
	protected <T> T createObject(Class<T> clazz) throws SQLException {
		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return null;
	}

	@Override
	protected SQLException tryClose() {
		SQLException e = CassandraUtils.tryClose(_cluster);
		_cluster = null;

		return e;
	}

	private Object[] populateTableMetaData(KeyspaceMetadata ks, TableMetadata t) {
		return new Object[] {
				ks.getName(), // TABLE_CAT
				null, // TABLE_SCHEM
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

	private Object[] populateColumnMetaData(KeyspaceMetadata ks,
			TableMetadata t, ColumnMetadata c, int colIndex) {
		String cqlType = c.getType().toString().toLowerCase();

		return new Object[] { ks.getName(), // TABLE_CAT
				null, // TABLE_SCHEM
				t.getName(), // TABLE_NAME
				c.getName(), // COLUMN_NAME
				CassandraDataTypeMappings // DATA_TYPE
						.sqlTypeFor(cqlType), cqlType, // TYPE_NAME
				0, // COLUMN_SIZE
				0, // BUFFER_LENGTH
				0, // DECIMAL_DIGITS
				10, // NUM_PREC_RADIX
				java.sql.DatabaseMetaData.typeNullable, // NULLABLE
				null, // REMARKS
				null, // COLUMN_DEF
				0, // SQL_DATA_TYPE
				0, // SQL_DATETIME_SUB
				0, // CHAR_OCTET_LENGTH
				colIndex, // ORDINAL_POSITION
				"YES", // IS_NULLABLE
				null, // SCOPE_CATALOG
				null, // SCOPE_SCHEMA
				null, // SCOPE_TABLE
				null, // SOURCE_DATA_TYPE
				"NO", // IS_AUTOINCREMENT
				"solr_query".equals(c.getName()) ? "YES" : "NO" // IS_GENERATEDCOLUMN

		};
	}

	private Object[] populateIndexMetaData(KeyspaceMetadata ks,
			TableMetadata t, IndexMetadata i) {
		return new Object[] { ks.getName(), // TABLE_CAT
				null, // TABLE_SCHEM
				t.getName(), // TABLE_NAME
				false, // NON_UNIQUE
				EMPTY_STRING, // INDEX_QUALIFIER
				i.getName(), // INDEX_NAME
				java.sql.DatabaseMetaData.tableIndexOther, // TYPE
				1, // ORDINAL_POSITION
				i.getIndexedColumn().getName(), // COLUMN_NAME
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
			TableMetadata t, ColumnMetadata pk, int colIndex, boolean unique) {
		return new Object[] { ks.getName(), // TABLE_CAT
				null, // TABLE_SCHEM
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

	private Object[] populatePrimaryKeyMetaData(KeyspaceMetadata ks,
			TableMetadata t, ColumnMetadata c, int colIndex) {
		return new Object[] { ks.getName(), // TABLE_CAT
				null, // TABLE_SCHEM
				t.getName(), // TABLE_NAME
				c.getName(), // COLUMN_NAME
				colIndex, // KEY_SEQ
				"PRIMARY" // PK_NAME
		};
	}

	private Object[] populateUdtMetaData(KeyspaceMetadata ks, UserType t) {
		return new Object[] { ks.getName(), // TYPE_CAT
				null, // TABLE_SCHEM
				t.getTypeName(), // TYPE_NAME
				t.getCustomTypeClassName(), // CLASS_NAME
				java.sql.Types.JAVA_OBJECT, // DATA_TYPE
				t.getName().toString(), // REMARKS
				null // BASE_TYPE
		};
	}

	private ResultSet buildResultSet(String[][] columns, List<Object[]> list) {
		Object[][] data = new Object[list.size()][];
		int index = 0;
		for (Object[] objs : list) {
			data[index++] = objs;
		}

		return new DummyCassandraResultSet(columns, data);
	}

	@Override
	protected ResultSet getObjectMetaData(CassandraObjectType objectType,
			Properties queryPatterns, Object... additionalHints) {
		if (logger.isTraceEnabled()) {
			logger.trace(new StringBuilder()
					.append("Trying to get meta data with the following parameters:\n")
					.append("objectType: ").append(objectType.name())
					.append("\nqueryPatterns:\n").append(queryPatterns)
					.append("\nadditionalHints: ")
					.append(Arrays.toString(additionalHints)).toString());
		}

		ResultSet rs = new DummyCassandraResultSet();

		Metadata m = _cluster.getMetadata();
		switch (objectType) {
		case KEYSPACE: {
			List<KeyspaceMetadata> keyspaces = m.getKeyspaces();
			String[][] data = new String[keyspaces.size()][1];
			int index = 0;
			for (KeyspaceMetadata km : keyspaces) {
				data[index++][0] = km.getName();
			}

			rs = new DummyCassandraResultSet(CATALOG_COLUMNS, data);
			break;
		}

		case TABLE: {
			String catalog = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_CATALOG);
			String tablePattern = CassandraUtils.getPropertyValue(
					queryPatterns, KEY_TABLE_PATTERN);

			List<Object[]> list = new ArrayList<Object[]>();
			for (KeyspaceMetadata ks : m.getKeyspaces()) {
				if (CassandraUtils.matchesPattern(ks.getName(), catalog)) {
					for (TableMetadata t : ks.getTables()) {
						if (CassandraUtils.matchesPattern(t.getName(),
								tablePattern)) {
							list.add(populateTableMetaData(ks, t));
						}
					}
				}
			}

			rs = buildResultSet(TABLE_COLUMNS, list);
			break;
		}

		case COLUMN: {
			String catalog = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_CATALOG);
			String tablePattern = CassandraUtils.getPropertyValue(
					queryPatterns, KEY_TABLE_PATTERN);
			String columnPattern = CassandraUtils.getPropertyValue(
					queryPatterns, KEY_COLUMN_PATTERN);

			List<Object[]> list = new ArrayList<Object[]>();
			for (KeyspaceMetadata ks : m.getKeyspaces()) {
				if (CassandraUtils.isNullOrEmptyString(catalog)
						|| catalog.equals(ks.getName())) {
					for (TableMetadata t : ks.getTables()) {
						if (CassandraUtils.matchesPattern(t.getName(),
								tablePattern)) {
							int colIndex = 0;
							for (ColumnMetadata c : t.getColumns()) {
								colIndex++;
								// Why DataStax Java driver turned additional
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
			String catalog = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_CATALOG);
			String table = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_TABLE_PATTERN);
			boolean uniqueIndexOnly = Boolean.valueOf(CassandraUtils
					.getPropertyValue(queryPatterns, KEY_UNIQUE_INDEX,
							Boolean.FALSE.toString()));
			boolean approximateIndex = Boolean.valueOf(CassandraUtils
					.getPropertyValue(queryPatterns, KEY_APPROXIMATE_INDEX,
							Boolean.TRUE.toString()));

			List<Object[]> list = new ArrayList<Object[]>();
			for (KeyspaceMetadata ks : m.getKeyspaces()) {
				if (CassandraUtils.isNullOrEmptyString(catalog)
						|| catalog.equals(ks.getName())) {
					for (TableMetadata t : ks.getTables()) {
						if (CassandraUtils.isNullOrEmptyString(table)
								|| table.equals(t.getName())) {
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

							for (ColumnMetadata c : t.getColumns()) {
								IndexMetadata i = c.getIndex();
								if (i != null && !primaryKeys.contains(c)) {
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
			String catalog = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_CATALOG);
			String table = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_TABLE_PATTERN);

			List<Object[]> list = new ArrayList<Object[]>();
			for (KeyspaceMetadata ks : m.getKeyspaces()) {
				if (CassandraUtils.isNullOrEmptyString(catalog)
						|| catalog.equals(ks.getName())) {
					for (TableMetadata t : ks.getTables()) {
						if (CassandraUtils.isNullOrEmptyString(table)
								|| table.equals(t.getName())) {
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
			String catalog = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_CATALOG);
			String typePattern = CassandraUtils.getPropertyValue(queryPatterns,
					KEY_TYPE_PATTERN);

			List<Object[]> list = new ArrayList<Object[]>();
			for (KeyspaceMetadata ks : m.getKeyspaces()) {
				if (CassandraUtils.isNullOrEmptyString(catalog)
						|| catalog.equals(ks.getName())) {
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

		if (logger.isTraceEnabled()) {
			int rowCount = 0;
			int colCount = 0;

			if (rs instanceof DummyCassandraResultSet) {
				DummyCassandraResultSet drs = (DummyCassandraResultSet) rs;

				rowCount = drs.getRowCount();
				colCount = drs.getColumnCount();
			}

			logger.trace(new StringBuilder().append("Returning results with ")
					.append(rowCount).append(" x ").append(colCount)
					.append(" two-dimensional array").toString());
		}

		return rs;
	}

	protected String touchKeyspace(String keyspace, boolean getDbVersion) {
		String dbVersion = EMPTY_STRING;

		// check if the given schema is valid
		Session session = null;
		try {
			session = _cluster.connect(keyspace);

			if (getDbVersion) {
				Row row = session.execute(CQL_TO_GET_VERSION).one();
				if (row != null) {
					dbVersion = row.getString(0);
				}
			}
		} catch (RuntimeException e) {
			throw e;
		} finally {
			if (session != null) {
				try {
					session.closeAsync().force();
				} catch (Exception e) {
					if (logger.isWarnEnabled()) {
						logger.warn("Failed to close session", e);
					}
				}
			}

			session = null;
		}

		return dbVersion;
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		validateState();

		return new CassandraStatement(this, _cluster.connect(_keyspace));
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		validateState();

		return new CassandraPreparedStatement(this, _cluster.connect(_keyspace));
	}

	public String getCatalog() throws SQLException {
		validateState();

		return _keyspace;
	}

	public void setCatalog(String catalog) throws SQLException {
		validateState();

		if (CassandraUtils.isNullOrEmptyString(catalog)
				|| catalog.equals(_keyspace)) {
			return;
		}

		try {
			touchKeyspace(catalog, false);

			if (logger.isDebugEnabled()) {
				logger.debug(new StringBuilder(
						"Current keyspace changed from \"").append(_keyspace)
						.append("\" to \"").append(catalog)
						.append("\" successfully").toString());
			}

			_keyspace = catalog;
		} catch (Exception e) {
			throw CassandraErrors.failedToChangeKeyspaceException(catalog, e);
		}
	}
}
