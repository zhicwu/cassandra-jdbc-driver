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

import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_DB_MAJOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_DB_MINOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_DRIVER_MAJOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_DRIVER_MINOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_DRIVER_NAME;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_DRIVER_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_PRODUCT_NAME;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_PRODUCT_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.DEFAULT_USERNAME;
import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_APPROXIMATE_INDEX;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CATALOG;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_COLUMN_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_CONNECTION_URL;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DB_MAJOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DB_MINOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_MAJOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_MINOR_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_NAME;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_DRIVER_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_NUMERIC_FUNCTIONS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PRODUCT_NAME;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_PRODUCT_VERSION;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_SCHEMA_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_SQL_KEYWORDS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_STRING_FUNCTIONS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_SYSTEM_FUNCTIONS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_TABLE_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_TIMEDATE_FUNCTIONS;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_TYPE_PATTERN;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_UNIQUE_INDEX;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This represents meta data of the connected Cassandra database.
 *
 * @author Zhichun Wu
 */
public class CassandraDatabaseMetaData extends BaseJdbcObject implements
		DatabaseMetaData {
	private final BaseCassandraConnection _conn;
	private final Properties _props;

	protected CassandraDatabaseMetaData(BaseCassandraConnection conn) {
		super(conn == null || conn.quiet);

		_conn = conn;
		_props = new Properties();
	}

	@Override
	protected SQLException tryClose() {
		return null;
	}

	@Override
	protected Object unwrap() {
		return this;
	}

	protected void validateState() throws SQLException {
		if (_conn == null || _conn.isClosed()) {
			throw CassandraErrors.connectionClosedException();
		}
	}

	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	public boolean allTablesAreSelectable() throws SQLException {
		return false;
	}

	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return true;
	}

	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return true;
	}

	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}

	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getCatalogs() throws SQLException {
		validateState();

		return _conn.getObjectMetaData(CassandraObjectType.KEYSPACE, null);
	}

	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	public String getCatalogTerm() throws SQLException {
		return "KEYSPACE";
	}

	public ResultSet getClientInfoProperties() throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		validateState();

		Properties queryPatterns = new Properties();
		queryPatterns.setProperty(KEY_CATALOG, CassandraUtils
				.isNullOrEmptyString(catalog) ? EMPTY_STRING : catalog);
		queryPatterns.setProperty(KEY_SCHEMA_PATTERN, CassandraUtils
				.isNullOrEmptyString(schemaPattern) ? EMPTY_STRING
				: schemaPattern);
		queryPatterns.setProperty(KEY_TABLE_PATTERN, CassandraUtils
				.isNullOrEmptyString(tableNamePattern) ? EMPTY_STRING
				: tableNamePattern);
		queryPatterns.setProperty(KEY_COLUMN_PATTERN, CassandraUtils
				.isNullOrEmptyString(columnNamePattern) ? EMPTY_STRING
				: columnNamePattern);

		return _conn.getObjectMetaData(CassandraObjectType.COLUMN,
				queryPatterns);
	}

	public Connection getConnection() throws SQLException {
		return _conn;
	}

	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public int getDatabaseMajorVersion() throws SQLException {
		return Integer.parseInt(CassandraUtils.getPropertyValue(_props,
				KEY_DB_MAJOR_VERSION, DEFAULT_DB_MAJOR_VERSION));
	}

	public int getDatabaseMinorVersion() throws SQLException {
		return Integer.parseInt(CassandraUtils.getPropertyValue(_props,
				KEY_DB_MINOR_VERSION, DEFAULT_DB_MINOR_VERSION));
	}

	public String getDatabaseProductName() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_PRODUCT_NAME,
				DEFAULT_PRODUCT_NAME);
	}

	public String getDatabaseProductVersion() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_PRODUCT_VERSION,
				DEFAULT_PRODUCT_VERSION);
	}

	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	public int getDriverMajorVersion() {
		return CassandraUtils.getPropertyValueAsInt(_props,
				KEY_DRIVER_MAJOR_VERSION, DEFAULT_DRIVER_MAJOR_VERSION);
	}

	public int getDriverMinorVersion() {
		return CassandraUtils.getPropertyValueAsInt(_props,
				KEY_DRIVER_MINOR_VERSION, DEFAULT_DRIVER_MINOR_VERSION);
	}

	public String getDriverName() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_DRIVER_NAME,
				DEFAULT_DRIVER_NAME);
	}

	public String getDriverVersion() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_DRIVER_VERSION,
				DEFAULT_DRIVER_VERSION);
	}

	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		validateState();

		Properties queryPatterns = new Properties();
		queryPatterns.setProperty(KEY_CATALOG, CassandraUtils
				.isNullOrEmptyString(catalog) ? EMPTY_STRING : catalog);
		queryPatterns.setProperty(KEY_SCHEMA_PATTERN, CassandraUtils
				.isNullOrEmptyString(schema) ? EMPTY_STRING : schema);
		queryPatterns.setProperty(KEY_TABLE_PATTERN, CassandraUtils
				.isNullOrEmptyString(table) ? EMPTY_STRING : table);
		queryPatterns.setProperty(KEY_UNIQUE_INDEX, Boolean.toString(unique));
		queryPatterns.setProperty(KEY_APPROXIMATE_INDEX,
				Boolean.toString(approximate));

		return _conn
				.getObjectMetaData(CassandraObjectType.INDEX, queryPatterns);
	}

	public int getJDBCMajorVersion() throws SQLException {
		return 4;
	}

	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	public int getMaxColumnsInTable() throws SQLException {
		return Integer.MAX_VALUE;
	}

	public int getMaxConnections() throws SQLException {
		return 0;
	}

	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	public int getMaxRowSize() throws SQLException {
		return Integer.MAX_VALUE;
	}

	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	public int getMaxStatements() throws SQLException {
		return 65535;
	}

	public int getMaxTableNameLength() throws SQLException {
		return 0;
	}

	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	public String getNumericFunctions() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_NUMERIC_FUNCTIONS);
	}

	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		validateState();

		Properties queryPatterns = new Properties();
		queryPatterns.setProperty(KEY_CATALOG, CassandraUtils
				.isNullOrEmptyString(catalog) ? EMPTY_STRING : catalog);
		queryPatterns.setProperty(KEY_SCHEMA_PATTERN, CassandraUtils
				.isNullOrEmptyString(schema) ? EMPTY_STRING : schema);
		queryPatterns.setProperty(KEY_TABLE_PATTERN, CassandraUtils
				.isNullOrEmptyString(table) ? EMPTY_STRING : table);

		return _conn.getObjectMetaData(CassandraObjectType.PRIMARY_KEY,
				queryPatterns);
	}

	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public String getProcedureTerm() throws SQLException {
		return "";
	}

	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	public ResultSet getSchemas() throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public String getSchemaTerm() throws SQLException {
		return "";
	}

	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	public String getSQLKeywords() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_SQL_KEYWORDS);
	}

	public int getSQLStateType() throws SQLException {
		return sqlStateSQL;
	}

	public String getStringFunctions() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_STRING_FUNCTIONS);
	}

	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public String getSystemFunctions() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_SYSTEM_FUNCTIONS);
	}

	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		validateState();

		Properties queryPatterns = new Properties();
		queryPatterns.setProperty(KEY_CATALOG, CassandraUtils
				.isNullOrEmptyString(catalog) ? EMPTY_STRING : catalog);
		queryPatterns.setProperty(KEY_SCHEMA_PATTERN, CassandraUtils
				.isNullOrEmptyString(schemaPattern) ? EMPTY_STRING
				: schemaPattern);
		queryPatterns.setProperty(KEY_TABLE_PATTERN, CassandraUtils
				.isNullOrEmptyString(tableNamePattern) ? EMPTY_STRING
				: tableNamePattern);

		return _conn.getObjectMetaData(CassandraObjectType.TABLE,
				queryPatterns, types);
	}

	public ResultSet getTableTypes() throws SQLException {
		validateState();

		return _conn.getObjectMetaData(CassandraObjectType.TABLE_TYPE, null);
	}

	public String getTimeDateFunctions() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_TIMEDATE_FUNCTIONS);
	}

	public ResultSet getTypeInfo() throws SQLException {
		return _conn.getObjectMetaData(CassandraObjectType.TYPE, null);
	}

	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		validateState();

		Properties queryPatterns = new Properties();
		queryPatterns.setProperty(KEY_CATALOG, CassandraUtils
				.isNullOrEmptyString(catalog) ? EMPTY_STRING : catalog);
		queryPatterns.setProperty(KEY_SCHEMA_PATTERN, CassandraUtils
				.isNullOrEmptyString(schemaPattern) ? EMPTY_STRING
				: schemaPattern);
		queryPatterns.setProperty(KEY_TYPE_PATTERN, CassandraUtils
				.isNullOrEmptyString(typeNamePattern) ? EMPTY_STRING
				: typeNamePattern);

		return _conn.getObjectMetaData(CassandraObjectType.UDT, queryPatterns,
				types);
	}

	public String getURL() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_CONNECTION_URL);
	}

	public String getUserName() throws SQLException {
		return CassandraUtils.getPropertyValue(_props, KEY_USERNAME,
				DEFAULT_USERNAME);
	}

	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		validateState();

		if (!quiet) {
			throw CassandraErrors.notSupportedException();
		}

		return CassandraUtils.DUMMY_RESULT_SET;
	}

	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	public boolean isReadOnly() throws SQLException {
		validateState();

		return _conn.isReadOnly();
	}

	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	public boolean nullPlusNonNullIsNull() throws SQLException {
		return false;
	}

	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	public void setProperties(Properties props) {
		if (props != null) {
			_props.putAll(props);
		}
	}

	public String setProperty(String key, String value) {
		Object oldValue = _props.setProperty(key, value);
		return oldValue == null ? null : oldValue.toString();
	}

	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return true;
	}

	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return true;
	}

	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return true;
	}

	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return true;
	}

	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return true;
	}

	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return true;
	}

	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return true;
	}

	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return true;
	}

	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	public boolean supportsConvert() throws SQLException {
		return false;
	}

	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		return false;
	}

	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return false;
	}

	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return false;
	}

	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	public boolean supportsGroupBy() throws SQLException {
		return false;
	}

	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return false;
	}

	public boolean supportsGroupByUnrelated() throws SQLException {
		return false;
	}

	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	public boolean supportsNonNullableColumns() throws SQLException {
		return false;
	}

	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return true;
	}

	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return true;
	}

	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY
				&& concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		return level == Connection.TRANSACTION_NONE;
	}

	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	public boolean supportsUnion() throws SQLException {
		return false;
	}

	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	public boolean usesLocalFiles() throws SQLException {
		return false;
	}
}
