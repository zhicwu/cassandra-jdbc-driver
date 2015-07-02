package com.github.cassandra.jdbc.provider.datastax;

import static com.github.cassandra.jdbc.CassandraUtils.KEY_PASSWORD;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.cassandra.jdbc.CassandraDriver;
import com.github.cassandra.jdbc.CassandraUtils;
import com.github.cassandra.jdbc.DummyCassandraResultSet;
import com.github.cassandra.jdbc.provider.datastax.CassandraConnection;

public class CassandraConnectionTest extends DataStaxTestCase {
	private String[] extractColumnNames(String[][] columns) {
		String[] names = new String[columns.length];
		int index = 0;
		for (String[] ss : columns) {
			names[index++] = ss[0];
		}

		return names;
	}

	@Test
	public void testGetMetaData() {
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			assertNotNull(metaData);
			assertEquals("KEYSPACE", metaData.getCatalogTerm());

			ResultSet rs = metaData.getTableTypes();
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.TABLE_TYPE_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			assertEquals(CassandraUtils.TABLE_TYPE_DATA,
					CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getCatalogs();
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.CATALOG_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getTables("system", null, "peers", null);
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.TABLE_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getColumns("system", null, "peers", null);
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.COLUMN_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getIndexInfo("system", null, "peers", false, true);
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.INDEX_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getPrimaryKeys("system", null, "peers");
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.PK_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getUDTs("system", null, "%", null);
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.UDT_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();

			rs = metaData.getColumns("system", null, "IndexInfo", null);
			assertTrue(rs instanceof DummyCassandraResultSet);
			assertEquals(extractColumnNames(CassandraUtils.COLUMN_COLUMNS),
					CassandraUtils.getColumnNames(rs));
			System.out.println(CassandraUtils.getAllData(rs));
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Error occurred during testing: " + e.getMessage());
		}
	}

	@Test
	public void testNativeSql() {
		try {
			String sql = "SELECT * FROM \"system\".\"peers\" LIMIT 5";
			assertEquals(sql, conn.nativeSQL(sql));
			assertEquals(
					sql,
					conn.nativeSQL("select a.* from \"system\".\"peers\" a limit 5"));
			sql = "SELECT a, b, c FROM test";
			assertEquals(sql, conn.nativeSQL(sql));
			assertEquals(sql, conn.nativeSQL("select t.a, t.b, c from test t"));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Error occurred during testing: " + e.getMessage());
		}
	}
}
