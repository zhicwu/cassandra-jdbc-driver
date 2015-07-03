package com.github.cassandra.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Test;

public class DummyCassandraResultSetTest {

	@Test
	public void testGetStringString() {
		String[][] columns = new String[][] { { "col_a", "text" },
				{ "col_b", "text" } };
		String[][] data = new String[][] { { "a1", "b1" }, { "a2", "b2" } };
		DummyCassandraResultSet rs = new DummyCassandraResultSet(columns, data);
		DummyCassandraResultSet emptyRs = new DummyCassandraResultSet();
		try {
			int row = 0;
			while (rs.next()) { // for each row
				for (int i = 1; i <= 2; i++) { // for each column
					assertEquals("", rs.getMetaData().getCatalogName(i));
					assertEquals("", rs.getMetaData().getSchemaName(i));
					assertEquals("", rs.getMetaData().getTableName(i));
					assertEquals("text", rs.getMetaData().getColumnTypeName(i));
					assertEquals(String.class.getName(), rs.getMetaData()
							.getColumnClassName(i));
					assertEquals(java.sql.Types.VARCHAR, rs.getMetaData()
							.getColumnType(i));
					assertEquals(columns[i - 1][0], rs.getMetaData()
							.getColumnName(i));
					assertEquals(columns[i - 1][0], rs.getMetaData()
							.getColumnLabel(i));
					assertEquals(data[row][i - 1], rs.getString(i));
				}
				row++;
			}
			rs.close();

			assertFalse(emptyRs.next());
			assertEquals(0, emptyRs.getMetaData().getColumnCount());
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Error occurred during testing: " + e.getMessage());
		}
	}
}
