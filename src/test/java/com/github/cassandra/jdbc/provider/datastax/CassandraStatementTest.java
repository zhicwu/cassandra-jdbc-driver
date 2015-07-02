package com.github.cassandra.jdbc.provider.datastax;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

import com.github.cassandra.jdbc.CassandraUtils;
import com.github.cassandra.jdbc.provider.datastax.CassandraStatement;

public class CassandraStatementTest extends DataStaxTestCase {
	@Test
	public void testExecuteString() {
		try {
			java.sql.Statement s = conn.createStatement();
			assertTrue(s instanceof CassandraStatement);

			CassandraStatement cs = (CassandraStatement) s;
			boolean result = cs.execute("select * from system.peers limit 5");
			assertEquals(true, result);
			java.sql.ResultSet rs = cs.getResultSet();
			assertTrue(rs != null);

			String[] columns = CassandraUtils.getColumnNames(rs);
			Object[][] data = CassandraUtils.getAllData(rs);
			assertTrue(columns.length > 0);			
			assertEquals(5, data.length);
			
			rs.close();
			cs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Error occurred during testing: " + e.getMessage());
		}
	}
}
