package com.github.cassandra.jdbc.provider.datastax;

import com.github.cassandra.jdbc.CassandraUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class CassandraStatementTest extends DataStaxTestCase {
    @Test
    public void testExecuteString() {
        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            CassandraStatement cs = (CassandraStatement) s;
            boolean result = cs.execute("select * from peers limit 1");
            assertEquals(true, result);
            java.sql.ResultSet rs = cs.getResultSet();
            assertTrue(rs != null);

            String[] columns = CassandraUtils.getColumnNames(rs);
            Object[][] data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);

            rs.close();
            cs.close();
            conn.close();

            this.setUp();

            cs = (CassandraStatement) conn.createStatement();
            result = cs.execute("select * from local limit 1");
            assertEquals(true, result);
            rs = cs.getResultSet();
            assertTrue(rs != null);

            columns = CassandraUtils.getColumnNames(rs);
            data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);

            rs.close();
            cs.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
