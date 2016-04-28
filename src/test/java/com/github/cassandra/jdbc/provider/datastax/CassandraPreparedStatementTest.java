package com.github.cassandra.jdbc.provider.datastax;

import org.junit.Test;

import static org.junit.Assert.*;

public class CassandraPreparedStatementTest extends DataStaxTestCase {
    @Test
    public void testExecuteString() {
        try {
            String id = "a";
            byte[] bytes = new byte[]{1, 2, 3};
            java.sql.PreparedStatement s = conn.prepareStatement("insert into testblob(id, file_content) values(?, ?)");
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;
            cs.setObject(1, id);
            cs.setObject(2, bytes);

            boolean result = cs.execute();
            assertEquals(true, result);
            java.sql.ResultSet rs = cs.getResultSet();
            assertTrue(rs != null);

            rs.close();
            cs.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
