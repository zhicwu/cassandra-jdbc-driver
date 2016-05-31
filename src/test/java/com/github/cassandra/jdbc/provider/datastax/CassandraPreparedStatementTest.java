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

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class CassandraPreparedStatementTest extends DataStaxTestCase {
    @Test(groups = {"unit", "server"})
    public void testNullParameter() {
        String sql = "-- set replace_null_value=true\n"
            + "select * from test_drive.basic_data_type where id_uuid = ?";
        try {

            java.sql.PreparedStatement s = conn.prepareStatement(sql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;
            cs.setObject(1, null);

            boolean result = cs.execute();
            assertEquals(true, result);
            java.sql.ResultSet rs = cs.getResultSet();
            assertTrue(rs != null);

            rs.close();
            cs.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testExecuteString() {
        try {
            byte[] bytes = new byte[]{1, 2, 3};
            java.sql.PreparedStatement s = conn.prepareStatement("insert into test_drive.basic_data_type(id_uuid, binary_data) values(uuid(), ?)");
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;
            cs.setObject(1, bytes);

            boolean result = cs.execute();
            assertEquals(true, result);
            java.sql.ResultSet rs = cs.getResultSet();
            assertTrue(rs != null);

            rs.close();
            cs.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
