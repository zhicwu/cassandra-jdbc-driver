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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

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
    public void testInsertBasicDataByObject() {
        String cql = "INSERT INTO test_drive.basic_data_type (id_uuid, binary_data, date_date, date_time, "
                + "date_timestamp, id_timeuuid, net_inet, num_big_integer, num_decimal, num_double, num_float, "
                + "num_int, num_small_int, num_tiny_int, num_varint, str_ascii, str_text, str_varchar, true_or_false) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;
            int index = 1;
            cs.setObject(index++, UUID.randomUUID());
            cs.setObject(index++, ByteBuffer.wrap(new byte[]{1, 2, 3}));
            cs.setObject(index++, Date.valueOf("2017-01-01"));
            cs.setObject(index++, Time.valueOf("11:50:30"));
            cs.setObject(index++, Timestamp.valueOf("2017-02-02 11:50:30.123"));
            cs.setObject(index++, UUID.randomUUID());
            cs.setObject(index++, InetAddress.getByName("192.168.10.11"));
            cs.setObject(index++, Long.MAX_VALUE);
            cs.setObject(index++, new BigDecimal("33333333333333333333333333333333333"));
            cs.setObject(index++, Double.MAX_VALUE);
            cs.setObject(index++, Float.MAX_VALUE);
            cs.setObject(index++, Integer.MAX_VALUE);
            cs.setObject(index++, Short.MAX_VALUE);
            cs.setObject(index++, Byte.MAX_VALUE);
            cs.setObject(index++, new BigInteger("2222222222222222222222222222222222"));
            cs.setObject(index++, "ascii");
            cs.setObject(index++, "text");
            cs.setObject(index++, "varchar");
            cs.setObject(index++, true);

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
    public void testInsertBasicDataByString() {
        String cql = "INSERT INTO test_drive.basic_data_type (id_uuid, binary_data, date_date, date_time, "
                + "date_timestamp, id_timeuuid, net_inet, num_big_integer, num_decimal, num_double, num_float, "
                + "num_int, num_small_int, num_tiny_int, num_varint, str_ascii, str_text, str_varchar, true_or_false) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;
            int index = 1;
            cs.setObject(index++, "c90d63ca-1d6d-45b9-bdce-e58395b3768d");
            cs.setObject(index++, "abc");
            cs.setObject(index++, "2017-01-01");
            cs.setObject(index++, "11:50:30");
            cs.setObject(index++, "2017-02-02 11:50:30.123");
            cs.setObject(index++, "e05c4d90-2802-11e6-97de-b991d5419640");
            cs.setObject(index++, "192.168.10.11");
            cs.setObject(index++, String.valueOf(Long.MAX_VALUE));
            cs.setObject(index++, "8888888888888888888888888888888");
            cs.setObject(index++, String.valueOf(Double.MAX_VALUE));
            cs.setObject(index++, String.valueOf(Float.MAX_VALUE));
            cs.setObject(index++, String.valueOf(Integer.MAX_VALUE));
            cs.setObject(index++, String.valueOf(Short.MAX_VALUE));
            cs.setObject(index++, String.valueOf(Byte.MAX_VALUE));
            cs.setObject(index++, "999999999999999999999999999999");
            cs.setObject(index++, "ascii");
            cs.setObject(index++, "text");
            cs.setObject(index++, "varchar");
            cs.setObject(index++, "True");

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
