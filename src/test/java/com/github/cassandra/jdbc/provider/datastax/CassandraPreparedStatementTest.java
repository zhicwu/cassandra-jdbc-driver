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
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

import static org.testng.Assert.*;

public class CassandraPreparedStatementTest extends DataStaxTestCase {
    private static final String TEST_KEY = "key";

    @Test(groups = {"unit", "server"})
    public void testNullParameter() {
        String sql = "-- set replace_null_value=true\n" +
                "select * from test_drive.basic_data_type where id_uuid = ?";
        try {

            java.sql.PreparedStatement s = conn.prepareStatement(sql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;
            cs.setObject(1, null);

            assertTrue(cs.execute());
            java.sql.ResultSet rs = cs.getResultSet();
            assertNotNull(rs);

            rs.close();
            cs.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testInsertBasicDataByObject() {
        String cql = "INSERT INTO test_drive.basic_data_type (id_uuid, binary_data, date_date, date_time, " +
                "date_timestamp, id_timeuuid, net_inet, num_big_integer, num_decimal, num_double, num_float, " +
                "num_int, num_small_int, num_tiny_int, num_varint, str_ascii, str_text, str_varchar, true_or_false) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            int index = 1;
            s.setObject(index++, UUID.randomUUID());
            s.setObject(index++, ByteBuffer.wrap(new byte[]{1, 2, 3}));
            s.setObject(index++, Date.valueOf("2017-01-01"));
            s.setObject(index++, Time.valueOf("11:50:30"));
            s.setObject(index++, Timestamp.valueOf("2017-02-02 11:50:30.123"));
            // or you'll likely end up with error like the following:
            // com.datastax.driver.core.exceptions.InvalidTypeException: xxx is not a Type 1 (time-based) UUID
            s.setObject(index++, ((CassandraPreparedStatement) s).getDataTypeConverters().defaultValueOf(UUID.class));
            s.setObject(index++, InetAddress.getByName("192.168.10.11"));
            s.setObject(index++, Long.MAX_VALUE);
            s.setObject(index++, new BigDecimal("33333333333333333333333333333333333"));
            s.setObject(index++, Double.MAX_VALUE);
            s.setObject(index++, Float.MAX_VALUE);
            s.setObject(index++, Integer.MAX_VALUE);
            s.setObject(index++, Short.MAX_VALUE);
            s.setObject(index++, Byte.MAX_VALUE);
            s.setObject(index++, new BigInteger("2222222222222222222222222222222222"));
            s.setObject(index++, "ascii");
            s.setObject(index++, "text");
            s.setObject(index++, "varchar");
            s.setObject(index++, true);

            assertFalse(s.execute());
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testInsertBasicDataByString() {
        String cql = "INSERT INTO test_drive.basic_data_type (id_uuid, binary_data, date_date, date_time, " +
                "date_timestamp, id_timeuuid, net_inet, num_big_integer, num_decimal, num_double, num_float, " +
                "num_int, num_small_int, num_tiny_int, num_varint, str_ascii, str_text, str_varchar, true_or_false) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            int index = 1;
            s.setObject(index++, "c90d63ca-1d6d-45b9-bdce-e58395b3768d");
            s.setObject(index++, "abc");
            s.setObject(index++, "2017-01-01");
            s.setObject(index++, "11:50:30");
            s.setObject(index++, "2017-02-02 11:50:30.123");
            s.setObject(index++, "e05c4d90-2802-11e6-97de-b991d5419640");
            s.setObject(index++, "192.168.10.11");
            s.setObject(index++, String.valueOf(Long.MAX_VALUE));
            s.setObject(index++, "8888888888888888888888888888888");
            s.setObject(index++, String.valueOf(Double.MAX_VALUE));
            s.setObject(index++, String.valueOf(Float.MAX_VALUE));
            s.setObject(index++, String.valueOf(Integer.MAX_VALUE));
            s.setObject(index++, String.valueOf(Short.MAX_VALUE));
            s.setObject(index++, String.valueOf(Byte.MAX_VALUE));
            s.setObject(index++, "999999999999999999999999999999");
            s.setObject(index++, "ascii");
            s.setObject(index++, "text");
            s.setObject(index++, "varchar");
            s.setObject(index++, "True");

            assertFalse(s.execute());
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testDate() {
        String insertCql = "insert into test_drive.basic_data_type(id_uuid, date_date) values(?, ?)";
        String queryCql = "select date_date from test_drive.basic_data_type where id_uuid = ?";
        UUID id = UUID.randomUUID();
        String date = "2015-01-01";
        try {
            java.sql.PreparedStatement s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, date);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            ResultSet rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getDate(1), Date.valueOf(date));
            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testTime() {
        String insertCql = "insert into test_drive.basic_data_type(id_uuid, date_time) values(?, ?)";
        String queryCql = "select date_time from test_drive.basic_data_type where id_uuid = ?";
        UUID id = UUID.randomUUID();
        String time = "15:15:15";
        try {
            java.sql.PreparedStatement s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, Time.valueOf(time));
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            ResultSet rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getTime(1), Time.valueOf(time));
            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testRepeatedInserts() {
        String cql = "insert into test_drive.basic_data_type(id_uuid, str_text) values(uuid(), ?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            s.setObject(1, TEST_KEY);

            assertFalse(s.execute());
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            assertEquals(s.executeUpdate(), 1);
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"}, dependsOnMethods = {"testRepeatedInserts"})
    public void testNullValues() {
        String cql = "select * from test_drive.basic_data_type where str_text = ? allow filtering";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);
            CassandraPreparedStatement cs = (CassandraPreparedStatement) s;

            s.setObject(1, TEST_KEY);
            assertTrue(s.execute());

            java.sql.ResultSet rs = s.getResultSet();
            assertTrue(rs instanceof CassandraResultSet);
            assertNotNull(rs);

            rs.next();
            int columnCount = rs.getMetaData().getColumnCount();
            CassandraResultSet crs = (CassandraResultSet) rs;
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rs.getMetaData().getColumnName(i);
                Class clazz = cs.getDataTypeMappings().javaTypeFor(rs.getMetaData().getColumnTypeName(i));
                if ("id_uuid".equals(columnName) || "str_text".equals(columnName)) {
                    continue;
                }

                assertNull(rs.getObject(i));
                assertTrue(rs.wasNull());
                assertNotNull(crs.getValue(i, clazz));
                assertTrue(rs.wasNull());
            }

            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
