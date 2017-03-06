/**
 * Copyright (C) 2015-2017, Zhichun Wu
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
 */
package com.github.cassandra.jdbc.provider.datastax;

import com.beust.jcommander.internal.Maps;
import com.datastax.driver.core.LocalDate;
import com.github.cassandra.jdbc.BaseCassandraTest;
import com.github.cassandra.jdbc.CassandraDataTypeConverters;
import com.github.cassandra.jdbc.CassandraTestHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.*;

public class CassandraPreparedStatementTest extends BaseCassandraTest {
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
            // s.setObject(index++, new byte[]{1, 2, 3});
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
            s.setObject(index++, CassandraTestHelper.getInstance().replaceParameter("11:50:30", Time.class));
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
        LocalDate ld = LocalDate.fromYearMonthDay(2015, 1, 1);
        Date d = Date.valueOf(date);
        org.joda.time.LocalDate jld = org.joda.time.LocalDate.fromDateFields(d);

        try {
            // set date by string
            java.sql.PreparedStatement s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, date);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            ResultSet rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), LocalDate.class), ld);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), LocalDate.class), date);
            assertEquals(rs.getDate(1), d);
            rs.close();
            s.close();

            // by LocalDate
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, CassandraTestHelper.getInstance().replaceParameter(ld, Date.class));
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), LocalDate.class), ld);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), LocalDate.class), date);
            assertEquals(rs.getDate(1), d);
            rs.close();
            s.close();

            // by date
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setDate(2, d);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), LocalDate.class), ld);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), LocalDate.class), date);
            assertEquals(rs.getDate(1), d);
            rs.close();
            s.close();

            // by Joda LocalDate
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, jld);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), LocalDate.class), ld);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), LocalDate.class), date);
            assertEquals(rs.getDate(1), d);
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
        String time = "13:30:54.234";
        long tl = 48654234000000L;
        LocalTime jlt = LocalTime.parse(time);
        Time t = new Time(jlt.toDateTimeToday().getMillis());

        try {
            // set time by string
            java.sql.PreparedStatement s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, CassandraTestHelper.getInstance().replaceParameter(time, Time.class));
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            ResultSet rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), Time.class), time);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), Time.class), tl);
            assertEquals(rs.getTime(1), t);
            rs.close();
            s.close();

            // by long
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, CassandraTestHelper.getInstance().replaceParameter(tl, Time.class));
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), Time.class), time);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), Time.class), tl);
            assertEquals(rs.getTime(1), t);
            rs.close();
            s.close();

            // by time
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, CassandraTestHelper.getInstance().replaceParameter(t, Time.class));
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), Time.class), time);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), Time.class), tl);
            assertEquals(rs.getTime(1), t);
            rs.close();
            s.close();

            // by Joda LocalTime
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, CassandraTestHelper.getInstance().replaceParameter(jlt, Time.class));
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getString(1), Time.class), time);
            assertEquals(CassandraTestHelper.getInstance().replaceResult(rs.getObject(1), Time.class), tl);
            assertEquals(rs.getTime(1), t);
            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testTimestamp() {
        String insertCql = "insert into test_drive.basic_data_type(id_uuid, date_timestamp) values(?, ?)";
        String queryCql = "select date_timestamp from test_drive.basic_data_type where id_uuid = ?";
        UUID id = UUID.randomUUID();
        String timestamp = "2019-02-01T04:12:21.330Z";
        String timestamp1 = "2019-02-01 04:12:21.330";
        Instant instant = Instant.parse(timestamp);
        long ts = instant.getMillis();
        Timestamp t = new Timestamp(ts);

        try {
            // set time by string
            java.sql.PreparedStatement s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setString(2, timestamp);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            ResultSet rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), timestamp);
            assertEquals(rs.getObject(1), instant.toDate());
            assertEquals(rs.getTimestamp(1), t);
            rs.close();
            s.close();

            // set time by string in the other format
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setString(2, timestamp1);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), timestamp);
            assertEquals(rs.getObject(1), instant.toDate());
            assertEquals(rs.getTimestamp(1), t);
            rs.close();
            s.close();

            // by long
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, ts);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), timestamp);
            assertEquals(rs.getObject(1), instant.toDate());
            assertEquals(rs.getTimestamp(1), t);
            rs.close();
            s.close();

            // by timestamp
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, t);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), timestamp);
            assertEquals(rs.getObject(1), instant.toDate());
            assertEquals(rs.getTimestamp(1), t);
            rs.close();
            s.close();

            // by Joda Instant
            s = conn.prepareStatement(insertCql);
            s.setObject(1, id);
            s.setObject(2, instant);
            s.execute();
            s.close();

            s = conn.prepareStatement(queryCql);
            s.setObject(1, id);
            rs = s.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), timestamp);
            assertEquals(rs.getObject(1), instant.toDate());
            assertEquals(rs.getTimestamp(1), t);
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

    @Test(groups = {"unit", "server"})
    public void testInsertLists() {
        String cql = "insert into test_drive.list_data_type(id,id_uuid,binary_data,date_date,date_time," +
                "date_timestamp,id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                "values(5d19b3b2-a889-4913-81ec-164e5845cf36,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraDataTypeConverters c = ((CassandraPreparedStatement) s).getDataTypeConverters();
            int index = 1;
            s.setObject(index++, Lists.newArrayList(UUID.randomUUID()));
            //s.setObject(index++, Lists.newArrayList(ByteBuffer.wrap(new byte[]{1, 2, 3})));
            s.setObject(index++, Lists.newArrayList(new byte[]{1, 2, 3}));
            //s.setObject(index++, Lists.newArrayList("2017-01-01"));
            s.setObject(index++, Lists.newArrayList(
                    CassandraTestHelper.getInstance().replaceParameter(
                            LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()), Date.class)));
            //s.setObject(index++, Lists.newArrayList("11:50:30"));
            //s.setObject(index++, Lists.newArrayList(LocalTime.now().getMillisOfDay() * 1000000L));
            s.setObject(index++, Lists.newArrayList(
                    CassandraTestHelper.getInstance().replaceParameter(
                            new Time(LocalTime.now().toDateTimeToday().getMillis()), Time.class)));
            //s.setObject(index++, Lists.newArrayList("2017-02-02 11:50:30.123"));
            s.setObject(index++, Lists.newArrayList(LocalDateTime.now().toDate()));
            // or you'll likely end up with error like the following:
            // com.datastax.driver.core.exceptions.InvalidTypeException: xxx is not a Type 1 (time-based) UUID
            s.setObject(index++, Lists.newArrayList(((CassandraPreparedStatement) s)
                    .getDataTypeConverters().defaultValueOf(UUID.class)));
            s.setObject(index++, Lists.newArrayList(InetAddress.getByName("192.168.10.11")));
            s.setObject(index++, Lists.newArrayList(Long.MAX_VALUE));
            s.setObject(index++, Lists.newArrayList(new BigDecimal("33333333333333333333333333333333333")));
            s.setObject(index++, Lists.newArrayList(Double.MAX_VALUE));
            s.setObject(index++, Lists.newArrayList(Float.MAX_VALUE));
            s.setObject(index++, Lists.newArrayList(Integer.MAX_VALUE));
            s.setObject(index++, Lists.newArrayList(
                    CassandraTestHelper.getInstance().replaceParameter(Short.MAX_VALUE, Short.class)));
            s.setObject(index++, Lists.newArrayList(
                    CassandraTestHelper.getInstance().replaceParameter(Byte.MAX_VALUE, Byte.class)));
            s.setObject(index++, Lists.newArrayList(new BigInteger("2222222222222222222222222222222222")));
            s.setObject(index++, Lists.newArrayList("ascii"));
            s.setObject(index++, Lists.newArrayList("text"));
            s.setObject(index++, Lists.newArrayList("varchar"));
            s.setObject(index++, Lists.newArrayList(true));

            assertFalse(s.execute());
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"}, dependsOnMethods = {"testInsertLists"})
    public void testQueryLists() {
        String cql = "select id_uuid,binary_data,date_date,date_time,date_timestamp,id_timeuuid," +
                "net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false\n" +
                "from test_drive.list_data_type where id = ?";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraDataTypeConverters c = ((CassandraPreparedStatement) s).getDataTypeConverters();

            s.setObject(1, "5d19b3b2-a889-4913-81ec-164e5845cf36");
            ResultSet rs = s.executeQuery();
            assertNotNull(s.getResultSet());
            assertTrue(rs.next());

            for (int i = 1; i < 19; i++) {
                assertTrue(rs.getObject(i) instanceof List);
                assertEquals(((List) rs.getObject(i)).size(), 1);
            }

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testInsertSets() {
        String cql = "insert into test_drive.set_data_type(id,id_uuid,binary_data,date_date,date_time," +
                "date_timestamp,id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                "values(5d19b3b2-a889-4913-81ec-164e5845cf36,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraDataTypeConverters c = ((CassandraPreparedStatement) s).getDataTypeConverters();
            int index = 1;
            s.setObject(index++, Sets.newHashSet(UUID.randomUUID()));
            //s.setObject(index++, Lists.newArrayList(ByteBuffer.wrap(new byte[]{1, 2, 3})));
            s.setObject(index++, Sets.newHashSet(new byte[]{1, 2, 3}));
            //s.setObject(index++, Lists.newArrayList("2017-01-01"));
            s.setObject(index++, Sets.newHashSet(
                    CassandraTestHelper.getInstance().replaceParameter(
                            LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()), Date.class)));
            //s.setObject(index++, Lists.newArrayList("11:50:30"));
            //s.setObject(index++, Lists.newArrayList(LocalTime.now().getMillisOfDay() * 1000000L));
            s.setObject(index++, Sets.newHashSet(
                    CassandraTestHelper.getInstance().replaceParameter(
                            new Time(LocalTime.now().toDateTimeToday().getMillis()), Time.class)));
            //s.setObject(index++, Lists.newArrayList("2017-02-02 11:50:30.123"));
            s.setObject(index++, Sets.newHashSet(LocalDateTime.now().toDate()));
            // or you'll likely end up with error like the following:
            // com.datastax.driver.core.exceptions.InvalidTypeException: xxx is not a Type 1 (time-based) UUID
            s.setObject(index++, Sets.newHashSet(((CassandraPreparedStatement) s)
                    .getDataTypeConverters().defaultValueOf(UUID.class)));
            s.setObject(index++, Sets.newHashSet(InetAddress.getByName("192.168.10.11")));
            s.setObject(index++, Sets.newHashSet(Long.MAX_VALUE));
            s.setObject(index++, Sets.newHashSet(new BigDecimal("33333333333333333333333333333333333")));
            s.setObject(index++, Sets.newHashSet(Double.MAX_VALUE));
            s.setObject(index++, Sets.newHashSet(Float.MAX_VALUE));
            s.setObject(index++, Sets.newHashSet(Integer.MAX_VALUE));
            s.setObject(index++, Sets.newHashSet(
                    CassandraTestHelper.getInstance().replaceParameter(Short.MAX_VALUE, Short.class)));
            s.setObject(index++, Sets.newHashSet(
                    CassandraTestHelper.getInstance().replaceParameter(Byte.MAX_VALUE, Byte.class)));
            s.setObject(index++, Sets.newHashSet(new BigInteger("2222222222222222222222222222222222")));
            s.setObject(index++, Sets.newHashSet("ascii"));
            s.setObject(index++, Sets.newHashSet("text"));
            s.setObject(index++, Sets.newHashSet("varchar"));
            s.setObject(index++, Sets.newHashSet(true));

            assertFalse(s.execute());
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"}, dependsOnMethods = {"testInsertSets"})
    public void testQuerySets() {
        String cql = "select id_uuid,binary_data,date_date,date_time,date_timestamp,id_timeuuid," +
                "net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false\n" +
                "from test_drive.set_data_type where id = ?";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraDataTypeConverters c = ((CassandraPreparedStatement) s).getDataTypeConverters();

            s.setObject(1, "5d19b3b2-a889-4913-81ec-164e5845cf36");
            ResultSet rs = s.executeQuery();
            assertNotNull(s.getResultSet());
            assertTrue(rs.next());

            for (int i = 1; i < 19; i++) {
                assertTrue(rs.getObject(i) instanceof Set);
                assertEquals(((Set) rs.getObject(i)).size(), 1);
            }

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testInsertMaps() {
        String cql = "insert into test_drive.map_data_type(id,id_uuid,binary_data,date_date,date_time," +
                "date_timestamp,id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                "values(5d19b3b2-a889-4913-81ec-164e5845cf36,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraDataTypeConverters c = ((CassandraPreparedStatement) s).getDataTypeConverters();
            int index = 1;
            s.setObject(index++, Maps.newHashMap(UUID.randomUUID(), UUID.randomUUID()));
            //s.setObject(index++, Lists.newArrayList(ByteBuffer.wrap(new byte[]{1, 2, 3})));
            s.setObject(index++, Maps.newHashMap(new byte[]{1, 2, 3}, new byte[]{1, 2, 3}));
            //s.setObject(index++, Lists.newArrayList("2017-01-01"));
            s.setObject(index++, Maps.newHashMap(
                    CassandraTestHelper.getInstance().replaceParameter(
                            LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()), Date.class),
                    CassandraTestHelper.getInstance().replaceParameter(
                            LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()), Date.class)));
            //s.setObject(index++, Lists.newArrayList("11:50:30"));
            //s.setObject(index++, Lists.newArrayList(LocalTime.now().getMillisOfDay() * 1000000L));
            s.setObject(index++, Maps.newHashMap(
                    CassandraTestHelper.getInstance().replaceParameter(
                            new Time(LocalTime.now().toDateTimeToday().getMillis()), Time.class),
                    CassandraTestHelper.getInstance().replaceParameter(
                            new Time(LocalTime.now().toDateTimeToday().getMillis()), Time.class)));
            //s.setObject(index++, Lists.newArrayList("2017-02-02 11:50:30.123"));
            s.setObject(index++, Maps.newHashMap(LocalDateTime.now().toDate(),
                    LocalDateTime.now().toDate()));
            // or you'll likely end up with error like the following:
            // com.datastax.driver.core.exceptions.InvalidTypeException: xxx is not a Type 1 (time-based) UUID
            s.setObject(index++, Maps.newHashMap(((CassandraPreparedStatement) s)
                            .getDataTypeConverters().defaultValueOf(UUID.class),
                    ((CassandraPreparedStatement) s).getDataTypeConverters().defaultValueOf(UUID.class)));
            s.setObject(index++, Maps.newHashMap(InetAddress.getByName("192.168.10.11"),
                    InetAddress.getByName("192.168.10.11")));
            s.setObject(index++, Maps.newHashMap(Long.MAX_VALUE, Long.MAX_VALUE));
            s.setObject(index++, Maps.newHashMap(new BigDecimal("33333333333333333333333333333333333"),
                    new BigDecimal("33333333333333333333333333333333333")));
            s.setObject(index++, Maps.newHashMap(Double.MAX_VALUE, Double.MAX_VALUE));
            s.setObject(index++, Maps.newHashMap(Float.MAX_VALUE, Float.MAX_VALUE));
            s.setObject(index++, Maps.newHashMap(Integer.MAX_VALUE, Integer.MAX_VALUE));
            s.setObject(index++, Maps.newHashMap(
                    CassandraTestHelper.getInstance().replaceParameter(Short.MAX_VALUE, Short.class),
                    CassandraTestHelper.getInstance().replaceParameter(Short.MAX_VALUE, Short.class)));
            s.setObject(index++, Maps.newHashMap(
                    CassandraTestHelper.getInstance().replaceParameter(Byte.MAX_VALUE, Byte.class),
                    CassandraTestHelper.getInstance().replaceParameter(Byte.MAX_VALUE, Byte.class)));
            s.setObject(index++, Maps.newHashMap(new BigInteger("2222222222222222222222222222222222"),
                    new BigInteger("2222222222222222222222222222222222")));
            s.setObject(index++, Maps.newHashMap("ascii", "ascii"));
            s.setObject(index++, Maps.newHashMap("text", "text"));
            s.setObject(index++, Maps.newHashMap("varchar", "varchar"));
            s.setObject(index++, Maps.newHashMap(true, true));

            assertFalse(s.execute());
            assertNull(s.getResultSet());
            assertEquals(s.getUpdateCount(), 1);

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"}, dependsOnMethods = {"testInsertMaps"})
    public void testQueryMaps() {
        String cql = "select id_uuid,binary_data,date_date,date_time,date_timestamp,id_timeuuid," +
                "net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false\n" +
                "from test_drive.map_data_type where id = ?";

        try {
            java.sql.PreparedStatement s = conn.prepareStatement(cql);
            assertTrue(s instanceof CassandraPreparedStatement);

            CassandraDataTypeConverters c = ((CassandraPreparedStatement) s).getDataTypeConverters();

            s.setObject(1, "5d19b3b2-a889-4913-81ec-164e5845cf36");
            ResultSet rs = s.executeQuery();
            assertNotNull(s.getResultSet());
            assertTrue(rs.next());

            for (int i = 1; i < 19; i++) {
                assertTrue(rs.getObject(i) instanceof Map);
                assertEquals(((Map) rs.getObject(i)).size(), 1);
            }

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
