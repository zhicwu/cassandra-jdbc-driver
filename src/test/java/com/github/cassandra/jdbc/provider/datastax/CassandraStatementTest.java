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

import com.datastax.driver.core.LocalDate;
import com.github.cassandra.jdbc.BaseCassandraTest;
import com.github.cassandra.jdbc.CassandraTestHelper;
import com.github.cassandra.jdbc.CassandraUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.util.UUID;

import static org.testng.Assert.*;

public class CassandraStatementTest extends BaseCassandraTest {
    @DataProvider(name = "upsert-sql")
    public Object[][] createTestSql() {
        return new Object[][]{
                // basic data types
                {"insert into test_drive.basic_data_type(id_uuid,binary_data,date_date,date_time,date_timestamp," +
                        "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int,num_small_int," +
                        "num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                        "values(uuid(),textAsBlob('123'),'2016-05-31','13:30:54.234','2016-05-31 13:30:54.234',now()," +
                        "'127.0.0.1',1,1,1.1,1.1,1,1,1,1,'a','aaa','aaa',true)"},
                // static data types
                {"insert into test_drive.static_data_type(id1,id2,id_uuid,binary_data,date_date,date_time," +
                        "date_timestamp,id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float," +
                        "num_int,num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                        "values(uuid(),uuid(),uuid(),textAsBlob('123'),'2016-05-31','13:30:54.234'," +
                        "'2016-05-31 13:30:54.234',now(),'127.0.0.1',1,1,1.1,1.1,1,1,1,1,'a','aaa','aaa',true)"},
                // counter
                {"update test_drive.counter_data_type set num_counter = num_counter + 123 where id_uuid = uuid()"},
                // list
                {"insert into test_drive.list_data_type(id,id_uuid,binary_data,date_date,date_time,date_timestamp," +
                        "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                        "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                        "values(uuid(),[uuid()], [textAsBlob('123')],['2016-05-31'],['13:30:54.234']," +
                        "['2016-05-31 13:30:54.234'],[now()],['127.0.0.1'],[1],[1],[1.1],[1.1],[1],[1]," +
                        "[1],[1],['a'],['aaa'],['aaa'],[true])"},
                // set
                {"insert into test_drive.set_data_type(id,id_uuid,binary_data,date_date,date_time,date_timestamp," +
                        "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                        "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                        "values(uuid(), {uuid()}, {textAsBlob('123')},{'2016-05-31'},{'13:30:54.234'}," +
                        "{'2016-05-31 13:30:54.234'},{now()},{'127.0.0.1'},{1},{1},{1.1},{1.1},{1},{1}," +
                        "{1},{1},{'a'},{'aaa'},{'aaa'},{true})"},
                // map
                {"insert into test_drive.map_data_type(id,id_uuid,binary_data,date_date,date_time,date_timestamp," +
                        "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int," +
                        "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n" +
                        "values(uuid(), {uuid():uuid()}, {textAsBlob('123'):textAsBlob('123')}," +
                        "{'2016-05-31':'2016-05-31'},{'13:30:54.234':'13:30:54.234'}," +
                        "{'2016-05-31 13:30:54.234':'2016-05-31 13:30:54.234'},{now():now()}," +
                        "{'127.0.0.1':'127.0.0.1'},{1:1},{1:1},{1.1:1.1},{1.1:1.1},{1:1},{1:1},{1:1},{1:1}," +
                        "{'a':'a'},{'aaa':'aaa'},{'aaa':'aaa'},{true:false})"},
                // frozen types
                {"insert into test_drive.frozen_data_type(id_uuid,fz_list,nested_fz_list,fz_set,nested_fz_set," +
                        "fz_map,nested_fz_map)\n" +
                        "values(uuid(),['a'],[['a'],['b1','b2']],{'a'},{{'a'},{'b1','b2'}},{'a':'1'}," +
                        "{'a':{'a':'1'},'b':{'b1':'b1','b2':'b2'}})"
                }
        };
    }

    @Test(groups = {"unit", "server"}, dataProvider = "upsert-sql")
    public void testInsertBasicData(String sql) {
        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            int result = s.executeUpdate(CassandraTestHelper.getInstance().replaceStatement(sql));
            assertEquals(result, 1);
            assertNull(s.getResultSet());

            assertFalse(s.execute(CassandraTestHelper.getInstance().replaceStatement(sql)));
            assertNull(s.getResultSet());

            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"}, dependsOnMethods = {"testInsertBasicData"})
    public void testQueryBasicDataAsObject() {
        String cql = "select tbl.id_uuid,tbl.binary_data,tbl.date_date,tbl.date_time,tbl.date_timestamp," +
                "tbl.id_timeuuid,tbl.net_inet,tbl.num_big_integer,tbl.num_decimal,tbl.num_double,tbl.num_float," +
                "tbl.num_int,tbl.num_small_int,tbl.num_tiny_int,tbl.num_varint,tbl.str_ascii,tbl.str_text," +
                "tbl.str_varchar,tbl.true_or_false from \"test_drive\".\"basic_data_type\" tbl limit 1";

        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            ResultSet rs = s.executeQuery(cql);
            assertTrue(rs instanceof CassandraResultSet);
            assertNotNull(rs);
            assertTrue(rs == s.getResultSet());

            while (rs.next()) { // only need to read one row
                int index = 1;
                validateObjectType(rs.getObject(index++), UUID.class);
                validateObjectType(rs.getObject(index++), ByteBuffer.class);
                validateObjectType(rs.getObject(index++), LocalDate.class);
                validateObjectType(rs.getObject(index++), Long.class);
                validateObjectType(rs.getObject(index++), java.util.Date.class);
                validateObjectType(rs.getObject(index++), UUID.class);
                validateObjectType(rs.getObject(index++), InetAddress.class);
                validateObjectType(rs.getObject(index++), Long.class);
                validateObjectType(rs.getObject(index++), BigDecimal.class);
                validateObjectType(rs.getObject(index++), Double.class);
                validateObjectType(rs.getObject(index++), Float.class);
                validateObjectType(rs.getObject(index++), Integer.class);
                validateObjectType(rs.getObject(index++), Short.class);
                validateObjectType(rs.getObject(index++), Byte.class);
                validateObjectType(rs.getObject(index++), BigInteger.class);
                validateObjectType(rs.getObject(index++), String.class);
                validateObjectType(rs.getObject(index++), String.class);
                validateObjectType(rs.getObject(index++), String.class);
                validateObjectType(rs.getObject(index++), Boolean.class);
            }

            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"}, dependsOnMethods = {"testInsertBasicData"})
    public void testMaxRows() {
        String cql = "select * from \"test_drive\".\"basic_data_type\" tbl";

        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            s.setMaxRows(1);
            ResultSet rs = s.executeQuery(cql);
            assertTrue(rs instanceof CassandraResultSet);
            assertNotNull(rs);
            assertTrue(rs == s.getResultSet());

            String[] columns = CassandraUtils.getColumnNames(rs);
            Object[][] data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);
            assertTrue(data.length == 1);

            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testSimpleQuery() {
        String cql = "select * from system.local limit 1";

        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            boolean result = s.execute(cql);
            assertEquals(result, true);
            java.sql.ResultSet rs = s.getResultSet();
            assertNotNull(rs);

            String[] columns = CassandraUtils.getColumnNames(rs);
            Object[][] data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);
            assertTrue(data.length > 0);

            rs.close();
            s.close();

            s = conn.createStatement();
            rs = s.executeQuery(cql);
            assertNotNull(rs);
            assertEquals(s.getResultSet(), rs);

            columns = CassandraUtils.getColumnNames(rs);
            data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);
            assertTrue(data.length > 0);

            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testAsyncQuery() {
        String cql = "-- set no_wait = true\nselect * from system.peers limit 1";

        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            boolean result = s.execute(cql);
            assertEquals(result, true);
            java.sql.ResultSet rs = s.getResultSet();
            assertNotNull(rs);

            String[] columns = CassandraUtils.getColumnNames(rs);
            Object[][] data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length == 0);
            assertTrue(data.length == 0);

            rs.close();
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
