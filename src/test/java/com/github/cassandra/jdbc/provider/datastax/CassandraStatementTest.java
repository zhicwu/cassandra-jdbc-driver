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

import com.github.cassandra.jdbc.CassandraUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class CassandraStatementTest extends DataStaxTestCase {
    @DataProvider(name = "upsert-sql")
    public Object[][] createTestSql() {
        return new Object[][]{
                // basic data types
                {"insert into test_drive.basic_data_type(id_uuid,binary_data,date_date,date_time,date_timestamp,"
                        + "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int,num_small_int,"
                        + "num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n"
                        + "values(uuid(),textAsBlob('123'),'2016-05-31','13:30:54.234','2016-05-31 13:30:54.234',now(),"
                        + "'127.0.0.1',1,1,1.1,1.1,1,1,1,1,'a','aaa','aaa',true)"},
                // counter
                {"update test_drive.counter_data_type set num_counter = num_counter + 123 where id_uuid = uuid()"},
                // list
                {"insert into test_drive.list_data_type(id,id_uuid,binary_data,date_date,date_time,date_timestamp,"
                        + "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int,"
                        + "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n"
                        + "values(uuid(),[uuid()], [textAsBlob('123')],['2016-05-31'],['13:30:54.234'],"
                        + "['2016-05-31 13:30:54.234'],[now()],['127.0.0.1'],[1],[1],[1.1],[1.1],[1],[1],"
                        + "[1],[1],['a'],['aaa'],['aaa'],[true])"},
                // set
                {"insert into test_drive.set_data_type(id,id_uuid,binary_data,date_date,date_time,date_timestamp,"
                        + "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int,"
                        + "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n"
                        + "values(uuid(), {uuid()}, {textAsBlob('123')},{'2016-05-31'},{'13:30:54.234'},"
                        + "{'2016-05-31 13:30:54.234'},{now()},{'127.0.0.1'},{1},{1},{1.1},{1.1},{1},{1},"
                        + "{1},{1},{'a'},{'aaa'},{'aaa'},{true})"},
                // map
                {"insert into test_drive.map_data_type(id,id_uuid,binary_data,date_date,date_time,date_timestamp,"
                        + "id_timeuuid,net_inet,num_big_integer,num_decimal,num_double,num_float,num_int,"
                        + "num_small_int,num_tiny_int,num_varint,str_ascii,str_text,str_varchar,true_or_false)\n"
                        + "values(uuid(), {uuid():uuid()}, {textAsBlob('123'):textAsBlob('123')},"
                        + "{'2016-05-31':'2016-05-31'},{'13:30:54.234':'13:30:54.234'},"
                        + "{'2016-05-31 13:30:54.234':'2016-05-31 13:30:54.234'},{now():now()},"
                        + "{'127.0.0.1':'127.0.0.1'},{1:1},{1:1},{1.1:1.1},{1.1:1.1},{1:1},{1:1},{1:1},{1:1},"
                        + "{'a':'a'},{'aaa':'aaa'},{'aaa':'aaa'},{true:false})"},
                // frozen types
                {"insert into test_drive.frozen_data_type(id_uuid,fz_list,nested_fz_list,fz_set,nested_fz_set,"
                        + "fz_map,nested_fz_map)\n"
                        + "values(uuid(),['a'],[['a'],['b1','b2']],{'a'},{{'a'},{'b1','b2'}},{'a':'1'},"
                        + "{'a':{'a':'1'},'b':{'b1':'b1','b2':'b2'}})"
                }
        };
    }

    @Test(groups = {"unit", "server"}, dataProvider = "upsert-sql")
    public void testInsertBasicData(String sql) {
        try {
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            CassandraStatement cs = (CassandraStatement) s;
            boolean result = cs.execute(sql);
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
            java.sql.Statement s = conn.createStatement();
            assertTrue(s instanceof CassandraStatement);

            CassandraStatement cs = (CassandraStatement) s;
            boolean result = cs.execute("select * from system.peers limit 1");
            assertEquals(true, result);
            java.sql.ResultSet rs = cs.getResultSet();
            assertTrue(rs != null);

            String[] columns = CassandraUtils.getColumnNames(rs);
            Object[][] data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);

            rs.close();
            cs.close();

            cs = (CassandraStatement) conn.createStatement();
            result = cs.execute("select * from system.local limit 1");
            assertEquals(true, result);
            rs = cs.getResultSet();
            assertTrue(rs != null);

            columns = CassandraUtils.getColumnNames(rs);
            data = CassandraUtils.getAllData(rs);
            assertTrue(columns.length > 0);

            rs.close();
            cs.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
