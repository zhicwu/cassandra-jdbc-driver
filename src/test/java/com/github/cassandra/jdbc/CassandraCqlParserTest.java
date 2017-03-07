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
package com.github.cassandra.jdbc;


import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class CassandraCqlParserTest {
    @DataProvider(name = "cql-scripts")
    public Object[][] createTestCql() {
        return new Object[][]{
                {"select a1,a2,a3 from a where status='NEW' allow filtering",
                        "select a1,a2,a3 from a where status='NEW' allow filtering", CassandraStatementType.SELECT},
                {"select * from a where status='NEW' allow filtering",
                        "select * from a where status='NEW' allow filtering", CassandraStatementType.SELECT},
                {"select * from a where id=1 and (s=2 or status='NEW') allow filtering",
                        "select * from a where id=1 and (s=2 or status='NEW') allow filtering", CassandraStatementType.SELECT},
        };
    }

    @DataProvider(name = "sql-scripts")
    public Object[][] createTestSql() {
        return new Object[][]{
                {"use system", "use system", CassandraStatementType.UNKNOWN},
                {"create table a(b int primary key)", "CREATE TABLE a (b int primary key)",
                        CassandraStatementType.CREATE},
                {"alter table a alter b type text", "alter table a alter b type text",
                        CassandraStatementType.ALTER},
                {"drop table a", "DROP table a", CassandraStatementType.DROP},
                {"-- set a=b\n-- set c=1\nselect * from a", "SELECT * FROM a LIMIT " +
                        CassandraConfiguration.DEFAULT.getRowLimit(),
                        CassandraStatementType.SELECT},
                {"Select 1 as a", "SELECT 1 AS a", CassandraStatementType.SELECT},
                {"select a + 1 as a from b", "SELECT a + 1 AS a FROM b LIMIT " +
                        CassandraConfiguration.DEFAULT.getRowLimit(),
                        CassandraStatementType.SELECT},
                {"select \ntbl.key,tbl.bootstrapped,tbl.cluster_name,tbl.cql_version," +
                        "tbl.data_center,tbl.dse_version,tbl.gossip_generation,tbl.host_id," +
                        "tbl.native_protocol_version,tbl.partitioner,tbl.rack,tbl.release_version," +
                        "tbl.schema_version,tbl.thrift_version,tbl.tokens,tbl.truncated_at,tbl.workload " +
                        "from \"system\".\"local\" tbl",
                        "SELECT \"key\", bootstrapped, cluster_name, cql_version, data_center, " +
                                "dse_version, gossip_generation, host_id, native_protocol_version, " +
                                "partitioner, rack, release_version, schema_version, thrift_version, " +
                                "tokens, truncated_at, workload FROM \"system\".\"local\" LIMIT " +
                                CassandraConfiguration.DEFAULT.getRowLimit(), CassandraStatementType.SELECT
                },
                {"select tbl.id_uuid,tbl.binary_data,tbl.date_date,tbl.date_time,tbl.date_timestamp," +
                        "tbl.id_timeuuid,tbl.net_inet,tbl.num_big_integer,tbl.num_decimal,tbl.num_double," +
                        "tbl.num_float,tbl.num_int,tbl.num_small_int,tbl.num_tiny_int,tbl.num_varint,tbl.str_ascii," +
                        "tbl.str_text,tbl.str_varchar,tbl.true_or_false from \"test_drive\".\"basic_data_type\" tbl",
                        "SELECT id_uuid, binary_data, date_date, date_time, date_timestamp, " +
                                "id_timeuuid, net_inet, num_big_integer, num_decimal, num_double, num_float, " +
                                "num_int, num_small_int, num_tiny_int, num_varint, str_ascii, str_text, " +
                                "str_varchar, true_or_false FROM \"test_drive\".\"basic_data_type\" LIMIT " +
                                CassandraConfiguration.DEFAULT.getRowLimit(), CassandraStatementType.SELECT
                },
                {"select * from a where b = 1 allow filtering", "select * from a where b = 1 allow filtering",
                        CassandraStatementType.SELECT
                },
                {"insert into a(b,c) values(1,'c')", "INSERT INTO a (b, c) VALUES (1, 'c')", CassandraStatementType.INSERT},
                {"update a set c = 'b' where b=1", "UPDATE a SET c = 'b' WHERE b = 1",
                        CassandraStatementType.UPDATE
                },
                {"delete from a where b=1", "DELETE FROM a WHERE b = 1",
                        CassandraStatementType.DELETE
                }
        };
    }

    @Test(groups = {"unit", "base"}, dataProvider = "cql-scripts")
    public void testParseCql(String cql, String expectedCql, CassandraStatementType expectedType) {
        try {
            CassandraCqlStatement stmt = CassandraCqlParser.parse(CassandraConfiguration.DEFAULT, cql);

            assertEquals(stmt.getCql(), expectedCql);
            assertEquals(stmt.getConfiguration().getStatementType(), expectedType);
        } catch (Exception e) {
            fail("Failed", e);
        }
    }

    @Test(groups = {"unit", "base"}, dataProvider = "sql-scripts")
    public void testParseSql(String sql, String expectedSql, CassandraStatementType expectedType) {
        try {
            CassandraCqlStatement stmt = CassandraCqlParser.parse(CassandraConfiguration.DEFAULT, sql);

            assertEquals(stmt.getCql(), expectedSql);
            assertEquals(stmt.getConfiguration().getStatementType(), expectedType);
        } catch (Exception e) {
            fail("Failed", e);
        }
    }

    @Test(groups = {"unit", "base"})
    public void testMagicComments() {
        String sql = "-- set consistency_level = aNY;fetch_size=991;;;\n" +
                "-- set no_limit=true ; tracing = true\n" +
                "-- set read_timeout = 51\n" +
                "-- set replace_null_value = true  ; sql_parser = true; \n" +
                "/* set replace_null_value = false ; sql_parser = false; \n" +
                "   set replace_null_value = false ; sql_parser = false; */\n" +
                "// set no_wait = true\n" +
                "select * from system.local";

        try {
            CassandraCqlStatement stmt = CassandraCqlParser.parse(CassandraConfiguration.DEFAULT, sql);

            CassandraCqlStmtConfiguration conf = stmt.getConfiguration();
            assertEquals(conf.getConsistencyLevel(), "ANY");
            assertEquals(conf.getFetchSize(), 991);
            assertEquals(conf.getReadTimeout(), 51 * 1000);
            assertTrue(conf.noLimit());
            assertTrue(conf.tracingEnabled());
            assertTrue(conf.replaceNullValue());
            assertTrue(conf.sqlParserEnabled());
            assertTrue(conf.noWait());
        } catch (Exception e) {
            fail("Failed", e);
        }
    }
}
