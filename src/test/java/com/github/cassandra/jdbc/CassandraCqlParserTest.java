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
package com.github.cassandra.jdbc;


import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.github.cassandra.jdbc.parser.SqlToCqlTranslator.DEFAULT_ROW_LIMIT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class CassandraCqlParserTest {
    @DataProvider(name = "parse-sql")
    public Object[][] createTestSql() {
        return new Object[][]{
                {"-- set a=b\n-- set c=1\nselect * from a", "SELECT * FROM a LIMIT " + DEFAULT_ROW_LIMIT},
                {"Select 1 as a", "SELECT 1 AS a"},
                {"select a + 1 as a from b", "SELECT a + 1 AS a FROM b LIMIT " + DEFAULT_ROW_LIMIT},
                {"select \ntbl.key,tbl.bootstrapped,tbl.cluster_name,tbl.cql_version,"
                        + "tbl.data_center,tbl.dse_version,tbl.gossip_generation,tbl.host_id,"
                        + "tbl.native_protocol_version,tbl.partitioner,tbl.rack,tbl.release_version,"
                        + "tbl.schema_version,tbl.thrift_version,tbl.tokens,tbl.truncated_at,tbl.workload "
                        + "from \"system\".\"local\" tbl",
                        "SELECT \"key\", bootstrapped, cluster_name, cql_version, data_center, "
                                + "dse_version, gossip_generation, host_id, native_protocol_version, "
                                + "partitioner, rack, release_version, schema_version, thrift_version, "
                                + "tokens, truncated_at, workload FROM \"system\".\"local\" LIMIT "
                                + DEFAULT_ROW_LIMIT
                }
        };
    }

    @Test(groups = {"unit", "base"}, dataProvider = "parse-sql")
    public void testNormalizeSql(String sql, String expectedSql) {
        try {
            CassandraCqlStatement stmt = CassandraCqlParser.parse(
                    CassandraConfiguration.load(getClass().getResourceAsStream("/connection.properties")), sql);

            assertEquals(stmt.getCql(), expectedSql);
        } catch (Exception e) {
            fail("Failed", e);
        }
    }
}
