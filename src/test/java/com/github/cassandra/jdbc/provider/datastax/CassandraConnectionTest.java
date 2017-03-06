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

import com.github.cassandra.jdbc.BaseCassandraTest;
import com.github.cassandra.jdbc.CassandraUtils;
import com.github.cassandra.jdbc.DummyCassandraResultSet;
import org.pmw.tinylog.Logger;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.Assert.*;

public class CassandraConnectionTest extends BaseCassandraTest {
    private String[] extractColumnNames(String[][] columns) {
        String[] names = new String[columns.length];
        int index = 0;
        for (String[] ss : columns) {
            names[index++] = ss[0];
        }

        return names;
    }

    @Test(groups = {"unit", "server"})
    public void testGetMetaData() {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            assertNotNull(metaData);
            assertEquals("KEYSPACE", metaData.getSchemaTerm());

            ResultSet rs = metaData.getTableTypes();
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.TABLE_TYPE_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            assertEquals(CassandraUtils.TABLE_TYPE_DATA[0],
                    CassandraUtils.getAllData(rs)[0]);
            rs.close();

            rs = metaData.getSchemas();
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.SCHEMA_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();

            rs = metaData.getTables(null, "system", "peers", null);
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.TABLE_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();

            rs = metaData.getColumns(null, "system", "peers", null);
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.COLUMN_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();

            rs = metaData.getIndexInfo(null, "system", "peers", false, true);
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.INDEX_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();

            rs = metaData.getPrimaryKeys(null, "system", "peers");
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.PK_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();

            rs = metaData.getUDTs(null, "system", "%", null);
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.UDT_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();

            rs = metaData.getColumns(null, "system", "IndexInfo", null);
            assertTrue(rs instanceof DummyCassandraResultSet);
            assertEquals(extractColumnNames(CassandraUtils.COLUMN_COLUMNS),
                    CassandraUtils.getColumnNames(rs));
            Logger.debug(CassandraUtils.getAllData(rs));
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testNativeSql() {
        try {
            String sql = "SELECT * FROM \"system\".\"peers\" LIMIT 5";
            assertEquals(sql, conn.nativeSQL(sql));
            assertEquals(
                    sql,
                    conn.nativeSQL("select a.* from \"system\".\"peers\" a limit 5"));
            sql = "SELECT a, b, c FROM test LIMIT 10000";
            assertEquals(sql, conn.nativeSQL(sql));
            assertEquals(sql, conn.nativeSQL("select t.a, t.b, c from test t"));
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
