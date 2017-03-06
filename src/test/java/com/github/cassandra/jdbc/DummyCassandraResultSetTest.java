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

import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.*;

public class DummyCassandraResultSetTest {

    @Test(groups = {"unit", "base"})
    public void testGetStringString() {
        String[][] columns = new String[][]{{"col_a", "text"},
                {"col_b", "text"}};
        String[][] data = new String[][]{{"a1", "b1"}, {"a2", "b2"}};
        DummyCassandraResultSet rs = new DummyCassandraResultSet(columns, data);
        DummyCassandraResultSet emptyRs = new DummyCassandraResultSet();
        try {
            int row = 0;
            while (rs.next()) { // for each row
                for (int i = 1; i <= 2; i++) { // for each column
                    assertEquals("", rs.getMetaData().getCatalogName(i));
                    assertEquals("", rs.getMetaData().getSchemaName(i));
                    assertEquals("", rs.getMetaData().getTableName(i));
                    assertEquals("text", rs.getMetaData().getColumnTypeName(i));
                    assertEquals(String.class.getName(), rs.getMetaData()
                            .getColumnClassName(i));
                    assertEquals(java.sql.Types.VARCHAR, rs.getMetaData()
                            .getColumnType(i));
                    assertEquals(columns[i - 1][0], rs.getMetaData()
                            .getColumnName(i));
                    assertEquals(columns[i - 1][0], rs.getMetaData()
                            .getColumnLabel(i));
                    assertEquals(data[row][i - 1], rs.getString(i));
                }
                row++;
            }
            rs.close();

            assertFalse(emptyRs.next());
            assertEquals(0, emptyRs.getMetaData().getColumnCount());
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Error occurred during testing: " + e.getMessage());
        }
    }
}
