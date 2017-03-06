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

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DataStaxProviderCompatibilityTest {
    @Test(groups = {"unit", "base"})
    public void testStatementImplementation() {
        CassandraStatement stmt = new CassandraStatement(null, null);
        assertTrue(stmt instanceof java.sql.Statement,
                "CassandraStatement should implement java.sql.Statement interface");
        assertFalse(stmt instanceof java.sql.PreparedStatement,
                "CassandraStatement should NOT implement java.sql.PreparedStatement interface");
    }

    @Test(groups = {"unit", "base"})
    public void testPreparedStatementImplementation() {
        try {
            CassandraPreparedStatement stmt = new CassandraPreparedStatement(null, null, "");
            assertTrue(stmt instanceof java.sql.PreparedStatement,
                    "CassandraPreparedStatement should implement java.sql.PreparedStatement interface");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }
}
