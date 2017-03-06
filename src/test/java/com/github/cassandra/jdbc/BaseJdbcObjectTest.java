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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.testng.Assert.*;

public class BaseJdbcObjectTest {
    private BaseJdbcObject jdbcObj;

    @BeforeClass(groups = {"unit", "base"})
    public void setUp() throws Exception {
        jdbcObj = new BaseJdbcObject(false) {
            @Override
            protected SQLException tryClose() {
                return null;
            }

            @Override
            protected Object unwrap() {
                return null;
            }
        };
    }

    @AfterClass(groups = {"unit", "base"})
    public void tearDown() throws Exception {
        jdbcObj = null;
    }

    @Test(groups = {"unit", "base"})
    public void testGetWarnings() {
        try {
            assertNull(jdbcObj.getWarnings());

            SQLWarning w = new SQLWarning("warning1");
            jdbcObj.appendWarning(w);
            assertEquals(w, jdbcObj.getWarnings());
            assertNull(jdbcObj.getWarnings().getNextWarning());
            jdbcObj.clearWarnings();
            assertNull(jdbcObj.getWarnings());

            jdbcObj.appendWarning(w);
            jdbcObj.appendWarning(w);
            assertEquals(w, jdbcObj.getWarnings());
            assertNull(jdbcObj.getWarnings().getNextWarning());
            jdbcObj.clearWarnings();
            assertNull(jdbcObj.getWarnings());

            SQLWarning w2 = new SQLWarning("warning2");
            jdbcObj.appendWarning(w);
            jdbcObj.appendWarning(w2);
            assertEquals(w, jdbcObj.getWarnings());
            assertEquals(w2, jdbcObj.getWarnings().getNextWarning());
            assertNull(jdbcObj.getWarnings().getNextWarning().getNextWarning());
            jdbcObj.clearWarnings();
            assertNull(jdbcObj.getWarnings());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }

}
