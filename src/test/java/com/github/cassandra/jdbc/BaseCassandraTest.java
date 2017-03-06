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

import com.github.cassandra.jdbc.provider.datastax.CassandraConnection;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.SQLException;

import static org.testng.Assert.assertTrue;

public class BaseCassandraTest {
    protected CassandraConnection conn;

    @BeforeClass(groups = {"unit", "server"})
    public void setUp() {
        CassandraDriver driver = new CassandraDriver();

        try {
            CassandraConfiguration config = CassandraConfiguration.DEFAULT;
            conn = (CassandraConnection) driver.connect(config.getConnectionUrl(), config.toProperties());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass(groups = {"unit", "server"})
    public void tearDown() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {

            }
        }
    }

    protected void validateObjectType(Object value, Class clazz) {
        clazz = CassandraTestHelper.getInstance().replaceDataType(clazz, value);
        assertTrue(value == null || clazz.isInstance(value), value + " is not instance of " + clazz);
    }
}
