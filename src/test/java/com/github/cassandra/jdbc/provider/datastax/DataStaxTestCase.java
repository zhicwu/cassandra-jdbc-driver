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

import com.github.cassandra.jdbc.CassandraDriver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Properties;

public class DataStaxTestCase {
    protected CassandraConnection conn;

    @BeforeClass
    public void setUp() throws Exception {
        CassandraDriver driver = new CassandraDriver();

        Properties props = new Properties();
        props.load(getClass().getResourceAsStream("/connection.properties"));
        conn = (CassandraConnection) driver.connect(props.getProperty("url"), props);
    }

    @AfterClass
    public void tearDown() throws Exception {
        conn.close();
    }
}
