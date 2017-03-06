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

import org.pmw.tinylog.Logger;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraConfiguration.KEY_PASSWORD;
import static com.github.cassandra.jdbc.CassandraConfiguration.KEY_USERNAME;
import static org.testng.Assert.*;

public class CassandraDriverTest {

    @Test(groups = {"unit", "base"})
    public void testAcceptsURL() {
        CassandraDriver driver = new CassandraDriver();
        try {
            String url = null;
            assertFalse(driver.acceptsURL(url));
            url = "jdbc:mysql:....";
            assertFalse(driver.acceptsURL(url));
            url = "jdbc:c*:datastax://host1,host2/keyspace1?key=value";
            assertTrue(driver.acceptsURL(url));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "server"})
    public void testConnect() {
        CassandraDriver driver = new CassandraDriver();

        try {
            CassandraConfiguration config = CassandraConfiguration.DEFAULT;
            Properties props = new Properties();
            props.setProperty(KEY_USERNAME, config.getUserName());
            props.setProperty(KEY_PASSWORD, config.getPassword());

            Connection conn = driver.connect(config.getConnectionUrl(), props);
            assertTrue(conn instanceof BaseCassandraConnection);
            assertTrue(conn.getClass().getName()
                    .endsWith("datastax.CassandraConnection"));

            conn.setSchema("system");
            ResultSet rs = conn.createStatement().executeQuery(
                    "select * from peers limit 5");
            while (rs.next()) {
                Logger.debug("{}\n=====", rs.getRow());
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    Object obj = rs.getObject(i);
                    Logger.debug("[{}]=[{}]", rs.getMetaData().getColumnName(i),
                            obj == null ? "null" : obj.getClass() + "@" + obj.hashCode());
                }
            }
            rs.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }

    @Test(groups = {"unit", "base"})
    public void testGetPropertyInfo() {
        CassandraDriver driver = new CassandraDriver();
        try {
            String url = "jdbc:c*:datastax://host1,host2/keyspace1?key=value";
            DriverPropertyInfo[] info = driver.getPropertyInfo(url, new Properties());
            assertNotNull(info);
            assertTrue(info.length > 1);

            for (DriverPropertyInfo i : info) {
                if (i.name.equals("consistencyLevel")) {
                    assertNotNull(i.choices);
                    assertTrue(i.choices.length > 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }
}
