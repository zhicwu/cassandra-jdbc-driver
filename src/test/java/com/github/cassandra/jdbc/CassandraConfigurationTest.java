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

import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraConfiguration.*;
import static org.testng.Assert.*;

public class CassandraConfigurationTest {
    @Test(groups = {"unit", "base"})
    public void testextractDriverConfig() {
        Properties props = new Properties();
        props.setProperty("hosts", "host1.test.com,host2.test.com,host3.test.com");
        props.setProperty("port", "999");
        props.setProperty("tracing", "true");
        props.setProperty("consistencyLevel", "QUORUM");
        CassandraConfiguration.DriverConfig config = generateDriverConfig(props);
        assertEquals(config.hosts, "host1.test.com,host2.test.com,host3.test.com");
        assertEquals(config.port, 999);
        assertEquals(config.tracing, true);
        assertEquals(config.consistencyLevel, CassandraEnums.ConsistencyLevel.QUORUM);
    }

    @Test(groups = {"unit", "base"})
    public void testParseConnectionURL() {
        String url = "jdbc:c*:mine://host1.a.com:9160,host2.a.com:9170/keyspace1?consistencyLevel=ONE";

        try {
            Properties props = CassandraConfiguration.parseConnectionURL(url);
            assertEquals("mine", props.getProperty(KEY_PROVIDER));
            assertEquals("host1.a.com:9160,host2.a.com:9170",
                    props.getProperty(KEY_HOSTS));
            assertEquals("keyspace1", props.getProperty(KEY_KEYSPACE));
            assertEquals("ONE", props.getProperty(KEY_CONSISTENCY_LEVEL));

            url = "jdbc:c*://host1";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertNull(props.getProperty(KEY_PROVIDER));
            assertEquals("host1", props.getProperty(KEY_HOSTS));
            assertNull(props.getProperty(KEY_KEYSPACE));

            url = "jdbc:c*://host2/?cc=1";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertNull(props.getProperty(KEY_PROVIDER));
            assertEquals("host2", props.getProperty(KEY_HOSTS));
            assertNull(props.getProperty(KEY_KEYSPACE));
            assertEquals("1", props.getProperty("cc"));

            url = "jdbc:c*://host3/system_auth";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertNull(props.getProperty(KEY_PROVIDER));
            assertEquals("host3", props.getProperty(KEY_HOSTS));
            assertEquals("system_auth", props.getProperty(KEY_KEYSPACE));

            url = "jdbc:c*://host4?a=b&c=1&consistencyLevel=ANY&compression=lz4&connectionTimeout=10&readTimeout=50&localDc=DD";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertNull(props.getProperty(KEY_PROVIDER));
            assertEquals("host4", props.getProperty(KEY_HOSTS));
            assertNull(props.getProperty(KEY_KEYSPACE));
            assertEquals("b", props.getProperty("a"));
            assertEquals("1", props.getProperty("c"));
            assertEquals("ANY", props.getProperty(KEY_CONSISTENCY_LEVEL));
            assertEquals("lz4", props.getProperty(KEY_COMPRESSION));
            assertEquals("10", props.getProperty(KEY_CONNECTION_TIMEOUT));
            assertEquals("50", props.getProperty(KEY_READ_TIMEOUT));
            assertEquals("DD", props.getProperty(KEY_LOCAL_DC));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }
}
