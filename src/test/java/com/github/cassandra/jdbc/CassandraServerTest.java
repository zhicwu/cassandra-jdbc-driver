/**
 * Copyright (C) 2015-2016, Zhichun Wu
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


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.VersionNumber;
import com.google.common.base.Splitter;
import com.google.common.io.CharStreams;
import org.pmw.tinylog.Logger;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStreamReader;

public class CassandraServerTest {
    @BeforeGroups(groups = {"server"})
    public void createKeyspaceAndTables() {
        String scripts = "";
        InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("/create.cql"));
        try {
            scripts = CharStreams.toString(reader);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {

                }
            }
        }

        Cluster cluster = null;

        try {
            CassandraConfiguration conf = CassandraConfiguration.DEFAULT;
            Cluster.Builder builder = Cluster.builder();
            for (String host : Splitter.on(',').trimResults().omitEmptyStrings().split(conf.getHosts())) {
                builder.addContactPoint(host);
            }
            if (conf.getPort() > 0) {
                builder.withPort(conf.getPort());
            }
            cluster = builder.withCredentials(conf.getUserName(), conf.getPassword()).build();
            Session session = cluster.newSession();

            VersionNumber cassandraVersion = cluster.getMetadata().getAllHosts().iterator().next().getCassandraVersion();
            CassandraTestHelper.init(cassandraVersion.getMajor(), cassandraVersion.getMinor());
            scripts = CassandraTestHelper.getInstance().replaceScript(scripts);

            for (String cql : Splitter.on(';').trimResults().omitEmptyStrings().split(scripts)) {
                Logger.debug("Executing:\n{}\n", cql);
                session.execute(cql);
            }
            cluster.close();
            cluster = null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (cluster != null && !cluster.isClosed()) {
                cluster.closeAsync().force();
            }
            cluster = null;
        }
    }

    @Test(groups = {"server"})
    public void testConnection() {

    }
}
