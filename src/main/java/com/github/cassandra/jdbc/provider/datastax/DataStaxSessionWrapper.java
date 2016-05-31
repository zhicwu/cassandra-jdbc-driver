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


import com.datastax.driver.core.*;
import com.github.cassandra.jdbc.CassandraErrors;
import org.pmw.tinylog.Logger;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class DataStaxSessionWrapper implements AutoCloseable {
    private final AtomicInteger references = new AtomicInteger(0);

    private Session session;

    protected DataStaxSessionWrapper(Session session) {
        this.session = session;
    }

    protected void validateState() throws SQLException {
        if (session == null || session.isClosed()) {
            session = null;
            throw CassandraErrors.connectionClosedException();
        }
    }

    protected ResultSet execute(String cql) throws SQLException {
        validateState();

        return session.execute(cql);
    }

    protected ResultSet execute(Statement statement) throws SQLException {
        validateState();

        return session.execute(statement);
    }

    protected Metadata getClusterMetaData() throws SQLException {
        validateState();

        return session.getCluster().getMetadata();
    }

    protected PreparedStatement prepare(String cql) throws SQLException {
        validateState();

        return session.prepare(cql);
    }

    protected String getLoggedKeyspace() throws SQLException {
        validateState();

        return session.getLoggedKeyspace();
    }

    public boolean isClosed() {
        return session == null || session.isClosed();
    }

    public void open() {
        references.incrementAndGet();
    }

    public void close() throws Exception {
        if (session == null || references.decrementAndGet() <= 0) {
            if (session != null) {
                Cluster cluster = session.getCluster();
                if (DataStaxClusterHelper.hasLiveSessions(cluster)) {
                    cluster.closeAsync().force();
                    Logger.info("Closing cluster@{} and all sessions underneath", cluster.hashCode());
                } else {
                    session.close();
                    Logger.info("Session@{} is closed", session.hashCode());
                }
                session = null;
            }
        }
    }
}
