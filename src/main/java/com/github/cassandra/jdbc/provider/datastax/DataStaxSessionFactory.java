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
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.github.cassandra.jdbc.CassandraConfiguration;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.pmw.tinylog.Logger;

import java.util.concurrent.Callable;

import static com.github.cassandra.jdbc.CassandraUtils.KEY_PORT;

/**
 * Session factory.
 */
final class DataStaxSessionFactory {
    private final static Cache<String, DataStaxSessionWrapper> _sessionCache;

    static {
        // FIXME when exactly to close the cluster object?
        _sessionCache = CacheBuilder.newBuilder().weakValues().removalListener(new RemovalListener<String, DataStaxSessionWrapper>() {
            public void onRemoval(RemovalNotification<String, DataStaxSessionWrapper> notification) {
                DataStaxSessionWrapper session = notification.getValue();

                Logger.debug(new StringBuilder().append("Closing [")
                        .append(session).append("] (cause: ").append(notification.getCause()).append(")...").toString());
                if (session != null) {
                    try {
                        session.close();
                    } catch (Throwable t) {
                        Logger.debug("Error occurred when closing session", t);
                    }
                }

                Logger.debug(new StringBuilder().append("Closed [")
                        .append(session).append("].").toString());
            }
        }).build();
    }

    private static DataStaxSessionWrapper newSession(CassandraConfiguration config) {
        return newSession(config, null);
    }

    private static DataStaxSessionWrapper newSession(CassandraConfiguration config, String keyspace) {
        keyspace = Strings.isNullOrEmpty(keyspace)
                || config.getKeyspace().equals(keyspace) ? config.getKeyspace() : keyspace;

        Logger.debug(new StringBuilder().append("Connecting to [")
                .append(config.getConnectionUrl()).append("]...").toString());

        Cluster.Builder builder = Cluster.builder();

        // add contact points
        for (String host : Splitter.on(",").trimResults().split(config.getHosts())) {
            builder.addContactPoint(host);
        }

        // set port if specified
        if (config.containsAdditionalProperty(KEY_PORT)) {
            builder.withPort(config.getAdditionalProperty(KEY_PORT, -1));
        }

        // set socket options
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(config.getConnectionTimeout());
        socketOptions.setReadTimeoutMillis(config.getReadTimeout());
        socketOptions.setKeepAlive(config.isKeepAlive());
        builder.withSocketOptions(socketOptions);

        // set query options
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.valueOf(config.getConsistencyLevel()));
        queryOptions.setFetchSize(config.getFetchSize());
        builder.withQueryOptions(queryOptions);

        // set pool options - use same defaults as in V3
        PoolingOptions poolOptions = new PoolingOptions();
        poolOptions.setConnectionsPerHost(HostDistance.LOCAL,
                config.getAdditionalProperty("corePoolLocal", 1),
                config.getAdditionalProperty("maxPoolLocal", 1));
        poolOptions.setConnectionsPerHost(HostDistance.REMOTE,
                config.getAdditionalProperty("corePoolRemote", 1),
                config.getAdditionalProperty("maxPoolRemote", 1));
        poolOptions.setIdleTimeoutSeconds(
                config.getAdditionalProperty("idleTimeoutSeconds", poolOptions.getIdleTimeoutSeconds()));
        poolOptions.setPoolTimeoutMillis(
                config.getAdditionalProperty("poolTimeoutMillis", poolOptions.getPoolTimeoutMillis()));
        poolOptions.setHeartbeatIntervalSeconds(
                config.getAdditionalProperty("heartbeatIntervalSeconds", poolOptions.getHeartbeatIntervalSeconds()));
        poolOptions.setMaxRequestsPerConnection(HostDistance.LOCAL,
                config.getAdditionalProperty("maxRequestsPerConnectionLocal", 800));
        poolOptions.setMaxRequestsPerConnection(HostDistance.REMOTE,
                config.getAdditionalProperty("maxRequestsPerConnectionRemote", 200));
        poolOptions.setNewConnectionThreshold(HostDistance.LOCAL,
                config.getAdditionalProperty("newConnectionThresholdLocal", 1024));
        poolOptions.setNewConnectionThreshold(HostDistance.REMOTE,
                config.getAdditionalProperty("newConnectionThresholdRemote", 256));
        builder.withPoolingOptions(poolOptions);

        if (!Strings.isNullOrEmpty(config.getLocalDc())) {
            builder.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(config.getLocalDc()).build());
        }

        // set compression
        builder.withCompression(ProtocolOptions.Compression.valueOf(config.getCompression()));

        // build the cluster
        Cluster cluster = builder.withCredentials(config.getUserName(),
                config.getPassword()).build();

        Logger.debug(new StringBuilder().append("Connected to [")
                .append(config.getConnectionUrl()).append('(').append(cluster.hashCode())
                .append(')').append("] successfully").toString());

        Metadata metadata = cluster.getMetadata();

        Logger.info(new StringBuilder("Connected to cluster: ").append(
                metadata.getClusterName()).toString());
        for (Host host : metadata.getAllHosts()) {
            Logger.info(new StringBuilder("-> Datacenter: ")
                    .append(host.getDatacenter()).append("; Host: ")
                    .append(host.getAddress()).append("; Rack: ")
                    .append(host.getRack()).toString());
        }

        return new DataStaxSessionWrapper(cluster.connect(keyspace));
    }

    static DataStaxSessionWrapper getSession(final CassandraConfiguration config) {
        return getSession(config, null);
    }

    static DataStaxSessionWrapper getSession(final CassandraConfiguration config, final String keyspace) {
        final String targetKeyspace = Strings.isNullOrEmpty(keyspace)
                || config.getKeyspace().equals(keyspace) ? config.getKeyspace() : keyspace;
        DataStaxSessionWrapper session = null;

        try {
            session = _sessionCache.get(config.getConnectionUrl(), new Callable<DataStaxSessionWrapper>() {
                public DataStaxSessionWrapper call() throws Exception {
                    return newSession(config, targetKeyspace);
                }
            });

            if (session.isClosed() || !session.getLoggedKeyspace().equals(targetKeyspace)) {
                // FIXME this will cause connection issues in other threads
                _sessionCache.invalidate(config.getConnectionUrl());
                session = getSession(config, targetKeyspace);
            }

            // active the session and increase the reference counter
            session.open();
        } catch (Exception e) {
            Logger.error(e, "Failed to obtain session object");
            throw new RuntimeException(e);
        }

        return session;
    }
}
