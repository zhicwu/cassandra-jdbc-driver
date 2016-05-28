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
package com.github.cassandra.jdbc;

import com.google.common.base.Strings;

import java.sql.SQLException;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraUtils.EMPTY_STRING;

public final class CassandraConfiguration {
    static final String INVALID_URL = "Invalid connection URL";

    static final String DEFAULT_FETCH_SIZE = "100";
    static final String DEFAULT_HOSTS = "localhost";
    static final String DEFAULT_KEEP_ALIVE = "true";
    static final String DEFAULT_KEYSPACE = "system";

    // default settings
    static final String DEFAULT_PROVIDER = "datastax";
    static final String DEFAULT_QUERY_TRACE = "false";
    static final String DEFAULT_READ_TIMEOUT = "30"; // 30 seconds
    static final String DEFAULT_QUIET = "true";
    static final String DEFAULT_SQL_FRIENDLY = "true";
    static final String DEFAULT_USERNAME = "cassandra";
    static final String DEFAULT_COMPRESSION = "LZ4";
    static final String DEFAULT_CONNECT_TIMEOUT = "5"; // 5 seconds
    static final String DEFAULT_CONSISTENCY_LEVEL = "LOCAL_ONE";

    static final String DRIVER_PROTOCOL = "jdbc:c*:";

    static final String KEY_COMPRESSION = "compression";

    static final String KEY_CONNECT_TIMEOUT = "connectTimeout";

    static final String KEY_CONNECTION_URL = "url";

    static final String KEY_CONSISTENCY_LEVEL = "consistencyLevel";
    static final String KEY_FETCH_SIZE = "fetchSize";
    static final String KEY_HOSTS = "hosts";
    static final String KEY_KEEP_ALIVE = "keepAlive";
    static final String KEY_KEYSPACE = "keyspace";
    static final String KEY_LOCAL_DC = "localDc";
    static final String KEY_USERNAME = "user";
    static final String KEY_PASSWORD = "password";
    static final String KEY_PROVIDER = "provider";
    static final String KEY_QUERY_TRACE = "queryTrace";
    static final String KEY_QUIET = "quiet";
    static final String KEY_READ_TIMEOUT = "readTimeout";
    static final String KEY_SQL_FRIENDLY = "sqlFriendly";

    static final String TOKEN_KVP_SEPARATOR = "=";

    static final String TOKEN_PARAM_SEPARATOR = "&";
    static final String TOKEN_PROTO_SEPARATOR = ":";

    static final String TOKEN_URL_SEPARATOR = "//";

    private final boolean autoCommit = true;
    private final boolean readOnly = false;
    private final String provider;
    private final String connectionUrl;
    private final String userName;
    private final String password;
    private final String hosts;
    private final String keyspace;
    private final boolean quiet;
    private final boolean sqlFriendly;
    private final boolean queryTrace;
    private final int connectionTimeout;
    private final int readTimeout;
    private final boolean keepAlive;
    private final String consistencyLevel;
    private final String localDc;
    private final String loadBalancingPolicy;
    private final String fallbackPolicy;
    private final int fetchSize;
    private final String compression;

    private final Properties additionalProperties;


    static boolean isValidUrl(String url) {
        return !Strings.isNullOrEmpty(url) && url.startsWith(DRIVER_PROTOCOL);
    }

    /**
     * Extract properties from given non-null connection URL.
     *
     * @param url connection URL
     * @return properties extracted from the given URL
     * @throws SQLException when failed to parse given URL
     */
    static Properties parseConnectionURL(String url) throws SQLException {
        Properties props = new Properties();

        // example URL:
        // jdbc:c*:datastax://host1:9160,host2/keyspace1?consistency=LOCAL_ONE
        String[] parts = url.split(TOKEN_URL_SEPARATOR);
        boolean invalidUrl = true;

        if (parts.length == 2) {
            // get provider
            String provider = parts[0].substring(DRIVER_PROTOCOL.length());
            if (Strings.isNullOrEmpty(provider)) {
                provider = DEFAULT_PROVIDER;
            } else {
                // this will also ignore extra protocol codes like ":a:b:c:d:"
                provider = provider.split(TOKEN_PROTO_SEPARATOR)[0];
            }
            props.setProperty(KEY_PROVIDER, provider);

            String restUrl = parts[1];
            int ksIdx = restUrl.indexOf('/');
            int pIdx = restUrl.indexOf('?');
            if (ksIdx > 0) {
                // get hosts
                String hosts = restUrl.substring(0, ksIdx);
                props.setProperty(KEY_HOSTS, hosts);

                // get keyspace
                String keyspace = restUrl.substring(ksIdx + 1,
                        pIdx > ksIdx ? pIdx : restUrl.length());
                if (Strings.isNullOrEmpty(keyspace)) {
                    keyspace = DEFAULT_KEYSPACE;
                }
                props.setProperty(KEY_KEYSPACE, keyspace);
            } else {
                props.setProperty(KEY_KEYSPACE, DEFAULT_KEYSPACE);
                props.setProperty(KEY_HOSTS,
                        pIdx > 0 ? restUrl.substring(0, pIdx) : restUrl);
            }

            invalidUrl = false;

            // now let's see if there's any optional parameters
            if (pIdx > ksIdx) {
                String[] params = restUrl.substring(pIdx + 1, restUrl.length())
                        .split(TOKEN_PARAM_SEPARATOR);
                for (String param : params) {
                    String[] kvPair = param.split(TOKEN_KVP_SEPARATOR);
                    if (kvPair.length == 2) {
                        String key = kvPair[0].trim();
                        String value = kvPair[1].trim();

                        if (!Strings.isNullOrEmpty(key)) {
                            props.setProperty(key, value);
                        }
                    }
                }
            }
        }

        if (invalidUrl) {
            throw new SQLException(INVALID_URL);
        }

        return props;
    }

    static String extractProperty(Properties props, String key, String defaultValue) {
        String value = (String) props.remove(key);
        return value == null ? defaultValue : value;
    }

    static String buildSimplifiedConnectionUrl(Properties props) {
        StringBuilder builder = new StringBuilder(DRIVER_PROTOCOL);
        builder.append(props.getProperty(KEY_PROVIDER, DEFAULT_PROVIDER))
                .append(':')
                .append(TOKEN_URL_SEPARATOR)
                .append(props.getProperty(KEY_HOSTS, DEFAULT_HOSTS))
                .append('/')
                .append(props.getProperty(KEY_KEYSPACE, DEFAULT_KEYSPACE))
                .append('?').append(KEY_USERNAME).append('=')
                .append(props.getProperty(KEY_USERNAME, DEFAULT_USERNAME));

        return builder.toString();
    }

    public CassandraConfiguration(String url, Properties props) throws SQLException {
        Properties connProps = new Properties();

        connProps.putAll(parseConnectionURL(url));
        connProps.putAll(props);

        provider = extractProperty(connProps, KEY_PROVIDER, DEFAULT_PROVIDER);
        connectionUrl = buildSimplifiedConnectionUrl(connProps);
        userName = extractProperty(connProps, KEY_USERNAME, DEFAULT_USERNAME);
        password = extractProperty(connProps, KEY_PASSWORD, DEFAULT_USERNAME);
        hosts = extractProperty(connProps, KEY_HOSTS, DEFAULT_HOSTS);
        keyspace = extractProperty(connProps, KEY_KEYSPACE, DEFAULT_KEYSPACE);
        quiet = Boolean.valueOf(extractProperty(connProps, KEY_QUIET, DEFAULT_QUIET).toLowerCase());
        sqlFriendly = Boolean.valueOf(extractProperty(connProps, KEY_SQL_FRIENDLY, DEFAULT_SQL_FRIENDLY).toLowerCase());
        queryTrace = Boolean.valueOf(extractProperty(connProps, KEY_QUERY_TRACE, DEFAULT_QUERY_TRACE).toLowerCase());
        connectionTimeout = Integer.parseInt(extractProperty(connProps, KEY_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT)) * 1000;
        readTimeout = Integer.parseInt(extractProperty(connProps, KEY_READ_TIMEOUT, DEFAULT_READ_TIMEOUT)) * 1000;
        keepAlive = Boolean.valueOf(extractProperty(connProps, KEY_KEEP_ALIVE, DEFAULT_KEEP_ALIVE).toLowerCase());
        consistencyLevel = extractProperty(connProps, KEY_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL).toUpperCase();
        localDc = extractProperty(connProps, KEY_LOCAL_DC, EMPTY_STRING);
        loadBalancingPolicy = "";
        fallbackPolicy = "";
        fetchSize = Integer.parseInt(extractProperty(connProps, KEY_FETCH_SIZE, DEFAULT_FETCH_SIZE));
        compression = extractProperty(connProps, KEY_COMPRESSION, DEFAULT_COMPRESSION).toUpperCase();

        additionalProperties = connProps;
    }

    public String getProvider() {
        return provider;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getHosts() {
        return hosts;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isSqlFriendly() {
        return sqlFriendly;
    }

    public boolean isQuiet() {
        return quiet;
    }

    public boolean isQueryTrace() {
        return queryTrace;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    @Deprecated
    public String getLocalDc() {
        return localDc;
    }

    public String getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    public String getFallbackPolicy() {
        return fallbackPolicy;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public String getCompression() {
        return compression;
    }

    public boolean containsAdditionalProperty(String key) {
        return additionalProperties.containsKey(key);
    }

    public String getAdditionalProperty(String key, String defaultValue) {
        return additionalProperties.getProperty(key, defaultValue);
    }

    public int getAdditionalProperty(String key, int defaultValue) {
        String value = getAdditionalProperty(key, EMPTY_STRING);
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.valueOf(value);
    }

    public Properties getAdditionalProperties() {
        Properties props = new Properties();
        props.putAll(additionalProperties);
        return props;
    }
}
