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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;
import org.yaml.snakeyaml.Yaml;

import java.lang.reflect.Field;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;

public final class CassandraConfiguration {
    public static final class DriverConfig {
        public String provider = "datastax";
        public String hosts = "localhost";
        public int port = -1;
        public String keyspace = "system";
        public String user = "cassandra";
        public String password = "cassandra";
        public boolean quiet = true;
        public CassandraEnums.ConsistencyLevel readConsistencyLevel = CassandraEnums.ConsistencyLevel.LOCAL_ONE;
        public CassandraEnums.ConsistencyLevel writeConsistencyLevel = CassandraEnums.ConsistencyLevel.ANY;
        public CassandraEnums.ConsistencyLevel consistencyLevel = readConsistencyLevel;
        public boolean sqlFriendly = true;
        public boolean tracing = false;
        public CassandraEnums.Batch batch = CassandraEnums.Batch.UNLOGGED;
        public int fetchSize = 100;
        public long rowLimit = 10000L;
        public int cqlCacheSize = 1000;
        public int readTimeout = 30 * 1000;
        public int connectionTimeout = 5 * 1000;
        public boolean keepAlive = true;
        public CassandraEnums.Compression compression = CassandraEnums.Compression.LZ4;
        public String localDc = "";
        public String loadBalancingPolicy = "";
        public String fallbackPolicy = "";

        // FIXME needs a better way to manage provider-specific configuration
        Properties advanced = new Properties();

        public DriverConfig() {
        }

        public SortedMap<String, Object> toSortedMap() {
            SortedMap<String, Object> map = Maps.newTreeMap();

            Field[] fields = DriverConfig.class.getFields();

            for (Field field : fields) {
                try {
                    map.put(field.getName(), field.get(this));
                } catch (IllegalAccessException e) {
                    // ignore non-public fields
                }
            }

            return map;
        }

        Properties toProperties() {
            Properties props = new Properties();

            props.putAll(toSortedMap());

            return props;
        }
    }

    public static final class LoggerConfig {
        public Level level = Level.INFO;
        public int stacktrace = -1;
        public String format = "{date:yyyy-MM-dd HH:mm:ss} [{thread}] {class_name}.{method}({line}) {level}: {message}";
    }

    public static final class YamlConfig {
        public String version = "0.1.0";
        public Locale locale = Locale.US;

        public DriverConfig driver = new DriverConfig();
        public LoggerConfig logger = new LoggerConfig();
    }

    public static final Splitter versionSplitter = Splitter.on('.').omitEmptyStrings().trimResults();

    public static final String DRIVER_NAME = "Cassandra JDBC Driver";
    public static final String DRIVER_VERSION;
    public static final int VERSION_MAJOR;
    public static final int VERSION_MINOR;
    // static final int VERSION_PATCH;

    public static final String KEY_COMPRESSION = "compression";

    public static final String KEY_CONNECTION_TIMEOUT = "connectionTimeout";

    public static final String KEY_CONNECTION_URL = "url";

    public static final String KEY_CONSISTENCY_LEVEL = "consistencyLevel";
    public static final String KEY_FETCH_SIZE = "fetchSize";
    public static final String KEY_HOSTS = "hosts";
    public static final String KEY_PORT = "port";
    public static final String KEY_KEEP_ALIVE = "keepAlive";
    public static final String KEY_KEYSPACE = "keyspace";
    public static final String KEY_LOCAL_DC = "localDc";
    public static final String KEY_USERNAME = "user";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_PROVIDER = "provider";
    public static final String KEY_QUERY_TRACE = "queryTrace";
    public static final String KEY_QUIET = "quiet";
    public static final String KEY_READ_TIMEOUT = "readTimeout";
    public static final String KEY_SQL_FRIENDLY = "sqlFriendly";

    static final String INVALID_URL = "Invalid connection URL";

    static final String DRIVER_PROTOCOL = "jdbc:c*:";

    static final String TOKEN_KVP_SEPARATOR = "=";

    static final String TOKEN_PARAM_SEPARATOR = "&";
    static final String TOKEN_PROTO_SEPARATOR = ":";

    static final String TOKEN_URL_SEPARATOR = "//";

    static final String YAML_KVP_SEPARATOR = " : ";

    static final CassandraConfiguration DEFAULT;

    static {
        String configUrl = System.getProperty("cassandra.jdbc.driver.config");

        URL url = null;
        if (!Strings.isNullOrEmpty(configUrl)) {
            try {
                url = new URL(configUrl);
                url.openStream().close(); // catches well-formed but bogus URLs
            } catch (Exception e) {
                ClassLoader loader = CassandraConfiguration.class.getClassLoader();
                url = loader.getResource(configUrl);
            }
        }

        Yaml yaml = new Yaml();
        YamlConfig defaultConfig = new YamlConfig();
        try {
            defaultConfig = yaml.loadAs(url != null
                            ? url.openStream() : CassandraConfiguration.class.getResourceAsStream("/config.yaml"),
                    YamlConfig.class);
        } catch (Throwable t) {
            // log before configuration is ready...
            Logger.warn(t, "Failed to load default configuration, but that's cool");
        }

        // configure tinylog
        Configurator.defaultConfig()
                .formatPattern(defaultConfig.logger.format)
                .level(defaultConfig.logger.level)
                .locale(defaultConfig.locale)
                .maxStackTraceElements(defaultConfig.logger.stacktrace)
                .activate();

        Logger.info("Configuration loaded from {}", url == null ? "(embedded)config.yaml" : url.toString());

        DRIVER_VERSION = Strings.isNullOrEmpty(defaultConfig.version) ? "0.1.0" : defaultConfig.version;

        List<String> versions = versionSplitter.splitToList(DRIVER_VERSION);
        if (versions.size() > 1) {
            VERSION_MAJOR = Ints.tryParse(versions.get(0));
            VERSION_MINOR = Ints.tryParse(versions.get(1));
        } else {
            VERSION_MAJOR = 0;
            VERSION_MINOR = 1;
        }

        try {
            DEFAULT = new CassandraConfiguration(defaultConfig.driver);
        } catch (SQLException e) {
            throw CassandraErrors.unexpectedException(e);
        }
    }

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

        // example URL: jdbc:c*:datastax://host1:9160,host2/keyspace1?consistency=LOCAL_ONE
        String[] parts = url.split(TOKEN_URL_SEPARATOR);
        boolean invalidUrl = true;

        if (parts.length == 2) {
            // get provider
            String provider = parts[0].substring(DRIVER_PROTOCOL.length());
            if (!Strings.isNullOrEmpty(provider)) {
                provider = provider.split(TOKEN_PROTO_SEPARATOR)[0];
                props.setProperty(KEY_PROVIDER, provider);
            }

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
                if (!Strings.isNullOrEmpty(keyspace)) {
                    props.setProperty(KEY_KEYSPACE, keyspace);
                }
            } else {
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

    static DriverConfig generateDriverConfig(Properties props) {
        Properties current = DEFAULT.config.toProperties();
        current.putAll(props);

        StringBuilder builder = new StringBuilder();
        for (Map.Entry entry : current.entrySet()) {
            String key = (String) entry.getKey();
            builder.append(key).append(YAML_KVP_SEPARATOR).append(entry.getValue()).append('\n');
        }

        return new Yaml().loadAs(builder.toString().trim(), DriverConfig.class);
    }

    static String buildSimplifiedConnectionUrl(DriverConfig config) {
        StringBuilder builder = new StringBuilder(DRIVER_PROTOCOL);
        builder.append(config.provider)
                .append(':')
                .append(TOKEN_URL_SEPARATOR)
                .append(config.hosts);

        if (config.port > 0) {
            builder.append(':').append(config.port);
        }

        builder.append('/')
                .append(config.keyspace)
                .append('?').append(KEY_USERNAME).append('=')
                .append(config.user);

        return builder.toString();
    }

    private final boolean autoCommit = true;
    private final boolean readOnly = false;
    private final String connectionUrl;
    private final DriverConfig config;

    private void init() {
        int tentativePort = config.port;
        Splitter splitter = Splitter.on(':').trimResults().omitEmptyStrings().limit(2);
        StringBuilder sb = new StringBuilder();
        for (String host : Splitter.on(',').trimResults().omitEmptyStrings().split(
                config.hosts)) {
            List<String> h = splitter.splitToList(host);
            sb.append(h.get(0)).append(',');
            if (h.size() > 1 && tentativePort <= 0) {
                tentativePort = Ints.tryParse(h.get(1));
            }
        }

        config.hosts = sb.deleteCharAt(sb.length() - 1).toString();
        config.port = tentativePort;

        // update timeouts
        config.connectionTimeout = config.connectionTimeout * 1000;
        config.readTimeout = config.readTimeout * 1000;
    }

    private CassandraConfiguration(DriverConfig config) throws SQLException {
        this.config = config;
        init();
        this.connectionUrl = buildSimplifiedConnectionUrl(config);
    }

    public CassandraConfiguration(String url, Properties props) throws SQLException {
        Properties connProps = new Properties();

        connProps.putAll(parseConnectionURL(url));
        connProps.putAll(props);

        config = generateDriverConfig(connProps);

        init();

        connectionUrl = buildSimplifiedConnectionUrl(config);
    }

    public String getProvider() {
        return config.provider;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getUserName() {
        return config.user;
    }

    public String getPassword() {
        return config.password;
    }

    public String getHosts() {
        return config.hosts;
    }

    public int getPort() {
        return config.port;
    }

    public String getKeyspace() {
        return config.keyspace;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isSqlFriendly() {
        return config.sqlFriendly;
    }

    public boolean isQuiet() {
        return config.quiet;
    }

    public boolean isTracingEnabled() {
        return config.tracing;
    }

    public int getConnectionTimeout() {
        return config.connectionTimeout;
    }

    public int getReadTimeout() {
        return config.readTimeout;
    }

    public boolean isKeepAlive() {
        return config.keepAlive;
    }

    public CassandraEnums.ConsistencyLevel getReadConsistencyLevel() {
        return config.readConsistencyLevel;
    }

    public CassandraEnums.ConsistencyLevel getWriteConsistencyLevel() {
        return config.writeConsistencyLevel;
    }

    public CassandraEnums.ConsistencyLevel getConsistencyLevel() {
        return config.consistencyLevel;
    }

    @Deprecated
    public String getLocalDc() {
        return config.localDc;
    }

    public String getLoadBalancingPolicy() {
        return config.loadBalancingPolicy;
    }

    public String getFallbackPolicy() {
        return config.fallbackPolicy;
    }

    public CassandraEnums.Batch getBatch() {
        return config.batch;
    }

    public int getFetchSize() {
        return config.fetchSize;
    }

    public long getRowLimit() {
        return config.rowLimit;
    }

    public int getCqlCacheSize() {
        return config.cqlCacheSize;
    }

    public CassandraEnums.Compression getCompression() {
        return config.compression;
    }

    public String getAdditionalProperty(String key, String defaultValue) {
        return config.advanced.getProperty(key, defaultValue);
    }

    public int getAdditionalProperty(String key, int defaultValue) {
        String value = getAdditionalProperty(key, null);
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.valueOf(value);
    }

    public Properties toProperties() {
        return config.toProperties();
    }

    public SortedMap<String, Object> toSortedMap() {
        return config.toSortedMap();
    }
}
