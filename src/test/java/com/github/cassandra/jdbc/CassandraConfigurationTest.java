package com.github.cassandra.jdbc;

import org.junit.Test;

import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraConfiguration.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CassandraConfigurationTest {

    @Test
    public void testNormalizeSql() {
        String sql = "select tbl.key,tbl.bootstrapped,tbl.cluster_name,tbl.cql_version,tbl.data_center,tbl.dse_version,tbl.gossip_generation,tbl.host_id,tbl.native_protocol_version,tbl.partitioner,tbl.rack,tbl.release_version,tbl.schema_version,tbl.thrift_version,tbl.tokens,tbl.truncated_at,tbl.workload from \"system\".\"local\" tbl";

        try {
            CassandraUtils.normalizeSql(sql, true, false);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }

    @Test
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
            assertEquals("datastax", props.getProperty(KEY_PROVIDER));
            assertEquals("host1", props.getProperty(KEY_HOSTS));
            assertEquals("system", props.getProperty(KEY_KEYSPACE));

            url = "jdbc:c*://host2/?cc=1";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertEquals("datastax", props.getProperty(KEY_PROVIDER));
            assertEquals("host2", props.getProperty(KEY_HOSTS));
            assertEquals("system", props.getProperty(KEY_KEYSPACE));
            assertEquals("1", props.getProperty("cc"));

            url = "jdbc:c*://host3/system_auth";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertEquals("datastax", props.getProperty(KEY_PROVIDER));
            assertEquals("host3", props.getProperty(KEY_HOSTS));
            assertEquals("system_auth", props.getProperty(KEY_KEYSPACE));

            url = "jdbc:c*://host4?a=b&c=1&consistencyLevel=ANY&compression=lz4&connectTimeout=10&readTimeout=50&localDc=DD";
            props = CassandraConfiguration.parseConnectionURL(url);
            assertEquals("datastax", props.getProperty(KEY_PROVIDER));
            assertEquals("host4", props.getProperty(KEY_HOSTS));
            assertEquals("system", props.getProperty(KEY_KEYSPACE));
            assertEquals("b", props.getProperty("a"));
            assertEquals("1", props.getProperty("c"));
            assertEquals("ANY", props.getProperty(KEY_CONSISTENCY_LEVEL));
            assertEquals("lz4", props.getProperty(KEY_COMPRESSION));
            assertEquals("10", props.getProperty(KEY_CONNECT_TIMEOUT));
            assertEquals("50", props.getProperty(KEY_READ_TIMEOUT));
            assertEquals("DD", props.getProperty(KEY_LOCAL_DC));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }
}
