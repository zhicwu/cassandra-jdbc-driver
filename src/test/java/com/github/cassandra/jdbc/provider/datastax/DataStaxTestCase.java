package com.github.cassandra.jdbc.provider.datastax;

import com.github.cassandra.jdbc.CassandraDriver;
import org.junit.After;
import org.junit.Before;

import java.util.Properties;

public class DataStaxTestCase {
    protected CassandraConnection conn;

    @Before
    public void setUp() throws Exception {
        CassandraDriver driver = new CassandraDriver();

        Properties props = new Properties();
        props.setProperty("user", "cassandra");
        props.setProperty("password", "cassandra");
        conn = (CassandraConnection) driver
                .connect("jdbc:c*:datastax://localhost/system?consistencyLevel=ONE&queryTrace=true", props);
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
}
