package com.github.cassandra.jdbc.provider.datastax;

import static com.github.cassandra.jdbc.CassandraUtils.KEY_PASSWORD;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import com.github.cassandra.jdbc.CassandraDriver;

public class DataStaxTestCase {
	protected CassandraConnection conn;

	@Before
	public void setUp() throws Exception {
		CassandraDriver driver = new CassandraDriver();
		Properties props = new Properties();
		props.setProperty(KEY_USERNAME, "cassandra");
		props.setProperty(KEY_PASSWORD, "cassandra");
		conn = (CassandraConnection) driver.connect(
				"jdbc:c*:datastax://localhost/system?consistencyLevel=ONE",
				props);
	}

	@After
	public void tearDown() throws Exception {
		conn.close();
	}
}
