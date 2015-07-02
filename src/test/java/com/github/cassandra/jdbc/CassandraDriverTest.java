package com.github.cassandra.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

import static com.github.cassandra.jdbc.CassandraUtils.KEY_PASSWORD;
import static com.github.cassandra.jdbc.CassandraUtils.KEY_USERNAME;
import static org.junit.Assert.*;

public class CassandraDriverTest {

	@Test
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

	@Test
	public void testConnect() {
		CassandraDriver driver = new CassandraDriver();
		Properties props = new Properties();
		props.setProperty(KEY_USERNAME, "dse");
		props.setProperty(KEY_PASSWORD, "111");
		try {
			Connection conn = driver
					.connect(
							"jdbc:c*:datastax://localhost/system_auth?compression=snappy&consistencyLevel=ONE",
							props);
			assertTrue(conn instanceof BaseCassandraConnection);
			assertTrue(conn.getClass().getName()
					.endsWith("datastax.CassandraConnection"));

			conn.setCatalog("system");
			ResultSet rs = conn.createStatement().executeQuery(
					"select * from peers limit 5");
			while (rs.next()) {
				System.out.println(rs.getRow() + "\n=====");
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					Object obj = rs.getObject(i);
					System.out.println(new StringBuilder()
							.append("[")
							.append(rs.getMetaData().getColumnName(i))
							.append("]=[")
							.append(obj == null ? "null" : obj.getClass() + "@"
									+ obj.hashCode()).append("]").toString());
				}
			}
			rs.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception happened during test: " + e.getMessage());
		}
	}
}
