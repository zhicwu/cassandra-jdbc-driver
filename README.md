# cassandra-jdbc-driver
Cassandra JDBC driver that works with 2.x and above. 

## What is this?
This is nothing but a JDBC driver build on top of existing popular java clients(e.g. DataStax Java Driver). It can be used with SQuirreL SQL for development and Pentaho BI Server for data analysis and reporting.

## How to use?
#### Hello World
```java
...
Driver driver = new com.github.cassandra.jdbc.CassandraDriver();
Properties props = new Properties();
props.setProperty("user", "cassandra");
props.setProperty("password", "cassandra");

Connection conn = driver.connect("java:c*:datastax://localhost/system_auth?consistency=ONE");
conn.setCatalog("system");

ResultSet rs = conn.createStatement().executeQuery("select * from peers limit 10");
while (rs.next()) {
...
}
...
```
#### Connection Properties
TBD
