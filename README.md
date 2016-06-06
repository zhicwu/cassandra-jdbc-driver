# cassandra-jdbc-driver
Cassandra JDBC driver that works with 2.x and above. It intends to provide better SQL compatibility over CQL,
so that it works well with existing tools like [SQuirreL SQL](http://www.squirrelsql.org/) for SQL development,
[JMeter](http://jmeter.apache.org) for stress testing, and [Pentaho BI Suite](http://community.pentaho.com/)
for data processing.

## What is this?
Basically this is a JDBC adaptor that talks to Cassandra using popular Java driver like
[DataStax Java Driver](https://github.com/datastax/java-driver/). Besides, it uses
[JSqlParser](https://github.com/JSQLParser/JSqlParser) to parse and translate the given SQL to CQL before execution.

You may find this helpful if you came from RDBMS world and was searching for a JDBC driver works with latest version
of Cassandra. Having said that, it is NOT recommended to use this for production but development and research.

OK, you have been warned :) Now go ahead to download the latest driver and give it a shot!

## Where we are?
[0.5.0 Release](https://github.com/zhicwu/cassandra-jdbc-driver/releases/tag/0.5.0) - Beta

## What's next?
- Advanced types(LOBs, Collections and UDTs) support
- Multiple ResultSet support
- Better SQL compatibility(e.g. SELECT INTO, GROUP BY and probably simple table joins and sub-queries)
- More providers...

## How to use?
#### Hello World
```java
...
// Driver driver = new com.github.cassandra.jdbc.CassandraDriver();
Properties props = new Properties();
props.setProperty("user", "cassandra");
props.setProperty("password", "cassandra");

// ":datastax" in the URL is optional, it suggests to use DataStax Java driver as the provider to connect to Cassandra
Connection conn = DriverManager.connect("jdbc:c*:datastax://host1,host2/system_auth?consistencyLevel=one", props);
// change current keyspace from system_auth to system
conn.setSchema("system");

// query peers table in current keyspace, by default the SQL below will be translated into the following CQL:
// SELECT * FROM peers LIMIT 10000
// Please be aware that the original SQL does not work in Cassandra as table alias is not supported
ResultSet rs = conn.createStatement().executeQuery("select p.* from peers p");
while (rs.next()) {
...
}
...
```

#### Connection Properties
![Connection Properties](../../raw/master/resources/images/connection_properties.png)

#### Magic Comments
To set read timeout to 120 seconds just for a specific query, you can do it by adding a single line comment:
```sql
-- set read_timeout=120
select * from xyz
```
Please notice that magic comments have to be started with "-- set ", and you can use semicolon as separator
in one line for multiple instructions:
```sql
-- set read_timeout = 120; replace_null_value = true
-- set no_limit = true
select * from xyz
```

#### SQuirrel SQL
1. Configure Apache Cassandra driver
    ![Configure Driver](../../raw/master/resources/images/configure_driver.png)
2. Create a new alias using above driver
    ![Configure Alias](../../raw/master/resources/images/configure_alias.png)
3. Congratulations! You now can to connect to Cassandra
    ![Query Trace](../../raw/master/resources/images/query_trace.png)

### JMeter

### Pentaho Data Integration(aka. Kettle)
1. Put the driver in $KETTLE_HOME/lib directory
2. Create new connection to Cassandra

### Pentaho BI Server
1. Put the driver in $BISERVER_HOME/tomcat/lib directory
2. Create new datasource pointing to Cassandra

## How to build?
To build the project on your own, please make sure you have Maven 3 and then follow instructions below:
```bash
$ git clone https://github.com/zhicwu/cassandra-jdbc-driver
$ cd cassandra-jdbc-driver
$ mvn -DskipTests clean package
$ ls -alF target/*cassandra-jdbc-driver*.jar
```
