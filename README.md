# JDBC Driver for Apache Cassandra

[![Build Status](https://travis-ci.org/zhicwu/cassandra-jdbc-driver.svg?branch=master)](https://travis-ci.org/zhicwu/cassandra-jdbc-driver)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.zhicwu/cassandra-jdbc-driver/badge.svg)](https://search.maven.org/remote_content?g=com.github.zhicwu&a=cassandra-jdbc-driver&v=LATEST&c=shaded)

Type 4 JDBC driver for Apache Cassandra. Building on top of [DataStax Java Driver](https://github.com/datastax/java-driver/)
and [JSqlParser](https://github.com/JSQLParser/JSqlParser), it intends to provide better SQL compatibility over CQL,
so that it works well with existing tools like [SQuirreL SQL](http://www.squirrelsql.org/) for SQL development,
[JMeter](http://jmeter.apache.org) for stress testing, and [Pentaho BI Suite](http://community.pentaho.com/)
for data processing and reporting.

You may find this helpful if you came from RDBMS world and hoping to get your hands on Apache Cassandra right away.
Having said that, it is **NOT** recommended to use this for production but development and research. You should use
DataStax Java Driver, Spark and maybe Presto if you want to do something serious.

OK, you have been warned :) Now go ahead to download the latest driver and give it a shot!

## Features

* Implicit type conversion for ease of use
```java
...
// set parameter
preparedStatment.setString(index, "13:30:54.234"); // or setTime(index, new Time(1465536654234L))
...
// get query result
resultSet.getTime(index); // or getString(index)
...
```

* Instruct CQL statement through CQL comments(aka. magic comments)
```cql
/* please be aware that only single line comment begins with "set" can be recognized */
-- set consistency_level = ALL; fetch_size = 10000;
// set no_limit = true; read_timeout = 600;
select * from logs
```

* Limit unconstrained queries according to configuration
```cql
-- the following SQL will be translated to "SELECT * FROM logs LIMIT 10000"
-- you may change the behavior via magic comments or config.yaml
select * from logs
```

* Improved SQL compatibility, for example: table alias, and more to come: group by, select into, insert select,
field expression, lucene filter(if you have [Stratio's Cassandra Lucene Index](https://github.com/Stratio/cassandra-lucene-index) installed)...
```cql
-- the following SQL will be translated into "SELECT * FROM logs LIMIT 10000"
select l.* from logs l
```

* Possibly support alternative Java driver, for example: [Netflix Astyanax](https://github.com/Netflix/astyanax)

* Possibly support alternative storage (e.g. [HBase](http://hbase.apache.org/) just for fun)

## Get Started
Before you start, please make sure you have JDK 7 or above - JDK 6 is not supported.

#### Get the driver
The last release of the driver is available on Maven Central. You can install it in your application using
the following Maven dependency:
```xml
<dependency>
	<groupId>com.github.zhicwu</groupId>
	<artifactId>cassandra-jdbc-driver</artifactId>
	<version>0.6.1</version>
	<!-- comment out the classifier if you don't need shaded jar -->
	<classifier>shaded</classifier>
</dependency>
```
If you can't use a dependency management tool, you can download the latest shaded jar from
[here](http://central.maven.org/maven2/com/github/zhicwu/cassandra-jdbc-driver/).

Optionally, if you want to build the driver on your own. You may follow the instructions below if you have both Git
and Maven installed:
```bash
$ git clone https://github.com/zhicwu/cassandra-jdbc-driver
$ cd cassandra-jdbc-driver
$ mvn clean package
$ ls -alF target/cassandra-jdbc-driver-*-shaded.jar
```

#### Say Hello to Cassandra
This is pretty much same as we did for any other database, except different driver and connection URL.
```java
...
// Driver driver = new com.github.cassandra.jdbc.CassandraDriver();
Properties props = new Properties();
props.setProperty("user", "cassandra");
props.setProperty("password", "cassandra");

// ":datastax" in the URL is optional, it suggests to use DataStax Java driver as the provider to connect to Cassandra
Connection conn = DriverManager.getConnection("jdbc:c*:datastax://host1,host2/system_auth?consistencyLevel=ONE", props);
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

## Configuration

#### Driver Configuration
Default settings of this driver can be found in [config.yaml](src/main/resources/config.yaml). Besides changing it
in the jar file, you may set system property "cassandra.jdbc.driver.config" to use your own config instead.
```bash
$ java -Dcassandra.jdbc.driver.config=/usr/local/private/new_config.yaml ...
```

#### Connection Properties
![Connection Properties](../../raw/master/resources/images/connection_properties.png)

#### Magic Comments
To set read timeout to 120 seconds just for a specific query, you can do it by adding a single line comment:
```sql
-- set read_timeout=120
select * from xyz
```
Please notice that magic comments have to be started with "-- set " or "// set ", and you can use semicolon as separator
in one line for multiple instructions:
```sql
-- set read_timeout = 120; replace_null_value = true
-- set no_limit = true
select * from xyz
```
All supported instructions in magic comments are declared at
[here](src/main/java/com/github/cassandra/jdbc/CassandraCqlStmtConfiguration.java).

## HOWTOs

#### SQuirrel SQL
1. Configure Apache Cassandra driver
    ![Configure Driver](../../raw/master/resources/images/configure_driver.png)
2. Create a new alias using above driver
    ![Configure Alias](../../raw/master/resources/images/configure_alias.png)
3. Congratulations! You now can to connect to Cassandra
    ![Query Trace](../../raw/master/resources/images/query_trace.png)
4. To use magic comments, please use "//" instead of "--" as SQuirrel SQL will remove the latter automatically
before sending the query to JDBC driver.

#### JMeter
1. Put the driver in $JMETER_HOME/lib directory
2. Use JDBC Sampler to access Cassandra

#### Pentaho Data Integration(aka. Kettle)
1. Put the driver in $KETTLE_HOME/lib directory
2. Create new connection to Cassandra
3. Use TableInput / TableOutput steps to query / update Cassandra data
4. You may want to add "-- set replace_null_value = true" to your query, as Kettle tries to use NULL value get meta data

#### Pentaho BI Server
1. Put the driver in $BISERVER_HOME/tomcat/lib directory
2. Create new datasource pointing to Cassandra
3. Use CDA to issue SQL to access Cassandra - Mondrian is not tested and is not supposed to work

## Build
```bash
$ mvn -Prelease notice:generate
$ mvn license:format
$ mvn clean package
```

## TODOs
- [x] ~~Remove CQL Parser to support JDK 7~~
- [ ] UDT support and smooth type conversion
- [ ] Multiple ResultSet support, especially when tracing turned on
- [ ] Better SQL compatibility(e.g. SELECT INTO, GROUP BY and probably simple table joins and sub-queries)
- [ ] (Basic)Mondrian support
- [ ] More providers(and storage?)...
