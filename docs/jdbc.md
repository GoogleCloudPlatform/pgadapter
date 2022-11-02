# PGAdapter - JDBC Connection Options

PGAdapter supports the [PostgreSQL JDBC driver](https://github.com/pgjdbc/pgjdbc) version 42.0.0 and higher.

## Usage

First start PGAdapter:

```shell
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz && tar -xzvf pgadapter.tar.gz
java -jar pgadapter.jar -p my-project -i my-instance
```

Connect to PGAdapter using TCP like this:

```java
// Make sure the PG JDBC driver is loaded.
Class.forName("org.postgresql.Driver");

// Replace localhost and 5432 with the host and port number where PGAdapter is running.
try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/my-database")) {
  try (ResultSet resultSet = connection.createStatement().executeQuery("select 'Hello world!' as hello")) {
    while (resultSet.next()) {
      System.out.printf("Greeting from Cloud Spanner PostgreSQL: %s\n", resultSet.getString(1));
    }
  }
}
```


### Unix Domain Sockets
Connect to PGAdapter using Unix Domain Sockets like this:

Add this to your dependencies:
```xml
<dependency>
  <groupId>com.kohlschutter.junixsocket</groupId>
  <artifactId>junixsocket-core</artifactId>
  <version>2.5.0</version>
  <type>pom</type>
</dependency>
<dependency>
  <groupId>com.kohlschutter.junixsocket</groupId>
  <artifactId>junixsocket-common</artifactId>
  <version>2.5.0</version>
</dependency>
```

```java
// Make sure the PG JDBC driver is loaded.
Class.forName("org.postgresql.Driver");

// '/tmp' is the default domain socket directory for PGAdapter. This can be changed using the -dir
// command line argument. 5432 is the default port number used by PGAdapter. Change this in the
// connection string if PGAdapter is running on a custom port.
try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/my-database"
    + "?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg" 
    + "&socketFactoryArg=/tmp/.s.PGSQL.5432")) {
  try (ResultSet resultSet = connection.createStatement().executeQuery("select 'Hello world!' as hello")) {
    while (resultSet.next()) {
      System.out.printf("Greeting from Cloud Spanner PostgreSQL: %s\n", resultSet.getString(1));
    }
  }
}
```

## Running PGAdapter

This example uses the pre-built jar to run PGAdapter as a standalone process.
See [README](../README.md) for more possibilities on how to run PGAdapter.

## Performance Considerations

The following will give you the best possible performance when using the JDBC driver with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host. See https://jdbc.postgresql.org/documentation/head/connect.html
for more information on connection options for the JDBC driver.

### JDBC Batching
Use the JDBC batching API when executing multiple DDL or DML statements. PGAdapter will combine
DML and DDL statements that are executed in a JDBC batch into a single request on Cloud Spanner.
This can significantly reduce the overhead of executing multiple DML or DDL statements.

Example for DML statements:

```java
try (java.sql.Statement statement = connection.createStatement()) {
  statement.addBatch("insert into my_table (key, value) values ('k1', 'value1')");
  statement.addBatch("insert into my_table (key, value) values ('k2', 'value2')");
  // This will execute the above statements as a single batch DML request on Cloud Spanner.
  int[] updateCounts = statement.executeBatch();
}
```

Example for DDL statements:

```java
try (java.sql.Statement statement = connection.createStatement()) {
  statement.addBatch("create table singers (singerid varchar primary key, name varchar)");
  statement.addBatch("create index idx_singers_name on singers (name)");
  // This will execute the above statements as a single DDL update request on Cloud Spanner.
  int[] updateCounts = statement.executeBatch();
}
```

## Limitations
- Server side [prepared statements](https://www.postgresql.org/docs/current/sql-prepare.html) are limited to at most 50 parameters.
  Note: This is not the same as `java.sql.PreparedStatement`. A `java.sql.PreparedStatement` will only use
  a server side prepared statement if it has been executed more at least `prepareThreshold` times.
  See https://jdbc.postgresql.org/documentation/server-prepare/#activation for more information.
