# Google Cloud Spanner PostgreSQL Adapter - JDBC Support

PGAdapter has partly support for the [PostgreSQL JDBC driver](https://github.com/pgjdbc/pgjdbc)
version 42.0.0 and higher.

## Usage

First start PGAdapter in JDBC mode (see [README](../README.md) for more possibilities on how to run
PGAdapter):

```shell
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter-v0.2.1.jar
java -jar pgadapter-v0.2.1.jar -p my-project -i my-instance -d my-database -jdbc
```

Connect to PGAdapter like this:

```java
// Make sure the PG JDBC driver is loaded.
Class.forName("org.postgresql.Driver");

// Replace localhost and 5432 with the host and port number where PGAdapter is running.
// prepareThreshold=0 disables the use of server-side prepared statements in JDBC.
try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/?prepareThreshold=0")) {
  try (ResultSet resultSet = connection.createStatement().executeQuery("select 'Hello world!' as hello")) {
    while (resultSet.next()) {
      System.out.printf("Greeting from Cloud Spanner PostgreSQL: %s\n", resultSet.getString(1));
    }
  }
}
```
