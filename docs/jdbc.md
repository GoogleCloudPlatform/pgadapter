# Google Cloud Spanner PGAdapter - JDBC Support

PGAdapter has __limited support__ for the [PostgreSQL JDBC driver](https://github.com/pgjdbc/pgjdbc)
version 42.0.0 and higher.

## Usage

First start PGAdapter in JDBC mode by adding `-jdbc` to the command line parameters:

```shell
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter-latest.jar
java -jar pgadapter-latest.jar -p my-project -i my-instance -d my-database -jdbc
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

## Running PGAdapter

This example uses the pre-built jar with dependencies to run PGAdapter as a standalone process.
See [README](../README.md) for more possibilities on how to run PGAdapter.
