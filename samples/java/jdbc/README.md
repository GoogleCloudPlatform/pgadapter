# JDBC sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using the standard
`JDBC` PostgreSQL driver. PGAdapter is automatically started in-process together with the sample
application.

### Dependencies

The sample application adds the following dependencies:

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```xml
<!-- [START pgadapter_and_jdbc_dependency] -->
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>postgresql</artifactId>
  <version>42.6.0</version>
</dependency>
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-spanner-pgadapter</artifactId>
  <version>0.23.0</version>
</dependency>
<!-- [END pgadapter_and_jdbc_dependency] -->
```
<!--- {x-version-update-end} -->

### Connecting
PGAdapter is started in-process with the sample application:

```java
<!-- [START pgadapter_start_in_process] -->
OptionsMetadata.Builder builder =
    OptionsMetadata.newBuilder()
        .setProject(project)
        .setInstance(instance)
        // Start PGAdapter on any available port.
        .setPort(0);
ProxyServer server = new ProxyServer(builder.build());
server.startServer();
server.awaitRunning();
<!-- [END pgadapter_start_in_process] -->
```

The PostgreSQL JDBC driver can connect to the in-process PGAdapter instance. This example uses
Unix Domain Sockets for the lowest possible latency:

```java
<!-- [START pgadapter_connect_in_process] -->
// Create a connection URL that will use Unix domain sockets to connect to PGAdapter.
String connectionUrl =
  String.format(
      "jdbc:postgresql://localhost/%s?"
          + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
          + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
      database, server.getLocalPort());
try (Connection connection = DriverManager.getConnection(connectionUrl)) {
  try (ResultSet resultSet = 
      connection.createStatement().executeQuery("select 'Hello World!' as greeting")) {
    while (resultSet.next()) {
      System.out.printf("\nGreeting: %s\n\n", resultSet.getString("greeting"));
    }
  }
}
<!-- [END pgadapter_connect_in_process] -->
```

### Run Sample

Run the sample with the following command:

```shell
mvn exec:java \
  -Dexec.args=" \
    -p my-project \
    -i my-instance \
    -d my-database"
```
