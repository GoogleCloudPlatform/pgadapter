# R2DBC sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using the standard
`R2DBC` PostgreSQL driver. PGAdapter is automatically started in-process together with the sample
application.

### Dependencies

The sample application adds the following dependencies:

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```xml
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>r2dbc-postgresql</artifactId>
  <version>1.0.5.RELEASE</version>
</dependency>
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-spanner-pgadapter</artifactId>
  <version>0.39.0</version>
</dependency>
```
<!--- {x-version-update-end} -->

### Connecting
PGAdapter is started in-process with the sample application:

```java
OptionsMetadata.Builder builder =
    OptionsMetadata.newBuilder()
        .setProject(project)
        .setInstance(instance)
        // Start PGAdapter on any available port.
        .setPort(0);
ProxyServer server = new ProxyServer(builder.build());
server.startServer();
server.awaitRunning();
```

The PostgreSQL R2DBC driver can connect to the in-process PGAdapter instance:

```java
ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
    .option(DRIVER, "postgresql")
    .option(HOST, "localhost")
    .option(PORT, port) 
    .option(USER, "")
    .option(PASSWORD, "")
    .option(DATABASE, database)
    .option(OPTIONS, options)
    .build());

CountDownLatch finished = new CountDownLatch(1);
Flux.usingWhen(
        connectionFactory.create(),
        connection ->
            Flux.from(connection.createStatement(
                        "select 'Hello World!' as greeting")
                    .execute())
                .flatMap(result ->
                    result.map(row -> row.get("greeting", String.class))),
        Connection::close)
    .doOnNext(greeting -> System.out.printf("\nGreeting: %s\n\n", greeting))
    .doOnError(Throwable::printStackTrace)
    .doFinally(ignore -> finished.countDown())
    .subscribe();

finished.await();
```

### Run Sample

Run the sample on the Spanner Emulator with the following command:

```shell
mvn exec:java
```

Run the sample on a real Spanner instance with the following command:

```shell
mvn exec:java \
  -Dexec.args=" \
    -p my-project \
    -i my-instance \
    -d my-database"
```
