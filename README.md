# Google Cloud Spanner PGAdapter

PGAdapter is a proxy that translates the PostgreSQL wire-protocol into the
equivalent for Spanner databases [that use the PostgreSQL interface](https://cloud.google.com/spanner/docs/postgresql-interface).
It enables you to use standard PostgreSQL drivers and tools with Cloud Spanner and is designed for
the [lowest possible latency](benchmarks/latency-comparison/README.md).

__Note__: JVM-based applications can add PGAdapter as a compile-time dependency and run the proxy
in the same process as the main application. See [samples/java/jdbc](samples/java/jdbc) for a
small sample application that shows how to do this.

## Drivers and Clients
PGAdapter can be used with the following drivers and clients:
1. `psql`: Versions 11, 12, 13 and 14 are supported. See [psql support](docs/psql.md) for more details.
1. `IntelliJ`, `DataGrip` and other `JetBrains` IDEs. See [Connect Cloud Spanner PostgreSQL to JetBrains](docs/intellij.md) for more details.
1. `JDBC`: Versions 42.x and higher are supported. See [JDBC support](docs/jdbc.md) for more details.
1. `pgx`: Version 4.15 and higher are supported. See [pgx support](docs/pgx.md) for more details.
1. `psycopg2`: Version 2.9.3 and higher are supported. See [psycopg2](docs/psycopg2.md) for more details.
1. `psycopg3`: Version 3.1.x and higher are supported. See [psycopg3 support](docs/psycopg3.md) for more details.
1. `node-postgres`: Version 8.8.0 and higher are supported. See [node-postgres support](docs/node-postgres.md) for more details.
1. `npgsql`: Version 6.0.x and higher have experimental support. See [npgsql support](docs/npgsql.md) for more details.
1. `postgres_fdw`: The PostgreSQL foreign data wrapper has experimental support. See [Foreign Data Wrapper sample](samples/foreign-data-wrapper) for more details.

## ORMs, Frameworks and Tools
PGAdapter can be used with the following frameworks and tools:
1. `Hibernate`: Version 5.3.20. Final and higher are supported. See [hibernate support](samples/java/hibernate/README.md) for more details.
1. `Spring Data JPA`: Spring Data JPA in combination with Hibernate is also supported. See the [Spring Data JPA Sample Application](samples/java/spring-data-jpa) for a full example.
1. `Liquibase`: Version 4.12.0 and higher are supported. See [Liquibase support](docs/liquibase.md)
   for more details. See also [this directory](samples/java/liquibase) for a sample application using `Liquibase`.
1. `gorm`: Version 1.23.8 and higher are supported. See [gorm support](docs/gorm.md) for more details.
   See also [this directory](samples/golang/gorm) for a sample application using `gorm`.
1. `SQLAlchemy 2.x`: Version 2.0.1 and higher are supported.
   See also [this directory](samples/python/sqlalchemy2-sample) for a sample application using `SQLAlchemy 2.x`.
1. `SQLAlchemy 1.x`: Version 1.4.45 and higher has _experimental support_. It is recommended to use `SQLAlchemy 2.x`
   instead of `SQLAlchemy 1.4.x` for the [best possible performance](docs/sqlalchemy.md#limitations). 
   See also [this directory](samples/python/sqlalchemy-sample) for a sample application using `SQLAlchemy 1.x`.
1. `pgbench` can be used with PGAdapter, but with some limitations. See [pgbench.md](docs/pgbench.md)
   for more details.
1. `Ruby ActiveRecord`: Version 7.x has _experimental support_ and with limitations. Please read the
   instructions in [PGAdapter - Ruby ActiveRecord Connection Options](docs/ruby-activerecord.md)
   carefully for how to set up ActiveRecord to work with PGAdapter.

## FAQ
See [Frequently Asked Questions](docs/faq.md) for answers to frequently asked questions.

## Performance
See [Latency Comparisons](benchmarks/latency-comparison/README.md) for benchmark comparisons between
using PostgreSQL drivers with PGAdapter and using native Cloud Spanner drivers and client libraries.

## Usage
PGAdapter can be started both as a Docker container, a standalone process as well as an
in-process server (the latter is only supported for Java and other JVM-based applications).

### Docker

* See [running PGAdapter using Docker](docs/docker.md) for more examples for running PGAdapter in Docker.
* See [running PGAdapter as a sidecar proxy](docs/sidecar-proxy.md) for how to run PGAdapter as a
  sidecar proxy in a Kubernetes cluster.

Replace the project, instance and database names and the credentials file in the example below to
run PGAdapter from a pre-built Docker image.

```shell
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v /path/to/credentials.json:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -c /credentials.json -x
```

The `-x` argument turns off the requirement that all TCP connections must come from localhost.
This is required when running PGAdapter in a Docker container.

See [Options](#Options) for an explanation of all further options.

#### Distroless Docker Image

We also publish a [distroless Docker image](https://github.com/GoogleContainerTools/distroless) for
PGAdapter under the tag `gcr.io/cloud-spanner-pg-adapter/pgadapter-distroless`. This Docker image
runs PGAdapter as a non-root user.

```shell
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter-distroless
docker run \
  -d -p 5432:5432 \
  -v /path/to/credentials.json:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter-distroless \
  -p my-project -i my-instance -d my-database \
  -c /credentials.json -x
```

### Standalone with pre-built jar

A pre-built jar and all dependencies can be downloaded from https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz

```shell
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz \
  && tar -xzvf pgadapter.tar.gz
java -jar pgadapter.jar -p my-project -i my-instance -d my-database
```

Use the `-s` option to specify a different local port than the default 5432 if you already have
PostgreSQL running on your local system.

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
You can also download a specific version of the jar. Example (replace `v0.23.1` with the version you want to download):
```shell
VERSION=v0.23.1
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter-${VERSION}.tar.gz \
  && tar -xzvf pgadapter-${VERSION}.tar.gz
java -jar pgadapter.jar -p my-project -i my-instance -d my-database
```
<!--- {x-version-update-end} -->

See [Options](#Options) for an explanation of all further options.

### Standalone with locally built jar
1. Build a jar file and assemble all dependencies by running

```shell
mvn package -P assembly
```

2. Execute (the binaries are in the target/pgadapter folder)
```shell
cd target/pgadapter
java -jar pgadapter.jar -p my-project -i my-instance -d my-database
```

See [Options](#Options) for an explanation of all further options.

### In-process
This option is only available for Java/JVM-based applications.

1. Add `google-cloud-spanner-pgadapter` as a dependency to your project by adding this to your `pom.xml` file:

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```xml
<!-- [START pgadapter_dependency] -->
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-spanner-pgadapter</artifactId>
  <version>0.23.1</version>
</dependency>
<!-- [END pgadapter_dependency] -->
```
<!--- {x-version-update-end} -->


2. Build a server using the `com.google.cloud.spanner.pgadapter.ProxyServer` class:

```java
class PGProxyRunner {
    public static void main() {
      String[] arguments =
              new String[] {
                      "-p",
                      "my-project",
                      "-i",
                      "my-instance",
                      "-d",
                      "my-database",
                      "-c",
                      "/path/to/credentials.json"
              };
      ProxyServer server = new ProxyServer(new OptionsMetadata(arguments));
      server.startServer();
      server.awaitRunning();
    }
}
```

See [samples/java/jdbc](samples/java/jdbc) for a small sample application that adds
PGAdapter as a compile-time dependency and runs it together with the main application.

### Options

The following list contains the most frequently used startup options for PGAdapter.

```
-p <projectname>
  * The project name where the Spanner database(s) is/are running. If omitted, all connection
    requests must use a fully qualified database name in the format
    'projects/my-project/instances/my-instance/databases/my-database'.
    
-i <instanceid>
  * The instance ID where the Spanner database(s) is/are running. If omitted, all connection
    requests must use a fully qualified database name in the format
    'projects/my-project/instances/my-instance/databases/my-database'.

-d <databasename>
  * The default Spanner database name to connect to. This is only required if you want PGAdapter to
    ignore the database that is given in the connection request from the client and to always
    connect to this database.
  * If set, any database given in a connection request will be ignored. \c commands in psql will not
    change the underlying database that PGAdapter is connected to.
  * If not set, the database to connect to must be included in any connection request. \c commands
    in psql will change the underlying database that PGAdapter connects to.

-c <credentialspath>
  * This argument should not be used in combination with -a (authentication mode).
  * This is only required if you have not already set up default credentials on the system where you
    are running PGAdapter. See https://cloud.google.com/spanner/docs/getting-started/set-up#set_up_authentication_and_authorization
    for more information on setting up authentication for Cloud Spanner.
  * The full path for the file containing the service account credentials in JSON format.
  * Do remember to grant the service account sufficient credentials to access the database. See
    https://cloud.google.com/docs/authentication/production for more information.

-s <port>
  * The inbound port for the proxy. Defaults to 5432. Choose a different port if you already have
    PostgreSQL running on your local system.

-dir <socket-file-directory>
  * This proxy's domain socket directory. Defaults to '/tmp'.
    Note: Some distributions of PostgreSQL and psql use '/var/run/postgresql' as the
    default directory for Unix domain socket file names. When connecting to PGAdapter
    using psql from one of these distributions, you either need to use 'psql -h /tmp'
    or change the default Unix domain socket directory used by PGAdapter to '/var/run/postgresql'.

-ddl <ddl-transaction-mode>
  * Determines the behavior of the proxy when DDL statements are executed in transactions.
    See DDL options for more information.

-a
  * Turns on authentication for the proxy server. Clients are then requested
    to supply a username and password during a connection request.
  * The username and password must contain one of the following:
    1. The password field must contain the JSON payload of a credentials file, for example from a service account key file. The username will be ignored in this case.
    2. The password field must contain the private key from a service account key file. The username must contain the email address of the corresponding service account.
  * Note that SSL is not supported for the connection between the client and
    PGAdapter. The proxy should therefore only be used within a private network.
    The connection between PGAdapter and Cloud Spanner is always secured with SSL.

-x
  * PGAdapter by default only allows TCP connections from localhost or Unix Domain Socket connections.
    Use the -x switch to turn off the localhost check. This is required when running PGAdapter in a
    Docker container, as the connections from the host machine will not be seen as a connection from
    localhost in the container.
```

* See [command line arguments](docs/command_line_arguments.md) for a list of all supported arguments.
* See [connection options](docs/connection_options.md) for all connection options.
* See [Authentication Options](docs/authentication.md) for more details on the `-a` command line
  argument and other authentication options.
* See [DDL Options](docs/ddl.md) for more details for the `-ddl` command line argument.

#### Example - Connect to a Single Database

This example starts PGAdapter and instructs it to always connect to the same database using a fixed
set of credentials:

```shell
java -jar pgadapter.jar \
     -p <project-id> -i <instance-id> -d <database-id> \
     -c <path to credentials file> -s 5432
psql -h localhost
```

The psql `-d` command line argument will be ignored. The psql `\c` meta-command will have no effect.

#### Example - Require Authentication and Fully Qualified Database Name

This example starts PGAdapter and requires the client to supply both credentials and a fully
qualified database name. This allows a single instance of PGAdapter to serve connections to any
Cloud Spanner database.

```shell
java -jar pgadapter.jar -a

# The credentials file must be a valid Google Cloud credentials file, such as a
# service account key file or a user credentials file.
# Note that you must enclose the database name in quotes as it contains slashes.
PGPASSWORD=$(cat /path/to/credentials.json) psql -h /tmp \
  -d "projects/my-project/instances/my-instance/databases/my-database"
```

The psql `\c` meta-command can be used to switch to a different database.

## Details
Google Cloud Spanner PGAdapter is a simple, MITM, forward, non-transparent proxy, which translates
the PostgreSQL wire-protocol into the Cloud Spanner equivalent. It can only be used with Cloud
Spanner databases that use the [PostgreSQL interface](https://cloud.google.com/spanner/docs/postgresql-interface).
By running this proxy locally, any PostgreSQL client (including the SQL command-line client PSQL)
should function seamlessly by simply pointing its outbound port to this proxy's inbound port.
The proxy does not support all parts of the PostgreSQL wire-protocol. See [Limitations](#Limitations)
for a list of current limitations.

In addition to translation, this proxy also concerns itself with authentication
and to some extent, connection pooling. Translation for the most part is simply
a transformation of the [PostgreSQL wire protocol](https://www.postgresql.org/docs/current/protocol-message-formats.html)
except for some cases concerning PSQL, wherein the query itself is translated.

Simple query mode and extended query mode are supported, and any data type
supported by Spanner is also supported. Cloud Spanner databases created with
PostgreSQL dialect do not support all `pg_catalog` tables.

Though the majority of functionality inherent in most PostgreSQL clients are included
out of the box, the following items are not supported:
* SSL
* COPY <table_name> FROM <filename | PROGRAM program>

See [COPY <table-name> FROM STDIN](docs/copy.md) for more information on the COPY operations that
are supported.

Only the following `psql` meta-commands are supported:
  * `\d <table>` 
  * `\dt <table>`
  * `\dn <table>`
  * `\di <table>`
  * `\l`

Other `psql` meta-commands are __not__ supported.

## Limitations

PGAdapter has the following known limitations at this moment:
- Only [password authentication](https://www.postgresql.org/docs/current/auth-password.html) using
  the `password` method is supported. All other authentication methods are not supported.
- The COPY protocol only supports COPY TO|FROM STDOUT|STDIN [BINARY]. COPY TO|FROM <FILE|PROGRAM> is not supported.
  See [COPY](docs/copy.md) for more information.
- DDL transactions are not supported. PGAdapter allows DDL statements in implicit transactions,
  and executes SQL strings that contain multiple DDL statements as a single DDL batch on Cloud
  Spanner. See [DDL transaction options](docs/ddl.md) for more information.

## Logging

PGAdapter uses `java.util.logging` for logging. Create a `logging.properties` file to configure
logging messages. See the following example for an example to get fine-grained logging.

```
handlers=java.util.logging.ConsoleHandler,java.util.logging.FileHandler
com.google.cloud.spanner.pgadapter.level=FINEST
java.util.logging.ConsoleHandler.level=FINEST
java.util.logging.FileHandler.level=INFO
java.util.logging.FileHandler.pattern=%h/log/pgadapter-%u.log
java.util.logging.FileHandler.append=false
io.grpc.internal.level = WARNING

java.util.logging.SimpleFormatter.format=[%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS] [%4$s] (%2$s): %5$s%6$s%n
java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter
```

Start PGAdapter with `-Djava.util.logging.config.file=logging.properties` when running PGAdapter as
a jar.

### Logging in Docker

You can configure PGAdapter to log to a file on your local system when running it in Docker by
following these steps.

1. Create a `logging.properties` on your local system like this:

```
handlers=java.util.logging.FileHandler
java.util.logging.FileHandler.level=INFO
java.util.logging.FileHandler.pattern=/home/pgadapter/log/pgadapter.log
java.util.logging.FileHandler.append=true
io.grpc.internal.level = WARNING

java.util.logging.SimpleFormatter.format=[%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS] [%4$s] (%2$s): %5$s%6$s%n
java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter
```

2. Start the PGAdapter Docker container with these options:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export LOGGING_PROPERTIES=/full/path/to/logging.properties
docker run \
    -d -p 5432:5432 \
    -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
    -v ${LOGGING_PROPERTIES}:${LOGGING_PROPERTIES}:ro \
    -v /home/my-user-name/log:/home/pgadapter/log \
    -e GOOGLE_APPLICATION_CREDENTIALS \
    gcr.io/cloud-spanner-pg-adapter/pgadapter \
    -Djava.util.logging.config.file=${LOGGING_PROPERTIES} \
    -p my-project -i my-instance -d my-database \
    -x
```

The directory `/home/my-user-name/log` will be created automatically and the log file will be
placed in this directory.

## Support Level

We are not currently accepting external code contributions to this project. 
Please feel free to file feature requests using GitHub's issue tracker or 
using the existing Cloud Spanner support channels.
