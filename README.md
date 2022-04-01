# Google Cloud Spanner PostgreSQL Adapter

Google Cloud Spanner PGAdapter is a proxy which translates the PostgreSQL wire-protocol into the
Cloud Spanner equivalent for Cloud Spanner databases [that use the PostgreSQL dialect](https://cloud.google.com/spanner/docs/postgresql-interface).

PGAdapter has been tested and can be used with the following clients:
1. `psql`: Versions 11, 12, 13 and 14 are supported. See [psql support](docs/psql.md) for more details.
2. `JDBC`: Versions 42.x and higher have __limited support__. See [JDBC support](docs/jdbc.md) for more details.
3. `pgx`: Version 4.15 and higher have __limited support__. See [pgx support](docs/pgx.md) for more details.

Other drivers and tools have __not been specifically tested__ and are not known to work with PGAdapter
and are therefore __not supported__. Drivers and tools that use [simple query protocol](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4)
are likely to work with PGAdapter. Drivers and tools that use the [extended query protocol](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
are more likely to at least have some issues when used with PGAdapter.

## Usage
The PostgreSQL adapter can be started both as a Docker container, a standalone process as well as an
in-process server (the latter is only supported for Java applications).

### Docker

See [running PGAdapter using Docker](docs/docker.md) for more examples for running PGAdapter in Docker.

Replace the project, instance and database names and the credentials file in the example below to
run PGAdapter from a pre-built Docker image.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter-docker-images/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x
```

The option `-v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro` mounts the
default credentials file on your local system to a virtual file with the same name in the Docker container.
The `-e GOOGLE_APPLICATION_CREDENTIALS` sets the file as the default credentials to use inside the container.

The `-x` option tells PGAdapter to accept connections other hosts than `localhost`. This is required
when running PGAdapter in a Docker container, as requests will not be seen as coming from `localhost`
in the container. Choose a different host port than 5432 if you already have PostgreSQL running on
your local system.

See [Options](#Options) for an explanation of all further options.

### Standalone with pre-built jar

A pre-built jar containing all dependencies can be downloaded from https://storage.googleapis.com/pgadapter-jar-releases/pgadapter-&lt;VERSION&gt;.jar

Example (replace `v0.2.1` with the version you want to download):

```shell
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter-v0.2.1.jar
java -jar pgadapter-v0.2.1.jar -p my-project -i my-instance -d my-database
```

Use the `-s` option to specify a different local port than the default 5432 if you already have
PostgreSQL running on your local system.

See [Options](#Options) for an explanation of all further options.

### Standalone with locally built jar
1. Build a jar file containing all dependencies by running `mvn package -P shade`.
2. Execute `java -jar <jar-file> <options>`.
3. To get fine-grained logging messages, make sure that you have the logging.properties file and run the jar with `-Djava.util.logging.config.file=logging.properties`. You need to create one according to this sample if it's missing.
    ```
    handlers=java.util.logging.ConsoleHandler,java.util.logging.FileHandler
    com.google.cloud.spanner.pgadapter.level=FINEST
    java.util.logging.ConsoleHandler.level=FINEST
    java.util.logging.FileHandler.level=INFO
    java.util.logging.FileHandler.pattern=%h/spanner-pg-adapter-%u.log
    java.util.logging.FileHandler.append=false
    io.grpc.internal.level = WARNING
    
    java.util.logging.SimpleFormatter.format=[%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS] [%4$s] (%2$s): %5$s%6$s%n
    java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter
    ```

See [Options](#Options) for an explanation of all further options.

### In-process
This option is only available for Java/JVM-based applications.

1. Add `google-cloud-spanner-pgadapter` as a dependency to your project.
2. Build a server using the `com.google.cloud.spanner.pgadapter.ProxyServer`
   class:

```java
class PGProxyRunner {
    public static void main() {
        ProxyServer server = new ProxyServer(
          new OptionsMetadata(
                "jdbc:cloudspanner:/projects/my-project"
                + "/instances/my-instance"
                + "/databases/my-database"
                + ";credentials=/home/user/service-account-credentials.json",
                portNumber,
                textFormat,
                forceBinary,
                authenticate,
                requiresMatcher,
                commandMetadataJSON)
        );
        server.startServer();
    }
}
```

Wherein the first item is the JDBC connection string containing pertinent
information regarding project id, instance id, database name, credentials file
path; All other items map directly to previously mentioned CLI options.

### Options
The following options are required to run the proxy:

```    
-p <projectname>
  * The project name where the desired Spanner database is running.
    
-i <instanceid>
  * The instance ID where the desired Spanner database is running.

-d <databasename>
  * The Spanner database name.

-c <credentialspath>
  * This is only required if you have not already set up default credentials on the system where you
    are running PGAdapter.
  * The full path for the file containing the service account credentials in JSON format.
  * Do remember to grant the service account sufficient credentials to access the database.
```

The following options are optional:
```    
-s <port>
  * The inbound port for the proxy. Defaults to 5432. Choose a different port if you already have
    PostgreSQL running on your local system.
   
-a
  * Use authentication when connecting. Currently authentication is not strictly
    implemented in the proxy layer, as it is expected to be run locally, and
    will ignore any connection not stemming from localhost. However, it is a
    useful compatibility option if the PostgreSQL client is set to always 
    authenticate. Note that SSL is not included for the same reason that
    authentication logic is not: since all connections are local, sniffing
    traffic should not generally be a concern.

-q
  * PSQL Mode. Use this option when fronting PSQL. This option will incur some
    performance penalties due to query matching and translation and as such is
    not recommended for production. It is further not guaranteed to perfectly
    match PSQL logic. Please only use this mode when using PSQL.
    
-jdbc
  * JDBC Mode. Use this option when fronting the native PostgreSQL JDBC driver.
    This option will inspect queries to look for native JDBC metadata queries and
    replace these with queries that are suppported by Cloud Spanner PostgreSQL
    databases.

-f <POSTGRESQL|SPANNER>
  * The data result format coming back to the client from the proxy. By default,
    this is POSTGRESQL, but you can choose SPANNER format if you do not wish the
    data to be modified and the client used can handle it. This is generally only
    useful when using `psql`.
    
-c <multi-statementcommand>
    Runs a single command before exiting. This command can be comprised of multiple 
    statments separated by ';'. Each SQL command string passed to -c is sent to the 
    server as a single request. Because of this, the server executes it as a single 
    transaction even if the string contains multiple SQL commands. There are some
    limitations in Cloud Spanner which require this option to operate in a different
    manner from PSQL. 'SELECT' statements are not allowed in batched commands. Mixing
    DML and DDL is not allowed in a single multi-statement command. Also, 
    'BEGIN TRANSACTION' and 'COMMIT' statements are not allowed in batched commands.

-b
  * Force the server to send data back in binary PostgreSQL format when no
    specific format has been requested. The PostgreSQL wire protocol specifies 
    that the server should send data in text format in those cases. This 
    setting overrides this default and should be used with caution, for example
    for testing purposes, as clients might not accept this behavior. This 
    setting only affects query results in extended query mode. Queries in 
    simple query mode will always return results in text format. If you do not 
    know what extended query mode and simple query mode is, then you should 
    probably not be using this setting.

-j <commandmetadatapath>
  * The full path for a file containing a JSON object to do SQL translation
    based on RegEx replacement. Any item matching the input_pattern will be
    replaced by the output_pattern string, wherein capture groups are allowed and
    their order is specified via the matcher_array item. Match replacement must be
    defined via %s in the output_pattern string. Set matcher_array to [] if no
    matches exist. Alternatively, you may place the matching group names 
    directly within the output_pattern string using matcher.replaceAll() rules
    (that is to say, placing the item within braces, preceeeded by a dollar sign);
    For this specific case, matcher_array must be left empty. User-specified 
    patterns will precede internal matches. Escaped and general regex syntax 
    matches Java RegEx syntax; more information on the Java RegEx syntax found 
    here: 
    https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
    * Example:
        { 
          "commands": 
            [ 
              {
                "input_pattern": "^SELECT * FROM users;$", 
                "output_pattern": "SELECT 1;",
                "matcher_array": []
              }, 
              {
                "input_pattern": "^ab(?<firstgroup>\\d)(?<secondgroup>\\d)$",
                "output_pattern": "second number: %s, first number: %s",
                "matcher_array": ["secondgroup", "firstgroup"] 
              },
              {
                "input_pattern": "^cd(?<firstgroup>\\d)(?<secondgroup>\\d)$",
                "output_pattern": "second number: ${secondgroup}, first number: ${firstgroup}",
                "matcher_array": [] 
              }
            ]
        }
        
        
        Input queries:
        "SELECT * FROM users;"
        "ab12"
        "cd34"

        Output queries:
        "SELECT 1;"
        "second number: 2, first number: 1"
        "second number: 4, first number: 3"
```
An example of a simple run string:

``` 
java -jar <jar-file> -p <project name> -i <instance id> -d <database name> -c
<path to credentials file> -s 5432 
```

## Details
Google Cloud Spanner PGAdapter is a simple, MITM, forward, non-transparent proxy, which translates
the PostgreSQL wire-protocol into the Cloud Spanner equivalent. It can only be used with Cloud
Spanner databases that use the [PostgreSQL dialect](https://cloud.google.com/spanner/docs/postgresql-interface).
By running this proxy locally, any PostgreSQL client (including the SQL command-line client PSQL)
should function seamlessly by simply pointing its outbound port to this proxy's inbound port.
The proxy does not support all parts of the PostgreSQL wire-protocol. See [Limitations](#Limitations)
for a list of current limitations.

In addition to translation, this proxy also concerns itself with authentication
and to some extent, connection pooling. Translation for the most part is simply
a transformation of the [PostgreSQL wire protocol](https://www.postgresql.org/docs/current/protocol-message-formats.html)
except for some cases concerning PSQL, wherein the query itself is translated.

Simple query mode and extended query mode are supported, and any data type
supported by Spanner is also supported. Items, tables and language not native to
Spanner are not supported, unless otherwise specified.

Though the majority of functionality inherent in most PostgreSQL clients
(including PSQL and JDBC) are included out of the box, the following items are
not supported:
* Prepared Statement DESCRIBE
* SSL
* Functions
* COPY <table_name> TO ...
* COPY <table_name> FROM <filename | PROGRAM program>

COPY <table_name> FROM STDIN __is supported__.

Only the following `psql` meta-commands are supported:
  * `\d <table>` 
  * `\dt <table>`
  * `\dn <table>`
  * `\di <table>`
  * `\l`

Other `psql` meta-commands are __not__ supported.

## COPY support
`COPY <table-name> FROM STDIN` is supported. This option can be used to insert bulk data to a Cloud
Spanner database. `COPY` operations are atomic by default, but the standard transaction limits of
Cloud Spanner apply to these transactions. That means that at most 20,000 mutations can be included
in one `COPY` operation. `COPY` can also be executed in non-atomic mode by executing the statement
`SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing the copy operation.

Although only `STDIN` is supported, export files can still be imported using `COPY` by piping files
into `psql`. See the examples below.

### Atomic COPY example
```sql
create table numbers (number bigint not null primary key, name varchar);
```

```shell
cat numbers.txt | psql -h localhost -d test-db -c "copy numbers from stdin;"
```

The above operation will fail if the `numbers.txt` file contains more than 20,000 mutations.

### Non-atomic COPY example
```sql
create table numbers (number bigint not null primary key, name varchar);
```

```shell
cat numbers.txt | psql -h localhost -d test-db -c "set autocommit_dml_mode='partitioned_non_atomic'" -c "copy numbers from stdin;"
```

The above operation will automatically split the data over multiple transactions if the file
contains more than 20,000 mutations.

Note that this also means that if an error is encountered
during the `COPY` operation, some rows may already have been persisted to the database. This will
not be rolled back after the error was encountered. The transactions are executed in parallel,
which means that data after the row that caused the error in the import file can still have been
imported to the database before the `COPY` operation was halted.

### Streaming data from PostgreSQL
`COPY` can also be used to stream data directly from a real PostgreSQL database to Cloud Spanner.
This makes it easy to quickly copy an entire table from PostgreSQL to Cloud Spanner, or to generate
test data for Cloud Spanner using PostgreSQL queries.

The following examples assume that a real PostgreSQL server is running on `localhost:5432` and
PGAdapter is running on `localhost:5433`.

```shell
psql -h localhost -p 5432 -d my-local-db \
  -c "copy (select i, to_char(i, 'fm000') from generate_series(1, 10) s(i)) to stdout" \
  | psql -h localhost -p 5433 -d my-spanner-db \
  -c "copy numbers from stdin;"
```

Larger datasets require that the Cloud Spanner database is set to `PARTITIONED_NON_ATOMIC` mode:

```shell
psql -h localhost -p 5432 -d my-local-db \
  -c "copy (select i, to_char(i, 'fm000') from generate_series(1, 1000000) s(i)) to stdout" \
  | psql -h localhost -p 5433 -d my-spanner-db \
  -c "set autocommit_dml_mode='partitioned_non_atomic'" \
  -c "copy numbers from stdin;"
```

## Limitations

PGAdapter has the following known limitations at this moment:
- PostgreSQL prepared statements are not fully supported. It is recommended not to use this feature.
  Note: This limitation relates specifically to server side PostgreSQL prepared statements.
  The JDBC `java.sql.PreparedStatement` interface is supported.
- DESCRIBE statement wire-protocol messages currently return `Oid.UNSPECIFIED` for all parameters in the query.
- DESCRIBE statement wire-protocol messages return `NoData` as the row description of the statement.
- The COPY protocol only supports COPY FROM STDIN.

## Support Level

We are not currently accepting external code contributions to this project. 
Please feel free to file feature requests using GitHub's issue tracker or 
using the existing Cloud Spanner support channels.
