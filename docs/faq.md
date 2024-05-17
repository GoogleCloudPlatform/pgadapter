# Google Cloud Spanner PGAdapter - Frequently Asked Questions

## Performance

### What latency does PGAdapter add?
PGAdapter adds very little latency if it is set up correctly. Make sure to follow these guidelines:
1. Run PGAdapter on the same host as your main application, for example as a side-car proxy.
2. When running PGAdapter in a Docker container separate from your main application, make sure that they are on the same Docker network.
3. If your application is written in Java: Run PGAdapter in-process with your application. See [this sample](../samples/java/jdbc/README.md) for an example.

You can run [these latency comparison benchmarks](../benchmarks/latency-comparison/README.md) to measure
the difference between using PostgreSQL drivers with PGAdapter and using native Cloud Spanner drivers
and client libraries.

### How can I test the latency that is added by PGAdapter?
You can run [these latency comparison benchmarks](../benchmarks/latency-comparison/README.md) to measure
the difference between using PostgreSQL drivers with PGAdapter and using native Cloud Spanner drivers
and client libraries.

### How can I get insights into execution times in PGAdapter?
PGAdapter supports [using OpenTelemetry](open_telemetry.md) to collect and export traces to Google Cloud Trace.

## Connection Options

### How can I specify the credentials that should be used for a connection?
PGAdapter supports two ways of setting the credentials that should be used for a connection:
1. Use the same credentials for all connections. The credentials must be set when PGAdapter is started,
   either using the `-c` command line argument or by letting PGAdapter use the default Google Cloud
   credentials of the runtime environment.

Example:
```shell
java -jar pgadapter.jar -p my-project -i my-instance -c /path/to/my-credentials.json
psql -h /tmp -d my-database
```

2. Let each connection supply the credentials as a password message to PGAdapter. This mode is enabled
   by starting PGAdapter with the `-a` command line argument.

Example:
```shell
java -jar pgadapter.jar -a -p my-project -i my-instance
PGPASSWORD=$(cat /path/to/my-credentials.json) psql -h /tmp -d my-database
```

See [authentication](authentication.md) for more information.

### How can I turn on SSL support?
The command line argument `-ssl` can be used to configure SSL support in PGAdapter. The following
three values are supported:
1. `Disabled`: This is the default. SSL connections will be rejected and the client will be asked to
   connect using a plain-text connection.
2. `Enabled`: SSL connections are accepted. Plain-text connections are also accepted.
3. `Required`: Only SSL connections are accepted. Plain-text connections will be rejected.

SSL modes `Enabled` and `Required` require that a private key and a public certificate is added to
the Java keystore. See [SSL Connections](ssl.md) for more information.

### How can I configure the number of sessions and/or number of gRPC channels that PGAdapter should use?
You can use the `-r` command line argument when starting PGAdapter to specify additional connection
properties that PGAdapter should use when it is connecting to Cloud Spanner. All connection properties
that are [supported by the JDBC driver for Cloud Spanner](https://github.com/googleapis/java-spanner-jdbc#connection-url-properties)
are also supported by PGAdapter.

Example: Use the following arguments to instruct PGAdapter to use a session pool with
`MinSessions=200`, `MaxSessions=1600` and `NumChannels=16` (number of gRPC channels).

```shell
java -jar pgadapter.jar -p my-project -i my-instance -d my-database \
     -r="minSessions=200;maxSessions=1600;numChannels=16"
```

## Docker

### Why am I getting the error 'server closed the connection unexpectedly'?
Why am I getting the error `error: connection to server at "localhost" (::1), port 5432 failed: server closed the connection unexpectedly`?

You need to start PGAdapter with the command line argument `-x` to allow connections from other hosts
than localhost. Example:

```shell
docker run -p 5432:5432 \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -x
```

PGAdapter will by default only allow connections from the same host as where PGAdapter is running.
This is to prevent accidentally giving access to unauthorized users to a Cloud Spanner database when
you start PGAdapter on a host without a firewall, or without a firewall rule that blocks incoming
traffic on port 5432. A connection from the host machine to a Docker container is not seen as a
connection coming from localhost.

### Why am I getting the error 'There is insufficient memory for the Java Runtime Environment to continue.'?
This error is caused by a change in the base image that is used for the Docker image for PGAdapter
that is incompatible with Docker versions prior to version [20.10.10](https://github.com/moby/moby/releases/tag/v20.10.10).
The added support for the clone3 syscall that is included in version 20.10.10 is required.

Upgrading your Docker engine to at least version 20.10.10 should fix this problem.

See also https://github.com/adoptium/containers/issues/215#issuecomment-1142046045 for more details.

## DDL
See [DDL options with PGAdapter](./ddl.md) for background information on the DDL options when starting
PGAdapter.

### Why does my DDL statement return the error message 'DDL statements are only allowed outside explicit transactions.'?
Cloud Spanner does not support transactional DDL. The error message 'DDL statements are only allowed outside explicit transactions.'
indicates that you have tried to execute a DDL statement while in an explicit transaction. An explicit
transaction is a transaction that was started by `BEGIN` command that was sent from the client.

You can solve this problem by either:
1. Remove the `BEGIN` command from your script, or set autocommit=true if you are using a driver that automatically starts an explicit transaction when a statement is executed.
2. Start PGAdapter with the command line argument `-ddl=AutocommitExplicitTransaction`. This will cause PGAdapter to automatically commit any active transaction when a DDL statement is encountered while in a transaction. Note that this does not make DDL statements transactional. It merely makes sure that the transaction is committed before the DDL statement is executed.

### Why does my DDL statement return the error message 'DDL statements are not allowed in mixed batches or transactions.'?
The error message 'DDL statements are not allowed in mixed batches or transactions.' indicates that
you have tried to execute one or more DDL statements in an implicit transaction or batch that also
contains queries and/or update statements. Cloud Spanner does not support executing a mix of DDL
statements with queries and/or updates in one batch or transaction.

You can solve this problem by either:
1. Make sure your batch only contains DDL statements. PGAdapter will execute these DDL statements as a single DDL batch on Cloud Spanner.
2. Start PGAdapter with the command line argument `-ddl=AutocommitImplicitTransaction`. This will cause PGAdapter to automatically commit any active implicit transaction when a DDL statement is encountered. Note that this does not make DDL statements transactional. It merely makes sure that the implicit transaction is committed before the DDL statement is executed. 

### Why does my DDL statement return the error message 'DDL statements are only allowed outside batches and transactions.'?
The error message 'DDL statements are only allowed outside batches and transactions.'  indicates that
you have tried to execute a batch of DDL statements while PGAdapter has been started with the command
line argument `-ddl=Single`. This option prohibits PGAdapter from executing batches of DDL statements.

You can solve this problem by either:
1. Execute the DDL statements as single statements instead of as a batch.
2. Remove the `-ddl=Single` command line argument when starting PGAdapter.

## COPY Statement
PGAdapter supports `COPY table_name FROM STDIN [BINARY]` and `COPY table_name TO STDOUT [BINARY]`.
These commands can be used to copy data between different Cloud Spanner databases, or between a
Cloud Spanner database and a PostgreSQL database. See [copy support](copy.md) for more information.

## Can I use user-defined functions or stored procedures?
PGAdapter and Cloud Spanner do not support user-defined functions or stored procedures. You can
however add PGAdapter as a [foreign server](https://www.postgresql.org/docs/current/sql-createserver.html)
to a normal PostgreSQL database and create user-defined functions and stored procedures in that
database. Those functions can be used with data from Cloud Spanner, as all tables from Cloud Spanner
can be [imported as foreign tables](https://www.postgresql.org/docs/current/sql-importforeignschema.html)
into the PostgreSQL database.

The local PostgreSQL database should be considered the same as your application. That means that it:
1. Should only define code / logic. It should not store any actual data.
2. Should be disposable. Dropping and re-creating it should always be possible.
3. Should let Cloud Spanner do as much as possible of the data processing. It should only fetch the
   rows from Cloud Spanner that it actually needs.

__This feature can also be used as a workaround for built-in PostgreSQL functions that are not supported
by Cloud Spanner.__

See the [foreign data wrapper sample](../samples/foreign-data-wrapper) for a complete sample on how
to set this up.


## How can I set a statement timeout?
You can set the statement timeout that a connection should use by executing a
`set statement_timeout=<timeout>` statement. The timeout is specified in milliseconds.

Example:

```sql
-- Use a 10 second (10,000 milliseconds) timeout.
set statement_timeout=10000;
select *
from large_table
order by my_col;
```

