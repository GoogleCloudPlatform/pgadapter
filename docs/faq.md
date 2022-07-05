# Google Cloud Spanner PGAdapter - Frequently Asked Questions

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
PGAdapter supports `COPY table_name FROM STDIN`. See [copy support](copy.md) for more information.

Other `COPY` variants, such as `COPY table_name FROM file_name` or `COPY table_name FROM PROGRAM`,
are not supported. `COPY table_name TO ...` is also not supported.
