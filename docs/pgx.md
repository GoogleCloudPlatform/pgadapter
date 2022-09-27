# Google Cloud Spanner PGAdapter - pgx Experimental Support

PGAdapter has __experimental support__ for the [Go pgx driver](https://github.com/jackc/pgx)
version 4.15.0 and higher. 

## Usage

First start PGAdapter:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

Then connect to PGAdapter using TCP like this:

```go
// pwd:uid is not used by PGAdapter, but it is required in the connection string.
// Replace localhost and 5432 with the host and port number where PGAdapter is running.
// sslmode=disable instructs pgx to try plain text mode directly. Otherwise, pgx will try two times
// with SSL enabled before trying plain text.
connString := "postgres://uid:pwd@localhost:5432/my-database?sslmode=disable"
ctx := context.Background()
conn, err := pgx.Connect(ctx, connString)
if err != nil {
    return err
}
defer conn.Close(ctx)

var greeting string
err = conn.QueryRow(ctx, "select 'Hello world!' as hello").Scan(&greeting)
if err != nil {
    return err
}
fmt.Printf("Greeting from Cloud Spanner PostgreSQL: %v\n", greeting)
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application:

```go
// '/tmp' is the default domain socket directory for PGAdapter. This can be changed using the -dir
// command line argument. 5432 is the default port number used by PGAdapter. Change this in the
// connection string if PGAdapter is running on a custom port.
connString := "host=/tmp port=5432 database=my-database"
ctx := context.Background()
conn, err := pgx.Connect(ctx, connString)
if err != nil {
    return err
}
defer conn.Close(ctx)

var greeting string
err = conn.QueryRow(ctx, "select 'Hello world!' as hello").Scan(&greeting)
if err != nil {
    return err
}
fmt.Printf("Greeting from Cloud Spanner PostgreSQL: %v\n", greeting)
```


## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.


## Performance Considerations

The following will give you the best possible performance when using pgx with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host. See https://pkg.go.dev/github.com/jackc/pgx#ConnConfig
for more information on connection options for pgx.

### Batching
Use the pgx batching API when executing multiple DDL or DML statements. PGAdapter will combine
DML and DDL statements that are executed in a batch into a single request on Cloud Spanner.
This can significantly reduce the overhead of executing multiple DML or DDL statements.

Example for DML statements:

```go
batch := &pgx.Batch{}
batch.Queue("insert into my_table (key, value) values ($1, $2)", "k1", "value1")
batch.Queue("insert into my_table (key, value) values ($1, $2)", "k2", "value2")
res := conn.SendBatch(context.Background(), batch)
```

Example for DDL statements:

```go
batch := &pgx.Batch{}
batch.Queue("create table singers (singerid varchar primary key, name varchar)")
batch.Queue("create index idx_singers_name on singers (name)")
res := conn.SendBatch(context.Background(), batch)
```

## Limitations
- Server side [prepared statements](https://www.postgresql.org/docs/current/sql-prepare.html) are limited to at most 50 parameters.
  `pgx` uses server side prepared statements for all parameterized statements in extended query mode.
  You can use the [simple query protocol](https://pkg.go.dev/github.com/jackc/pgx/v4#QuerySimpleProtocol) to work around this limitation.
