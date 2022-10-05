# Google Cloud Spanner PGAdapter - node-postgres Experimental Support

PGAdapter has __experimental support__ for the [node-postgres driver](https://node-postgres.com/)
version 8.8.0 and higher.

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

```typescript
const { Client } = require('pg');
const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'my-database',
});
await client.connect();
const res = await client.query("select 'Hello world!' as hello");
console.log(res.rows[0]);
await client.end();
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application, or the `/tmp` directory in the Docker container has been mapped to a
directory on the local machine:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v /tmp:/tmp
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

```typescript
const { Client } = require('pg');
const client = new Client({
  host: '/tmp',
  port: 5432,
  database: 'my-database',
});
await client.connect();
const res = await client.query("select 'Hello world!' as hello");
console.log(res.rows[0]);
await client.end();
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
