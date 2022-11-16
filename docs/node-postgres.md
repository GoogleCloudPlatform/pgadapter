# Google Cloud Spanner PGAdapter - node-postgres Experimental Support

PGAdapter supports the [node-postgres driver](https://node-postgres.com/) version 8.8.0 and higher.

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
console.log(res.rows[0].hello);
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
console.log(res.rows[0].hello);
await client.end();
```


## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more options for how to run PGAdapter.


## Performance Considerations

The following will give you the best possible performance when using node-postgres with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host. See https://node-postgres.com/features/connecting
for more information on connection options for node-postgres.

### Batching
Use the batching options that are available as SQL commands in PGAdapter to batch DDL or DML
statements. PGAdapter will combine DML and DDL statements that are executed in a batch into a single
request on Cloud Spanner. This can significantly reduce the overhead of executing multiple DML or
DDL statements.

Example for DML statements:

```typescript
  const sql = "insert into test (id, value) values ($1, $2)";
// This will start a DML batch for this client. All subsequent
// DML statements will be cached locally until RUN BATCH is executed.
await client.query("start batch dml");
await client.query({text: sql, values: [1, 'One']});
await client.query({text: sql, values: [2, 'Two']});
await client.query({text: sql, values: [3, 'Three']});
// This will send the DML statements to Cloud Spanner as one batch.
const res = await client.query("run batch");
console.log(res);
```

Example for DDL statements:

```typescript
// This will start a DDL batch for this client. All subsequent
// DDL statements will be cached locally until RUN BATCH is executed.
await client.query("start batch ddl");
await client.query("create table my_table1 (key varchar primary key, value varchar)");
await client.query("create table my_table2 (key varchar primary key, value varchar)");
await client.query("create index my_index1 on my_table1 (value)");
// This will send the DDL statements to Cloud Spanner as one batch.
const res = await client.query("run batch");
console.log(res);
```

## Limitations
- [Prepared statements](https://www.postgresql.org/docs/current/sql-prepare.html) are limited to at most 50 parameters.
