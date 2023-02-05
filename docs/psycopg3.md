# PGAdapter - psycopg3 Connection Options

PGAdapter has experimental support for the [Python psycopg3 driver](https://www.psycopg.org/psycopg3/docs/index.html).

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

```python
import psycopg

# Replace localhost and 5432 with the host and port number where PGAdapter is running.
# sslmode=disable instructs psycopg3 to try plain text mode directly instead of first trying SSL.
with psycopg.connect("host=localhost port=5432 dbname=my-database sslmode=disable") as conn:
  conn.autocommit = True
  with conn.cursor() as cur:
    cur.execute("select 'Hello world!' as hello")
    print("Greeting from Cloud Spanner PostgreSQL:", cur.fetchone()[0])
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application:

```python
import psycopg

# '/tmp' is the default domain socket directory for PGAdapter. This can be changed using the -dir
# command line argument. 5432 is the default port number used by PGAdapter. Change this in the
# connection string if PGAdapter is running on a custom port.
with psycopg.connect("host=/tmp port=5432 dbname=my-database") as conn:
  conn.autocommit = True
  with conn.cursor() as cur:
    cur.execute("select 'Hello world!' as hello")
    print("Greeting from Cloud Spanner PostgreSQL:", cur.fetchone()[0])
```


## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.

## Limitations

### Named Cursors
`psycopg3` can be used to create [server-side cursors](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html#server-side-cursors).
Server-side cursors are sometimes also referred to as 'named cursors'. This feature is currently not
supported with PGAdapter.

Creating a server-side cursor in `psycopg3` with PGAdapter will cause an error like the following:

```
psycopg.errors.RaiseException: Unknown statement: DECLARE "my_cursor" CURSOR FOR SELECT * FROM my_table WHERE my_column=$1
```

## Performance Considerations

The following will give you the best possible performance when using `psycopg3` with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host. `psycopg3` uses `libpq` for the underlying connection.
See https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.2 for more information
on `libpq` connection string options.

PGAdapter uses `/tmp` as the default directory for domain sockets.

```python
with psycopg.connect("host=/tmp port=5432 dbname=my-database") as conn:
  conn.execute("SELECT 1")
```

### Autocommit or Read-Only Transactions
Use auto-commit or read-only transactions for workloads that only read data. Reading in a read/write
transaction takes unnecessary locks if you are not planning on executing any write operations.

#### Autocommit
Set the `Autocommit` property of a `Connection` object to `True` to enable autocommit.

```python
with psycopg.connect("host=localhost port=5432 dbname=my-database") as conn:
  conn.autocommit = True
  # The next command will not start a transaction.
  conn.execute("SELECT 1")
```

It can also be set directly when creating the connection:

```python
with psycopg.connect("host=localhost port=5432 dbname=my-database", autocommit=True) as conn:
  # The next command will not start a transaction.
  conn.execute("SELECT 1")
```

#### Read-Only Transaction
Set the `read_only` property of the connection to `True` to instruct `psycopg3` to create read-only
transactions instead of read/write transactions. A read-only transaction will not take any locks.
See the Cloud Spanner [read-only transaction documentation](https://cloud.google.com/spanner/docs/transactions#read-only_transactions)
for more information on read-only transactions.

NOTE: You must commit or rollback the read-only transaction in `psycopg3` to end the transaction.
There  is no semantic or performance difference between committing or rolling back a read-only
transaction.

```python
with psycopg.connect("host=localhost port=5432 dbname=my-database") as conn:
  conn.read_only = True
  # The next command will start a read-only transaction.
  conn.execute("SELECT 1")
  conn.execute("SELECT 2")
  # The commit call will mark the end of the transaction. There is no semantic difference between
  # committing or rolling back a read-only transaction.
  conn.commit()
```

### Batching / Pipelining
Use [batching / pipelining](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) for
optimal performance when executing multiple statements. This both saves round-trips between your
application and PGAdapter and between PGAdapter and Cloud Spanner.

You can batch any type of statement. A batch can also contain a mix of different types of statements,
such as both queries and DML statements. It is also possible (and recommended!) to batch DDL
statements, but it is not recommended to mix DDL statements with other types of statements in one
batch.

#### DML Batch Example

```python
with psycopg.connect("host=localhost port=5432 dbname=my-database") as conn:
  with conn.pipeline():
    curs = conn.cursor()
    curs.execute("INSERT INTO my_table (key, value) values (%s, %s)", (1, 'One',))
    curs.execute("INSERT INTO my_table (key, value) values (%s, %s)", (2, 'Two',))
    curs.execute("update my_table set value='Zero' where key=%s", (0,))
```

#### Execute Many
`psycopg3` implements the `executemany` method using pipelining. Prefer this method for executing
the same statement multiple times with different parameter values, for example for inserting
multiple rows into the same table.

```python
with psycopg.connect("host=localhost port=5432 dbname=my-database") as conn:
  curs = conn.cursor()
  # executemany is automatically translated to Batch DML by PGAdapter.
  curs.executemany(
    "INSERT INTO my_table (key, value) values (%s, %s)",
    [(1, "One",), (2, "Two",), (4, "Four",)])
  print("Insert count:", curs.rowcount)
```

Note that it is recommended to use `COPY` for [bulk insert operations](copy.md).

#### DDL Batch Example

```python
with psycopg.connect("host=localhost port=5432 dbname=my-database") as conn:
  # Note: Cloud Spanner does not support DDL transactions.
  # Set the connection to autocommit before executing any DDL statements.
  conn.autocommit = True
  curs = conn.cursor()
  with conn.pipeline():
    curs.execute("""
      create table singers (
        id         varchar not null primary key,
        version_id int not null,
        first_name varchar,
        last_name  varchar not null,
        full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
        active     boolean,
        created_at timestamptz,
        updated_at timestamptz
      )""")
    curs.execute("""
      create table albums (
        id               varchar not null primary key,
        version_id       int not null,
        title            varchar not null,
        marketing_budget numeric,
        release_date     date,
        cover_picture    bytea,
        singer_id        varchar not null,
        created_at       timestamptz,
        updated_at       timestamptz,
        constraint fk_albums_singers foreign key (singer_id) references singers (id)
      )""")
```