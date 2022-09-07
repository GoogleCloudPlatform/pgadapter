# Google Cloud Spanner PGAdapter - psycopg2 Experimental Support

PGAdapter has __experimental support__ for the [Python psycopg2 driver](https://www.psycopg.org/)
version 2.9.3 and higher.

## Usage

First start PGAdapter:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -v /tmp:/tmp:rw \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

Then connect to PGAdapter like this:

```python
'''
Replace 'my-database' with the actual name of your database.
Replace localhost and 5432 with the host and port number where PGAdapter is running.
'''

connection = psycopg2.connect(database="my-database",
                              host="localhost",
                              port=5432)

cursor = connection.cursor()
cursor.execute('select \'Hello World\'')
for row in cursor:
  print(row)

cursor.close()
connection.close()
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application:

```python
'''
'/tmp' is the default domain socket directory for PGAdapter. This can be changed using the -dir
command line argument. 5432 is the default port number used by PGAdapter. Change this in the
connection string if PGAdapter is running on a custom port.
'''
connection = psycopg2.connect(database="my-database",
                              host="/tmp",
                              port=5432)

cursor = connection.cursor()
cursor.execute('select \'Hello World\'')
for row in cursor:
  print(row)

cursor.close()
connection.close()
```


## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.

## Limitations and Known Bugs
- Only [copy_expert](https://www.psycopg.org/docs/cursor.html#cursor.copy_expert) is supported.
  [copy_from](https://www.psycopg.org/docs/cursor.html#cursor.copy_from) and [copy_to](https://www.psycopg.org/docs/cursor.html#cursor.copy_to) are NOT supported.
- [copy_expert](https://www.psycopg.org/docs/cursor.html#cursor.copy_expert) has the following additional limitations:
  - copy_expert does not support NULL option
  - copy_expert does not support escape characters for the DELIMITER option
  - copy_expert does not support the `AS` keyword
- `set transaction_isolation level to default` is not supported.
- `set_session(deferrable = false)` is not supported.
- `connection.lobject()` is not supported
- Most `pg_catalog` tables and functions are not supported
- The Python datetime module only supports timestamp up to microsecond precision. Cloud Spanner
  supports timestamps with nanosecond precision. The nanosecond precision will be lost when used
  with the `psycopg2` driver.


## Performance Considerations

The following will give you the best possible performance when using psycopg2 with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host.

### Batching Using execute_batch
Use `execute_batch` in `psycopg2.extras` for better performance when executing multiple DML
statements. See https://www.psycopg.org/docs/extras.html#fast-execution-helpers for more information. 

### Batching Using Semi-colon Separated Statements
You can also run multiple DML or DDL statements as a single batch by sending these in a semi-colon
separated string to PGAdapter.

Example for DML statements:

```python
connection = psycopg2.connect(database="my-database",
                              host="localhost",
                              port=5432)
connection.autocommit = True
cursor = connection.cursor()
cursor.execute("""
   insert into my_table (id, value) values (1, 'value 1');
   insert into my_table (id, value) values (2, 'value 2');
   """)
```

Example for DDL statements:

```python
connection = psycopg2.connect(database="my-database",
                              host="localhost",
                              port=5432)
connection.autocommit = True
cursor = connection.cursor()
cursor.execute("""
   create table singers (
     singer_id bigint not null primary key,
     first_name varchar,
     last_name varchar
   );
   create index idx_singers_last_name on singers (last_name);
   """)
```
