# PGAdapter - psycopg2 Connection Options

PGAdapter supports the [Python psycopg2 driver](https://www.psycopg.org/) version 2.9.3 and higher.
This does not include [psycopg3](https://www.psycopg.org/psycopg3/).

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
- `psycopg2` by design never uses server side query parameters. This means that SQL statements
  that are syntactically equal and only differ in values will still be treated as different
  statements by Cloud Spanner. This will add additional parsing and planning time to each statement
  that is executed. See [Performance Considerations](#performance-considerations) for more information.
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

### Server Side Parameters
`psycopg2` does not use server side parameters. Instead, it will add all parameters as literals to
the SQL string. This means that SQL statements that are syntactically equal and only differ in the
values that are used in the query, will still be treated as different statements by Cloud Spanner.
This will add additional parsing and planning time to each statement that is executed. This makes
`psycopg2` less usable for applications that require low latency query round-trips.

The following function call in `psycopg2` will for example be converted to a SQL string that
contains literals instead of using actual query parameters.

```python
>>> cur.execute("""
...     INSERT INTO some_table (an_int, a_date, a_string)
...     VALUES (%s, %s, %s);
...     """,
...     (10, datetime.date(2005, 11, 18), "O'Reilly"))
```

Is translated to this SQL statement:

```python
INSERT INTO some_table (an_int, a_date, a_string)
VALUES (10, '2005-11-18', 'O''Reilly');
```

#### Alternative: Prepared Statements
A possible workaround for the lack of query parameter support in `psycopg2` is to use prepared
statements. The above example can be re-written to the following that will use query parameters.
The `my_insert_statement` can be reused multiple times to insert different rows.

```python
>>> cur.execute("""
...     PREPARE my_insert_statement AS 
...     INSERT INTO some_table (an_int, a_date, a_string)
...     VALUES ($1, $2, $3);
...     """)
>>> cur.execute("""
...     EXECUTE my_insert_statement (%s, %s, %s);
...     """,
...     (10, datetime.date(2005, 11, 18), "O'Reilly"))
>>> cur.execute("""
...     EXECUTE my_insert_statement (%s, %s, %s);
...     """,
...     (11, datetime.date(2005, 11, 19), "Shannon"))
```

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
