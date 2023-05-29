# Foreign Data Wrapper

PGAdapter can be used as a foreign server in a real PostgreSQL database. Tables in Cloud Spanner can
be added to a local PostgreSQL database as foreign tables. This enables the use of some PostgreSQL
features that are not available in Cloud Spanner, such as user-defined functions, stored procedures,
or missing system functions.

Note that PostgreSQL in this case should be treated as a 'client'. That is; executing a user-defined
function on data in a Cloud Spanner database requires PostgreSQL to fetch the data from Cloud Spanner
and execute the function locally. You should therefore use a WHERE clause that will limit the data
that is fetched as much as possible. The WHERE clause will be pushed down to Cloud Spanner, as long
as the WHERE clause uses standard comparison operators and built-in functions.

## Example

You can add a Cloud Spanner table as a foreign table in a local PostgreSQL database by following
these steps:

```sql
-- Create the foreign data wrapper extension.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create a server. It is highly recommended to create a 'read-only' server if you are only going to
-- query the foreign table. This will ensure that Cloud Spanner uses a read-only transaction for any
-- query that is executed on the foreign table.
CREATE SERVER IF NOT EXISTS pgadapter_read_only FOREIGN DATA WRAPPER postgres_fdw 
    OPTIONS (
        -- This assumes that PGAdapter accepts Unix Domain Sockets on '/tmp'.
        host '/tmp',
        -- This assumes that PGAdapter runs on local port 9002.
        port '9002',
        -- Change to match your database id.
        dbname 'my-database',
        -- This instructs PGAdapter to use read-only transactions.
        options '-c spanner.readonly=true');

-- We must create a user mapping for the local user. The user is ignored by PGAdapter, unless PGAdapter
-- has been started with the command line argument that requires clients to authenticate.
CREATE USER MAPPING IF NOT EXISTS FOR myuser SERVER pgadapter_read_only
    OPTIONS (user 'myuser', password_required 'false');

-- Grant access to the user in the user mapping.
GRANT USAGE ON FOREIGN SERVER pgadapter_read_only TO myuser;

-- Create a foreign table for a table in Cloud Spanner. 
-- The table name in the local database will be `f_test`. The table name in Cloud Spanner is `test`.
-- Note that the IMPORT FOREIGN SCHEMA command does not work with Cloud Spanner.
CREATE FOREIGN TABLE IF NOT EXISTS f_concerts (id bigint, name varchar, start_time timestamptz, end_time timestamptz)
    server pgadapter_read_only
    options (table_name 'concerts');

-- You can now create a user-defined function in PostgreSQL and apply that function to data that is
-- fetched from Cloud Spanner.
CREATE FUNCTION timestamp_diff_minutes(earlier timestamptz, later timestamptz) RETURNS integer AS $$
    SELECT (extract(epoch from later) - extract(epoch from earlier)) / 60;
$$ LANGUAGE SQL;

-- Select data from Cloud Spanner and apply the user-defined function in data that is selected.
SELECT name, timestamp_diff_minutes(start_time, end_time) as duration_in_minutes
FROM f_concerts
WHERE name='Open Air'
```

## Limitations

### Import Foreign Schema
The `IMPORT FOREIGN SCHEMA` statement is not supported for Cloud Spanner databases.

### Parameterized Queries for Foreign Tables
You can use parameterized queries with Cloud Spanner foreign tables. The PostgreSQL Foreign Data
Wrapper extension will however replace any parameter values with literals before sending the query
to Cloud Spanner. That means that Cloud Spanner will see the query as an un-parameterized query,
which again means that Cloud Spanner will need to re-compile the query each time the statement is
executed with different parameter values.

Example:

```sql
PREPARE my_statement AS SELECT * FROM f_concerts WHERE name=$1;
EXECUTE my_statement ('Open Air');

-- postgresql_fdw will internally translate the above prepared statement into the following query
-- string that is sent to Cloud Spanner:
SELECT * FROM f_concerts WHERE name='Open Air';
```

### Transactions
The PostgreSQL Foreign Data Wrapper extension fetches data from Cloud Spanner using a CURSOR.
Cursors can only be used in transactions. This means that postgres_fdw will always start a
transaction before executing a query or any other statement on a foreign table. It is recommended to
use a read-only transaction for statements that only read. The best way to achieve this is by
defining a FOREIGN SERVER that uses a read-only connection to Cloud Spanner. You can specify this
by adding the option `-c spanner.read_only=true` to the connection options:

```sql
CREATE SERVER IF NOT EXISTS pgadapter_read_only FOREIGN DATA WRAPPER postgres_fdw 
    OPTIONS (
        -- This assumes that PGAdapter accepts Unix Domain Sockets on '/tmp'.
        host '/tmp',
        -- This assumes that PGAdapter runs on local port 9002.
        port '9002',
        -- Change to match your database id.
        dbname 'my-database',
        -- This instructs PGAdapter to use read-only transactions.
        options '-c spanner.readonly=true');
```

### Writing Data
The PostgreSQL Foreign Data Wrapper extension also supports writing data to a foreign table. You
should only use this if your insert/update/delete statement requires a feature that is not available
in Cloud Spanner, as writing through a foreign table adds latency to each write operation in two
ways:
1. Each statement first has to go through the local PostgreSQL server to PGAdapter and then to Cloud Spanner.
2. The PostgreSQL Foreign Data Wrapper extension replaces any query parameters with literals. This means
   that Cloud Spanner needs to re-compile the statement each time it is executed with new values.

#### Example

```sql
CREATE SERVER IF NOT EXISTS pgadapter_read_write FOREIGN DATA WRAPPER postgres_fdw 
    OPTIONS (
        -- This assumes that PGAdapter accepts Unix Domain Sockets on '/tmp'.
        host '/tmp',
        -- This assumes that PGAdapter runs on local port 9002.
        port '9002',
        -- Change to match your database id.
        dbname 'my-database');

CREATE FOREIGN TABLE IF NOT EXISTS f_concerts_writeable (id bigint, name varchar, start_time timestamptz, end_time timestamptz)
    server pgadapter_read_write
    options (table_name 'concerts');

PREPARE insert_concert AS
    INSERT INTO f_concerts_writeable (id, name, start_time, end_time)
    VALUES ($1, $2, $3, $4);
EXECUTE insert_concert (1, 'Open Air', '2023-06-01 20:00:00+01'::timestamptz, '2023-06-02 02:30:00+01'::timestamptz);
```
