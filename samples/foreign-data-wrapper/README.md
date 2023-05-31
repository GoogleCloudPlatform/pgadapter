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

## Use Cases

Adding PGAdapter as a foreign server in a real PostgreSQL database can be useful if you need access
to PostgreSQL features that are not available in Cloud Spanner, such as:
* Built-in functions and operators that are not available in Cloud Spanner.
* User-defined functions.
* Stored procedures.
* PL/pgSQL procedural language.

Note that many of the above features are commonly used in PostgreSQL to reduce the amount of data
that needs to be transferred from the database server to a client for processing. This is not the
case when using a foreign data wrapper. This feature should therefore be used with care.

## Example

This example shows how you can use Docker Compose to start a PGAdapter and a PostgreSQL container
and link the two. Note that both containers are completely disposable and do not store any permanent
data.

1. Set up some environment variables. These will be used by all following steps.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export GOOGLE_CLOUD_PROJECT=my-project
export SPANNER_INSTANCE=my-instance
export SPANNER_DATABASE=my-database
```

2. (Optional) Create the Cloud Spanner PostgreSQL-dialect database if it does not already exist:

```shell
gcloud spanner databases create ${SPANNER_DATABASE} --instance=${SPANNER_INSTANCE} --database-dialect=POSTGRESQL
```

3. Start PGAdapter and PostgreSQL using Docker Compose. This will start both PGAdapter and PostgreSQL
   in a Docker container and connect both to the same Docker network.

```shell
docker compose up -d
```

4. Create a table in the Cloud Spanner database and insert a couple of test rows.

```shell
docker run -it --rm \
  --network foreign-data-wrapper_default \
  postgres psql -h pgadapter \
    -c "create table if not exists concerts (
          id bigint not null primary key,
          name varchar,
          start_time timestamptz,
          end_time timestamptz);" \
    -c "insert into concerts values (1, 'Open Air', '2020-03-01 19:30+01'::timestamptz, '2020-03-02 01:00+01'::timestamptz)" \
    -c "insert into concerts values (2, 'Closed Air', '2020-04-01 13:30+01'::timestamptz, '2020-04-01 18:00+01'::timestamptz)"
```

5. Add PGAdapter as a foreign server to the PostgreSQL instance and add the Cloud Spanner table as a
   foreign table in the PostgreSQL database:

```shell
docker run -it --rm \
  --network foreign-data-wrapper_default \
  --env PGPASSWORD=mysecret \
  postgres psql -h postgres -U postgres \
  -c "-- Create the foreign data wrapper extension.
      CREATE EXTENSION IF NOT EXISTS postgres_fdw;" \
  -c "-- Create a server. It is highly recommended to create a 'read-only' server if you are only going to
      -- query the foreign table. This will ensure that Cloud Spanner uses a read-only transaction for any
      -- query that is executed on the foreign table.
      CREATE SERVER IF NOT EXISTS pgadapter_read_only FOREIGN DATA WRAPPER postgres_fdw 
          OPTIONS (
              -- This is the host name of the PGAdapter container.
              host 'pgadapter', port '5432',
              -- Change to match your database id.
              dbname 'my-database',
              -- This instructs PGAdapter to use read-only transactions.
              options '-c spanner.readonly=true');" \
  -c "-- We must create a user mapping for the local user. The user is ignored by PGAdapter, unless PGAdapter
      -- has been started with the command line argument that requires clients to authenticate.
      CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER pgadapter_read_only
          OPTIONS (user 'postgres', password_required 'false');" \
  -c "-- Create a schema that we will use for the Cloud Spanner tables.
      -- This makes it easier to just drop and re-create and re-import the schema if
      -- the schema in Cloud Spanner has changed.
      CREATE SCHEMA IF NOT EXISTS spanner;" \
  -c "-- Switch to the spanner schema.
      SET search_path TO spanner;" \
  -c "-- Import the schema from Cloud Spanner.
      IMPORT FOREIGN SCHEMA public from server pgadapter_read_only into spanner;" \
  -c "-- Create a user-defined function in PostgreSQL.
      CREATE FUNCTION timestamp_diff_minutes(earlier timestamptz, later timestamptz) RETURNS integer AS \$\$
          SELECT (extract(epoch from later) - extract(epoch from earlier)) / 60;
      \$\$ LANGUAGE SQL;" \
  -c "-- Select data from Cloud Spanner and apply the user-defined function in data that is selected.
      SELECT name, timestamp_diff_minutes(start_time, end_time) as duration_in_minutes
      FROM concerts
      WHERE name='Open Air'"
```

The following command will start an interactive psql session on the PostgreSQL database. This
database has the `concerts` foreign table defined that actually resides in Cloud Spanner. You can
create user-defined functions or stored procedures and use these with the data from Cloud Spanner.

You can also use standard PostgreSQL functions that are not supported by Cloud Spanner. Normally,
the PostgreSQL foreign data wrapper extension will push down the execution of built-in functions to
the foreign server. You can prevent this by creating a user-defined function that is just a wrapper
around the built-in function:

```shell
docker run -it --rm \
  --network foreign-data-wrapper_default \
  --env PGPASSWORD=mysecret \
  postgres psql -h postgres -U postgres

set search_path to spanner;

create function to_char(ts timestamptz, format text) returns text as $$
  select pg_catalog.to_char(ts, format);
$$ language sql;

select to_char(start_time, 'HH12:MI:SS') from concerts;
```

### Re-importing the schema
Changes to the schema in Cloud Spanner are not directly visible in the imported schema in PostgreSQL.
You can re-import the schema in PostgreSQL by dropping, re-creating and re-importing the schema in
PostgreSQL:

```sql
drop schema spanner cascade;
create schema spanner;
import foreign schema public from server pgadapter_read_only into spanner;
```

## Limitations

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
        -- This assumes that PGAdapter runs on a host named 'pgadapter'.
        host 'pgadapter', port '5432',
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
       -- This assumes that PGAdapter runs on a host named 'pgadapter'.
        host 'pgadapter', port '5432',
        -- Change to match your database id.
        dbname 'my-database');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER pgadapter_read_write
   OPTIONS (user 'postgres', password_required 'false');

CREATE FOREIGN TABLE IF NOT EXISTS concerts_writeable (id bigint, name varchar, start_time timestamptz, end_time timestamptz)
    server pgadapter_read_write
    options (table_name 'concerts');

PREPARE insert_concert AS
    INSERT INTO concerts_writeable (id, name, start_time, end_time)
    VALUES ($1, $2, $3, $4);
EXECUTE insert_concert (3, 'Clean Air', '2023-06-01 20:00:00+01'::timestamptz, '2023-06-02 02:30:00+01'::timestamptz);
```
