# Google Cloud Spanner PGAdapter - COPY support

PGAdapter supports `COPY table_name FROM STDIN [BINARY]` and `COPY table_name TO STDOUT [BINARY]`.

`COPY table_name FROM/TO file_name` and `COPY table_name FROM/TO PROGRAM` are not supported.
You can still copy data from/to a file by redirecting STDIN/STOUT to a file.

## COPY table_name FROM STDIN [BINARY]
`COPY table_name FROM STDIN [BINARY]` is supported. This feature can be used to insert bulk data to a Cloud
Spanner database. `COPY FROM STDIN` operations are atomic by default, but the standard transaction limits of
Cloud Spanner apply to these transactions. That means that at most 20,000 mutations can be included
in one `COPY` operation. `COPY FROM STDIN` can also be executed in non-atomic mode by executing the statement
`SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing the copy operation.

Although only `STDIN` is supported, export files can still be imported using `COPY` by piping files
into `psql`. See the examples below.

## COPY table_name TO STDOUT [BINARY]
`COPY table_name TO STDOUT [BINARY]` is supported. This option can be used to download bulk data from a Cloud
Spanner database. `COPY TO STDOUT` operations use a read-only transaction. This guarantees that the
copied data will represent a consistent snapshot of the data in the table.

`COPY TO STDOUT` currently does not support copying data using a query expression. Instead, a `VIEW`
can be created for the query expression that you want to copy, and then be used as the table name in
a `COPY view_name TO STDOUT` command.

Although only `STDOUT` is supported, data can still be exported to a file using `COPY` by redirecting
STDOUT to a file. See the examples below.

### Atomic COPY FROM STDIN example
```sql
create table numbers (number bigint not null primary key, name varchar);
```

```shell
cat numbers.txt | psql -h /tmp -d test-db -c "copy numbers from stdin;"
```

The above operation will fail if the `numbers.txt` file contains more than 20,000 mutations.

### Non-atomic COPY FROM STDIN example
```sql
create table numbers (number bigint not null primary key, name varchar);
```

```shell
cat numbers.txt | psql -h /tmp -d test-db -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy numbers from stdin;"
```

The above operation will automatically split the data over multiple transactions if the file
contains more than 20,000 mutations.

Note that this also means that if an error is encountered
during the `COPY` operation, some rows may already have been persisted to the database. This will
not be rolled back after the error was encountered. The transactions are executed in parallel,
which means that data after the row that caused the error in the import file can still have been
imported to the database before the `COPY` operation was halted.

### COPY TO STDOUT example

```shell
psql -h /tmp -d test-db -c "copy numbers to stdout;" > numbers.txt
```


### Copy Data From PostgreSQL
`COPY` can also be used to stream data directly from a real PostgreSQL database to Cloud Spanner.
This makes it easy to quickly copy an entire table from PostgreSQL to Cloud Spanner, or to generate
test data for Cloud Spanner using PostgreSQL queries.

The following examples assume that a real PostgreSQL server is running on `localhost:5432` and
PGAdapter is running on `localhost:5433`. These examples use the `binary` copy format. This format
is recommended if the data types of the tables in both databases are equal, as the format is more
efficient and does not require escaping of special characters or `null` values.

```shell
psql -h localhost -p 5432 -d my-local-db \
  -c "copy (select i, to_char(i, 'fm000') from generate_series(1, 10) s(i)) to stdout binary" \
  | psql -h localhost -p 5433 -d my-spanner-db \
  -c "copy numbers from stdin binary;"
```

Larger datasets require that the Cloud Spanner database is set to `PARTITIONED_NON_ATOMIC` mode:

```shell
psql -h localhost -p 5432 -d my-local-db \
  -c "copy (select i, to_char(i, 'fm000') from generate_series(1, 1000000) s(i)) to stdout binary" \
  | psql -h localhost -p 5433 -d my-spanner-db \
  -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy numbers from stdin binary;"
```

### Copy Data To PostgreSQL
`COPY` can be used to stream data directly from a Cloud Spanner database to a PostgreSQL database.
This makes it easy to quickly create a local backup of a Cloud Spanner table.

The following examples assume that a real PostgreSQL server is running on `localhost:5432` and
PGAdapter is running on `localhost:5433`. These examples use the `binary` copy format. This format
is recommended if the data types of the tables in both databases are equal, as the format is more
efficient and does not require escaping of special characters or `null` values.

```shell
psql -h localhost -p 5433 -d my-spanner-db \
  -c "copy numbers to stdout binary;" \
  | psql -h localhost -p 5432 -d my-local-db \
  -c "copy numbers from stdin binary"
```
