# Google Cloud Spanner PGAdapter - COPY support

PGAdapter supports `COPY table_name FROM STDIN`.

Other `COPY` variants, such as `COPY table_name FROM file_name` or `COPY table_name FROM PROGRAM`,
are not supported. `COPY table_name TO ...` is also not supported.

## COPY table_name FROM STDIN
`COPY table_name FROM STDIN` is supported. This option can be used to insert bulk data to a Cloud
Spanner database. `COPY` operations are atomic by default, but the standard transaction limits of
Cloud Spanner apply to these transactions. That means that at most 20,000 mutations can be included
in one `COPY` operation. `COPY` can also be executed in non-atomic mode by executing the statement
`SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing the copy operation.

Although only `STDIN` is supported, export files can still be imported using `COPY` by piping files
into `psql`. See the examples below.

### Atomic COPY example
```sql
create table numbers (number bigint not null primary key, name varchar);
```

```shell
cat numbers.txt | psql -h /tmp -d test-db -c "copy numbers from stdin;"
```

The above operation will fail if the `numbers.txt` file contains more than 20,000 mutations.

### Non-atomic COPY example
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

### Copy Data From PostgreSQL
`COPY` can also be used to stream data directly from a real PostgreSQL database to Cloud Spanner.
This makes it easy to quickly copy an entire table from PostgreSQL to Cloud Spanner, or to generate
test data for Cloud Spanner using PostgreSQL queries.

The following examples assume that a real PostgreSQL server is running on `localhost:5432` and
PGAdapter is running on `localhost:5433`.

```shell
psql -h localhost -p 5432 -d my-local-db \
  -c "copy (select i, to_char(i, 'fm000') from generate_series(1, 10) s(i)) to stdout" \
  | psql -h localhost -p 5433 -d my-spanner-db \
  -c "copy numbers from stdin;"
```

Larger datasets require that the Cloud Spanner database is set to `PARTITIONED_NON_ATOMIC` mode:

```shell
psql -h localhost -p 5432 -d my-local-db \
  -c "copy (select i, to_char(i, 'fm000') from generate_series(1, 1000000) s(i)) to stdout" \
  | psql -h localhost -p 5433 -d my-spanner-db \
  -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy numbers from stdin;"
```
