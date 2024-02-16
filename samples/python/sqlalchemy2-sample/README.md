# PGAdapter and SQLAlchemy 2.x

PGAdapter supports [SQLAlchemy 2.0](https://docs.sqlalchemy.org/en/20/)
with the `psycopg` driver. This document shows how to use this sample application, and lists the
limitations when working with `SQLAlchemy 2.x` with PGAdapter.

The [sample.py](sample.py) file contains a sample application using `SQLAlchemy 2.x` with PGAdapter.
Use this as a reference for features of `SQLAlchemy 2.x` that are supported with PGAdapter. This sample
assumes that the reader is familiar with `SQLAlchemy 2.x`, and it is not intended as a tutorial for how
to use `SQLAlchemy` in general.

See [Limitations](#limitations) for a full list of known limitations when working with `SQLAlchemy 2.x`.

## Running the Sample

You can run the sample directly on the Spanner emulator with the following command. It will
automatically start both PGAdapter and the Spanner emulator. This requires Docker on the local
machine to work:

```shell
python run_sample.py
```

You can also connect to a real Spanner instance instead of the emulator by running the sample like
this. The database must exist. The sample will automatically create the tables that are needed for
the sample:

```shell
python run_sample.py \
  --project my-project \
  --instance my-instance \
  --database my-database \
  --credentials /path/to/credentials.json
```


### Start PGAdapter Manually
You can also start PGAdapter before you run the sample and connect to that PGAdapter instance. The
following command shows how to start PGAdapter using the pre-built Docker image and then run the
sample against that instance.
See [Running PGAdapter](../../../README.md#usage) for more information on other options for how to run PGAdapter.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  -v /tmp:/tmp \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
python run_sample.py \
  --host localhost \
  --port 5432 \
  --database my-database
```

## Creating the Sample Data Model
The sample data model is created automatically by the sample script.

The sample data model contains example tables that cover all supported data types the Cloud Spanner
PostgreSQL dialect. It also includes an example for how [interleaved tables](https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#extensions_to)
can be used with SQLAlchemy. Interleaved tables is a Cloud Spanner extension of the standard
PostgreSQL dialect.

The corresponding SQLAlchemy model is defined in [model.py](model.py).

You can also create the sample data model manually using for example `psql`.
Run the following command in this directory. Replace the host, port and database name with the
actual host, port and database name for your PGAdapter and database setup.

```shell
psql -h localhost -p 5432 -d my-database -f create_data_model.sql
```

You can also drop an existing data model using the `drop_data_model.sql` script:

```shell
psql -h localhost -p 5432 -d my-database -f drop_data_model.sql
```

## Data Types
Cloud Spanner supports the following data types in combination with `SQLAlchemy 2.x`.

| PostgreSQL Type                        | SQLAlchemy type         |
|----------------------------------------|-------------------------|
| boolean                                | Boolean                 |
| bigint / int8                          | Integer, BigInteger     |
| varchar                                | String                  |
| text                                   | String                  |
| float8 / double precision              | Float                   |
| numeric                                | Numeric                 |
| timestamptz / timestamp with time zone | DateTime(timezone=True) |
| date                                   | Date                    |
| bytea                                  | LargeBinary             |
| jsonb                                  | JSONB                   |
| Arrays                                 | Column()                |

## Limitations
The following limitations are currently known:

| Limitation                     | Workaround                                                                                                                                                                                                                                                          |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Creating and Dropping Tables   | Cloud Spanner does not support the full PostgreSQL DDL dialect. Automated creation of tables using `SQLAlchemy` is therefore not supported.                                                                                                                         |
| metadata.reflect()             | Cloud Spanner does not support all PostgreSQL `pg_catalog` tables. Using `metadata.reflect()` to get the current objects in the database is therefore not supported.                                                                                                |
| DDL Transactions               | Cloud Spanner does not support DDL statements in a transaction. Add `?options=-c spanner.ddl_transaction_mode=AutocommitExplicitTransaction` to your connection string to automatically convert DDL transactions to [non-atomic DDL batches](../../../docs/ddl.md). |
| INSERT ... ON CONFLICT         | `INSERT ... ON CONFLICT` is not supported.                                                                                                                                                                                                                          |
| SAVEPOINT                      | Rolling back to a `SAVEPOINT` can fail if the transaction contained at least one query that called a volatile function.                                                                                                                                             |
| SELECT ... FOR UPDATE          | `SELECT ... FOR UPDATE` is not supported.                                                                                                                                                                                                                           |
| Server side cursors            | Server side cursors are currently not supported.                                                                                                                                                                                                                    |
| Transaction isolation level    | Only SERIALIZABLE and AUTOCOMMIT are supported. `postgresql_readonly=True` is also supported. It is recommended to use either autocommit or read-only for workloads that only read data and/or that do not need to be atomic to get the best possible performance.  |
| Stored procedures              | Cloud Spanner does not support Stored Procedures.                                                                                                                                                                                                                   |
| User defined functions         | Cloud Spanner does not support User Defined Functions.                                                                                                                                                                                                              |
| Other drivers than psycopg 3.x | PGAdapter does not support using SQLAlchemy 2.x with any other drivers than `psycopg 3.x`.                                                                                                                                                                          |

### Generated Primary Keys
Generated primary keys can be used in combination with a bit-reversed sequence.

The `TicketSale` model in this sample application uses an auto-generated primary key that is
generated from a bit-reversed sequence:
1. See [model.py](model.py) for the model definition.
2. See [create_data_model.sql](create_data_model.sql) for the sequence and table definition.

See https://cloud.google.com/spanner/docs/primary-key-default-value#bit-reversed-sequence for more
information on bit-reversed sequences in Cloud Spanner.

#### Example Mapping for Generated Primary Key using a Bit-Reversed Sequence

Python model definition:

```python
class Singer(Base):
  id = Column(Integer, primary_key=True)
  name = Column(String(100))

singer = Singer(name="Alice")
session.add(singer)
session.commit()
```

Sequence and table definition:

```sql
create sequence if not exists singers_seq bit_reversed_positive;
create table if not exists singers (
  id   bigint not null primary key default nextval('singers_seq'),
  name varchar not null
);
```

### ON CONFLICT Clauses
`INSERT ... ON CONFLICT ...` are not supported by Cloud Spanner and should not be used. Trying to
use https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.Insert.on_conflict_do_update
or https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.Insert.on_conflict_do_nothing
will fail.

### SAVEPOINT - Nested transactions
Rolling back to a `SAVEPOINT` can fail if the transaction contained at least one query that called a
volatile function or if the underlying data that has been accessed by the transaction has been
modified by another transaction.

### Locking - SELECT ... FOR UPDATE
Locking clauses, like `SELECT ... FOR UPDATE`, are not supported (see also https://docs.sqlalchemy.org/en/20/orm/queryguide/query.html#sqlalchemy.orm.Query.with_for_update).
These are normally also not required, as Cloud Spanner uses isolation level `serializable` for
read/write transactions.

## Performance Considerations

### Read-only Transactions
SQLAlchemy will by default use read/write transactions for all database operations, including for
workloads that only read data. This will cause Cloud Spanner to take locks for all data that is read
during the transaction. It is recommended to use either autocommit or [read-only transactions](https://cloud.google.com/spanner/docs/transactions#read-only_transactions)
for workloads that are known to only execute read operations. Read-only transactions do not take any
locks. You can create a separate database engine that can be used for read-only transactions from
your default database engine by adding the `postgresql_readonly=True` execution option.

```python
read_only_engine = engine.execution_options(postgresql_readonly=True)
```

### Autocommit
Using isolation level `AUTOCOMMIT` will suppress the use of (read/write) transactions for each
database operation in SQLAlchemy. Using autocommit is more efficient than read/write transactions
for workloads that only read and/or that do not need the atomicity that is offered by transactions.

You can create a separate database engine that can be used for workloads that do not need
transactions by adding the `isolation_level="AUTOCOMMIT"` execution option to your default database
engine.

```python
autocommit_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
```

### Stale reads
Read-only transactions and database engines using `AUTOCOMMIT` will by default use strong reads for
queries. Cloud Spanner also supports stale reads.

* A strong read is a read at a current timestamp and is guaranteed to see all data that has been
  committed up until the start of this read. Spanner defaults to using strong reads to serve read requests.
* A stale read is read at a timestamp in the past. If your application is latency sensitive but
  tolerant of stale data, then stale reads can provide performance benefits.

See also https://cloud.google.com/spanner/docs/reads#read_types

You can create a database engine that will use stale reads in autocommit mode by adding the following
to the connection string and execution options of the engine:

```python
conn_string = "postgresql+psycopg://user:password@localhost:5432/my-database" \
              "?options=-c spanner.read_only_staleness='MAX_STALENESS 10s'"
engine = create_engine(conn_string).execution_options(isolation_level="AUTOCOMMIT")
```
