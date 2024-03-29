# PGAdapter - SQLAlchemy 1.4 Connection Options

__It is recommended to use SQLAlchemy 2.x with psycopg 3.x when working with PGAdapter.__
See [SQLAlchemy 2.x Connection Options](sqlalchemy2.md) for more information on how to use SQLAlchemy 2.x.

## Limitations
PGAdapter has experimental support for SQLAlchemy 1.4 with Cloud Spanner PostgreSQL databases. It 
has been tested with SQLAlchemy 1.4.45 and psycopg2 2.9.3. Developing new applications using
SQLAlchemy is possible as long as the listed limitations are taken into account.
Porting an existing application from PostgreSQL to Cloud Spanner is likely to require code changes.

It is __recommended to use SQLAlchemy 2.x in combination with psycopg 3.x__ instead of SQLAlchemy 1.4
with psycopg2. Psycopg2 never uses server-side query parameters. This will increase the latency of all
queries that are executed by SQLAlchemy. See [SQLAlchemy 2.x Connection Options](sqlalchemy2.md) for
more information on how to use SQLAlchemy 2.x.

See [Limitations](../samples/python/sqlalchemy-sample/README.md#limitations) in the `sqlalchemy-sample`
directory for a full list of limitations.

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

Then connect to PGAdapter and use `SQLAlchemy` like this:

```python
conn_string = "postgresql+psycopg2://user:password@localhost:5432/my-database"
engine = create_engine(conn_string)
with Session(engine) as session:
  user1 = User(
    name="user1",
    fullname="Full name of User 1",
    addresses=[Address(email_address="user1@sqlalchemy.org")]
  )
  session.add(user1)
  session.commit()
```

## Limitations
`SQLAlchemy 1.x` uses `psycopg2` for connecting to `PostgreSQL`. `psycopg2` does not use parameterized
queries. This affects query execution negatively on Cloud Spanner. It is therefore recommended to
use `SQLAlchemy 2.x` instead of `SQLAlchemy 1.x` with Cloud Spanner and PGAdapter.
* See [SQLAlchemy 1.x limitations](../samples/python/sqlalchemy-sample/README.md#limitations) for more information about this limitation.
* See [SQLAlchemy 2.x sample](../samples/python/sqlalchemy2-sample) for more information about using `SQLAlchemy 2.x` with PGAdapter.

## Full Sample and Limitations
[This directory](../samples/python/sqlalchemy-sample) contains a full sample of how to work with
`SQLAlchemy` with Cloud Spanner and PGAdapter. The sample readme file also lists the
[current limitations](../samples/python/sqlalchemy-sample/README.md#limitations) when working with
`SQLAlchemy`.
