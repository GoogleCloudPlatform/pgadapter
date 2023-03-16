# PGAdapter - SQLAlchemy 2.x Connection Options

## Limitations
PGAdapter has experimental support for SQLAlchemy 2.x with Cloud Spanner PostgreSQL databases. It 
has been tested with SQLAlchemy 2.0.1 and psycopg 3.1.8. Developing new applications using
SQLAlchemy 2.x is possible as long as the listed limitations are taken into account.
Porting an existing application from PostgreSQL to Cloud Spanner is likely to require code changes.

PGAdapter does not support SQLAlchemy 2.x with any other engines than psycopg 3.x.

See [Limitations](../samples/python/sqlalchemy2-sample/README.md#limitations) in the `sqlalchemy2-sample`
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

Then connect to PGAdapter and use `SQLAlchemy 2.x` like this (note the use of psycopg and NOT psycopg2):

```python
conn_string = "postgresql+psycopg://user:password@localhost:5432/my-database"
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

## Full Sample and Limitations
[This directory](../samples/python/sqlalchemy2-sample) contains a full sample of how to work with
`SQLAlchemy 2.x` with Cloud Spanner and PGAdapter. The sample readme file also lists the
[current limitations](../samples/python/sqlalchemy2-sample/README.md#limitations) when working with
`SQLAlchemy 2.x`.
