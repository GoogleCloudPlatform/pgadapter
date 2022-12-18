# PGAdapter - SQLAlchemy Connection Options

## Limitations
PGAdapter has experimental support for SQLAlchemy 1.4 with Cloud Spanner PostgreSQL databases.
Porting an existing application from PostgreSQL to Cloud Spanner will probably require code changes.
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
  user1 = User(name="user1", fullname="Full name of User 1",
                   addresses=[Address(email_address="user1@sqlalchemy.org")]
               )
  session.add(user1)
  session.commit()
```

## Full Sample and Limitations
[This directory](../samples/python/sqlalchemy-sample) contains a full sample of how to work with
`SQLAlchemy` with Cloud Spanner and PGAdapter. The sample readme file also lists the
[current limitations](../samples/python/sqlalchemy-sample/README.md#limitations) when working with
`SQLAlchemy`.
