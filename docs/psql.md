# Google Cloud Spanner PGAdapter - PSQL Support

PGAdapter supports [psql](https://www.postgresql.org/docs/current/app-psql.html) versions 11, 12, 13 and 14.

## Usage

First start PGAdapter.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x
```

- PGAdapter automatically recognizes connections from psql and translates queries from `psql` for 
  meta commands like `\dt` into queries that are supported by Cloud Spanner.
- The `-x` option tells PGAdapter to accept incoming connections from other hosts than localhost.
  This is necessary when running PGAdapter in a Docker container, as you will be connecting to it
  from outside the container, which will be seen as a connection from a different host.

Then connect to PGAdapter using `psql`:

```shell
psql -h localhost
```

## Running alongside a local PostgreSQL server

If you have PostgreSQL installed locally on your machine, it is likely that PostgreSQL is
already using port 5432. You then need to run PGAdapter on a different port, and specify this port
when starting `psql` to connect to PGAdapter instead of the local PostgreSQL server:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5433:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x
psql -h localhost -p 5433
```

## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.

## Limitations
- Server side [prepared statements](https://www.postgresql.org/docs/current/sql-prepare.html) are limited to at most 50 parameters.
