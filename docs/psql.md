# Google Cloud Spanner PostgreSQL Adapter - JDBC Support

PGAdapter supports [psql](https://www.postgresql.org/docs/current/app-psql.html) versions 11, 12, 13 and 14.

## Usage

First start PGAdapter in `psql` mode (see [README](../README.md) for more possibilities on how to run
PGAdapter).

```shell
docker pull us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter-docker-images/pgadapter
docker run \
  -d -p 5432:5432 \
  -v /local/path/to/credentials.json:/tmp/keys/key.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
  us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x -q
```

Note:
- The `-x` option tells PGAdapter to accept incoming connections from other hosts than localhost.
  This is necessary when running PGAdapter in a Docker container, as you will be connecting to it
  from outside the container, which will be seen as a connection from a different host.
- The `-q` option tells PGAdapter to translate queries from `psql` for meta commands like `\dt` into
  queries that are supported by Cloud Spanner.

Then connect to PGAdapter using `psql`:

```shell
psql -h localhost
```

## Running alongside a local PostgreSQL server

If you have PostgreSQL installed locally on your machine, it is highly likely that PostgreSQL is
already using port 5432. You then need to run PGAdapter on a different port, and specify this port
when starting `psql` to connect to PGAdapter instead of the local PostgreSQL server:

```shell
docker pull us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter-docker-images/pgadapter
docker run \
  -d -p 5433:5432 \
  -v /local/path/to/credentials.json:/tmp/keys/key.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
  us-west1-docker.pkg.dev/cloud-spanner-pg-adapter/pgadapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x -q
psql -h localhost -p 5433
```
