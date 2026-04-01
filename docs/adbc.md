# PGAdapter - ADBC Connection Options

PGAdapter supports the [Python ADBC driver for PostgreSQL](https://arrow.apache.org/adbc/).

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

Then connect to PGAdapter using TCP like this:

```python
import adbc_driver_postgresql.dbapi

# Replace localhost and 5432 with the host and port number where PGAdapter is running.
with adbc_driver_postgresql.dbapi.connect("host=localhost port=5432 dbname=my-database") as conn:
  with conn.cursor() as cur:
    cur.execute("select 'Hello world!' as hello")
    print("Greeting from Cloud Spanner PostgreSQL:", cur.fetchone()[0])
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application:

```python
import adbc_driver_postgresql.dbapi

# '/tmp' is the default domain socket directory for PGAdapter. This can be changed using the -dir
# command line argument. 5432 is the default port number used by PGAdapter. Change this in the
# connection string if PGAdapter is running on a custom port.
with adbc_driver_postgresql.dbapi.connect("host=/tmp port=5432 dbname=my-database") as conn:
  with conn.cursor() as cur:
    cur.execute("select 'Hello world!' as hello")
    print("Greeting from Cloud Spanner PostgreSQL:", cur.fetchone()[0])
```

## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more possibilities on how to run PGAdapter.