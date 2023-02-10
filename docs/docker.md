# Google Cloud Spanner PGAdapter - Docker

## Basic Usage

Each version of PGAdapter is published as a pre-built Docker image. You can pull and run this Docker
image without the need to build it yourself:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  -v /tmp:/tmp:rw \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x
```

The Docker options in the `docker run` command that are used in the above example are:
* `-d`: Start the Docker container in [detached mode](https://docs.docker.com/engine/reference/run/#detached--d).
* `-p 5432:5432`: Map local port 5432 to port 5432 on the container. This will forward traffic to port
  5432 on localhost to port 5432 in the container where PGAdapter is running.
* `-v`: Map the local file `/path/to/credentials.json` to the virtual file `/path/to/credentials.json` in the container.
  The `:ro` suffix indicates that the file should be read-only, preventing the container from ever modifying the file.
  The local file should contain the credentials that should be used by PGAdapter.
* `-e`: Copy the value of the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the container.
  This will make the virtual file `/path/to/credentials.json` the default credentials in the container.
* `-v /tmp:/tmp:rw`: Map the `/tmp` host directory to the `/tmp` directory in the container. PGAdapter by
  default uses `/tmp` as the directory where it creates a Unix Domain Socket.

The PGAdapter options in the `docker run` command that are used in the above example are:
* `-p`: The Google Cloud project name where the Cloud Spanner database is located.
* `-i`: The name of the Cloud Spanner instance where the databsae is located.
* `-d`: The name of the Cloud Spanner database that PGAdapter should connect to.
* `-x`: Allow PGAdapter to accept connections from other hosts than localhost. This is required as
  PGAdapter is running in a Docker container. This means that connections from the host machine will
  not be seen as coming from localhost in PGAdapter.

## Running on a different port

If you already have PostgreSQL running on your local system on port 5432, you need to assign a
different host port to forward to the Docker container:

```shell
docker run \
  -d -p 5433:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -x
psql -h localhost -p 5433
```

The above example starts PGAdapter in a Docker container on the default port 5432. That port is
mapped to port 5433 on the host machine. When connecting to PGAdapter in the Docker container, you
must specify port 5433 for the connection.

## Using a different directory for Unix Domain Sockets

PGAdapter by default uses `/tmp` as the directory for Unix Domain Sockets. You can change this with
the `-dir` command line argument for PGAdapter. You must then also change the volume mapping to your
host to ensure that you can connect to the Unix Domain Socket.

```shell
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  -v /var/pgadapter:/var/pgadapter:rw \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance -d my-database \
  -dir /var/pgadapter \
  -x
psql -h /var/pgadapter
```

The above example uses `/var/pgadapter` as the directory for Unix Domain Sockets.
