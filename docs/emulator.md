# Google Cloud Spanner PGAdapter - Emulator

## Requirements
Connecting to the Cloud Spanner Emulator is supported with:
1. PGAdapter version 0.26.0 and higher.
2. Cloud Spanner Emulator 1.5.12 and higher.

## Usage

PGAdapter can connect to the Cloud Spanner Emulator. The best way to do this is by following these
steps:
1. Start PGAdapter with the additional command line argument `-r autoConfigEmulator=true`. Adding
   this command line argument will instruct PGAdapter to automatically create the Cloud Spanner
   instance and database that is specified in any connection request. This means that you do not
   need to first create the instance and database before connecting to the emulator. It will also
   instruct PGAdapter to connect to the default emulator host `localhost:9010`. You can override
   this by starting PGAdapter with the `-e` command line argument (see next point).
2. Start PGAdapter with the additional command line argument `-e <host:port>` with the host name and
   port number where the emulator is running. This is only necessary if the emulator is not running
   on `localhost:9010`. This will typically be the case if both PGAdapter and the emulator are both
   running in separate Docker containers.

### Example: Running PGAdapter in a Docker Container

The additional complexity when running PGAdapter in a Docker container is that PGAdapter must have
network access to the Cloud Spanner Emulator. The easiest way to achieve this is by running both in
Docker and connect them both to the same Docker network. This example uses Docker Compose to set up
a Docker network and start both the emulator and PGAdapter.

```shell
cat <<EOT > docker-compose.yml
version: "3.9"
services:
  emulator:
    image: "gcr.io/cloud-spanner-emulator/emulator"
    pull_policy: always
    container_name: spanner-emulator
    ports:
      - "9010:9010"
      - "9020:9020"
  pgadapter:
    depends_on:
      emulator:
        condition: service_started
    image: "gcr.io/cloud-spanner-pg-adapter/pgadapter"
    pull_policy: always
    container_name: pgadapter-connected-to-emulator
    command:
      - "-p test-project"
      - "-i test-instance"
      - "-r autoConfigEmulator=true"
      - "-e emulator:9010"
      - "-c \"\""
      - "-x"
    ports:
      - "5432:5432"
EOT
docker compose up -d
sleep 2
psql -h localhost -p 5432 -d test-database
```

Run `docker compose down` to stop both the emulator and PGAdapter.

### Example: Running PGAdapter as a Java Application

This script does the following:
1. Pulls and starts the Cloud Spanner Emulator Docker image.
2. Downloads the latest version of PGAdapter and unpacks this to the current directory.
3. Starts PGAdapter and instructs it to automatically configure a connection to the emulator. Any
   project, instance, or database that is specified in a connection string to PGAdapter will
   automatically be created on the emulator. PGAdapter by default assumes that the emulator is
   running on `localhost:9010`.
4. Sleep 2 seconds to wait for PGAdapter to start up. Then run `psql` against PGAdapter, which again
   connects to the emulator.

```shell
docker pull gcr.io/cloud-spanner-emulator/emulator
docker run --name emulator -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz \
  && tar -xzvf pgadapter.tar.gz
java -jar pgadapter.jar -p test-project -i test-instance -r autoConfigEmulator=true > /dev/null 2>&1 &
sleep 2
psql -h localhost -p 5432 -d test-database
```

## Using the SPANNER_EMULATOR_HOST environment variable
PGAdapter also respects the `SPANNER_EMULATOR_HOST` environment variable. Setting this on the system
where PGAdapter runs will instruct PGAdapter to connect to the emulator specified in the variable.

Using this option requires that you manually create the instance and database that you want
PGAdapter to connect to before actually making the connection. The `autoConfigEmulator=true` option
described above does this automatically for you.
