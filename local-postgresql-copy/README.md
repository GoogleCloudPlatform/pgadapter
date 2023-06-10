# Local PostgreSQL Copy

This folder contains a `docker compose` file and a set of scripts that automatically create
a local PostgreSQL database in a Docker container with the same schema as a Cloud Spanner database.

Follow these steps to try it out:

1. Edit the `.env` file in this directory to match your Cloud Spanner setup. These variables must be
   set to the correct values:

```
# The full path of the Google Cloud credentials file that should be used for PGAdapter.
GOOGLE_APPLICATION_CREDENTIALS=/local/path/to/credentials.json

# Project, instance and database for Cloud Spanner.
GOOGLE_CLOUD_PROJECT=my-project
SPANNER_INSTANCE=my-instance
SPANNER_DATABASE=my-database
```

2. Start the Docker containers.

```shell
docker compose --env-file .env up -d
```

3. Start an interactive `psql` shell on the local PostgreSQL database. The command below assumes
   that the `POSTGRES_CONTAINER_NAME` was kept at its default value `postgres` in the `.env` file.

```shell
docker exec -it postgres /bin/bash -c 'psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"'
```

You can now look around in the local database. The default `public` schema contains a copy of all
tables from the Cloud Spanner database, but without any data. See [Copy Data](#copy-data) for more
information on how to copy data from Cloud Spanner to the local PostgreSQL database.

The following command will show all the tables in the current schema:

```shell
\dt
```

Now switch to the schema `_public_read_only`. This schema contains a `FOREIGN TABLE` definition for
each Cloud Spanner table. These tables are read-only and can be used to query data on Cloud Spanner:

```shell
set search_path to _public_read_only;
select * from my_table limit 10;
```


Now switch to the schema `_public_read_write`. This schema also contains a `FOREIGN TABLE` definition
for each Cloud Spanner table. These tables can be used for both reading and writing to Cloud Spanner.

```shell
set search_path to _public_read_write;
insert into my_table (id, value) values (1, 'One');
```

Use the `\q` command to exit `psql`.

```shell
\q
```

## Schema

The local copy will by default only copy the schema from the Cloud Spanner database and not any data.
The local copy will include `FOREIGN TABLE` definitions for all tables in the Cloud Spanner database.

The local PostgreSQL database will by default contain:
1. The exact same schema as the Cloud Spanner database. The only exceptions are Cloud Spanner
   schema extensions to open-source PostgreSQL, such as interleaved tables. These tables will be
   present in the local PostgreSQL database, but without an interleave relationship.
2. Two additional schemas that both import all tables that are present in the Cloud Spanner database
   as `FOREIGN TABLE`s. These tables can be used to query the Cloud Spanner database through the
   local PostgreSQL database. The two schemas are named:
   1. `_public_read_only`: This schema uses a `FOREIGN SERVER` definition that will always use
     read-only transactions. Use this schema for read-only operations on the Cloud Spanner data.
   2. `_public_read_write`: This schema uses a `FOREIGN SERVER` definition that always uses read/write
     transactions. This schema can be used to write data to Cloud Spanner.


## Copy Data

This folder also contains a script that can be used to copy all data from Cloud Spanner into the
tables that were created in the local PostgreSQL database. __This script will delete all data that is
already present in the local tables__.

__NOTE: Copying a large Cloud Spanner database to a local PostgreSQL database could take a long time.__
Copying a small test/development database with some millions of rows should be relatively quick.

Execute the following command to copy all data from Cloud Spanner to the local PostgreSQL database:

```shell
docker exec postgres /copy-data.sh
```

## Connecting to the containers

The `docker compose` file starts two containers:
1. `pgadapter`: This container by default exposes its PostgreSQL port on local port 9001.
2. `postgres`: This container by default exposes its PostgreSQL port on local port 9002.

You can change these ports in the `.env` file if they clash with anything you have running on your
local machine. You can also remove the port mapping in the `docker-compose.yml` file if you do not
need to access the containers from your local machine.

Connect to PGAdapter with `psql` with this command on your local machine:

```shell
psql -h localhost -p 9001 -d my-database
```

Connect to the local PostgreSQL database with `psql` with this command on your local machine:

```shell
PGPASSWORD=secret psql -h localhost -p 9002 -d my-database -U postgres
```
