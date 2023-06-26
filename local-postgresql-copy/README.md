# Local PostgreSQL Copy

This folder contains a `docker compose` file and a set of scripts that automatically create
a local PostgreSQL database in a Docker container with the same schema as a Cloud Spanner database.

The folder also contains these scripts:

| Script                     | Description                                                                                                                                                                                                                                                                                                                                                                  |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `copy-schema.sh`           | Copies the schema of a Cloud Spanner database to a PostgreSQL database. This script can be used to create a copy of a Cloud Spanner database.                                                                                                                                                                                                                                |
| `copy-data.sh`             | Copies all data from a Cloud Spanner database to a PostgreSQL database. This script can be used to create a copy of a Cloud Spanner database.                                                                                                                                                                                                                                |
| `import-foreign-schema.sh` | Imports the schema of a Cloud Spanner database as a foreign schema in a PostgreSQL database. This can be used to query Cloud Spanner database directly in a PostgreSQL database. This allows the use of specific PostgreSQL features with Cloud Spanner data, such as user-defined functions, stored procedures or system functions that are not available in Cloud Spanner. |
| `run-pg-dump.sh`           | Copies all data from the Cloud Spanner database to the local PostgreSQL database, exports the schema of the Cloud Spanner database to a `schema.sql` file, and runs `pg_dump` on the local PostgreSQL database. This can be used to reconstruct the Cloud Spanner database at a later moment.                                                                                |
| `run-pg-restore.sh`        | Runs `pg_restore` on the local PostgreSQL database using a previously generated dump, executes the `schema.sql` file on the Cloud Spanner database to re-create the schema, and copies all data from the local PostgreSQL database to Cloud Spanner.                                                                                                                         |

These scripts can also be used to copy a Cloud Spanner database to a PostgreSQL database running on
an existing server or to a CloudSQL instance.

## Try it out

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

Also ensure that the `PGADAPTER_HOST_PORT` (default 9001) and `POSTGRES_HOST_PORT` (default 9002)
are not in use by any other services on your local machine, and change the values if necessary.

2. Start the Docker containers.

```shell
docker compose --env-file .env up -d
```

3. Start an interactive `psql` shell on the local PostgreSQL database. The command below assumes
   that the `POSTGRES_CONTAINER_NAME` was left unmodified with its default value `postgres` in the `.env` file.

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
3. An additional `_information_schema` schema that imports all tables that are present in the Cloud
   Spanner `information_schema` as `FOREIGN TABLE`s.


## Copy Data

This folder also contains a script that can be used to copy all data from Cloud Spanner into the
tables that were created in the local PostgreSQL database. __This script will delete all data that is
already present in the local PostgreSQL database__.

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

## pg_dump and pg_restore

The folder contains scripts that allow you to create a `pg_dump` of your Cloud Spanner database, and
to restore this into the same or a different Cloud Spanner database. These scripts can also be used
to copy your Cloud Spanner database to any other PostgreSQL-compatible database system.

You can use the scripts as-is to create a `pg_dump` of your Cloud Spanner PostgreSQL database, and to
restore it again, for example on a different Cloud Spanner instance. The scripts can also be modified
to better fit your specific situation. The scripts only use standard PostgreSQL tools and standard
features in PGAdapter.

### pg_dump

The `run-pg-dump.sh` script can be used to create a dump of your Cloud Spanner database. This script will
execute the following steps:
1. Copy all data from your Cloud Spanner database to the local PostgreSQL database.
2. Generate the DDL statements that are needed to re-create your Cloud Spanner database
   and save these to the file `./backup/schema.sql` in the `postgres` Docker container.
3. Execute `pg_dump` on the local PostgreSQL database and store the data in `directory`
   format in the `./backup/data` folder on the `postgres` Docker container.
   See https://www.postgresql.org/docs/current/app-pgdump.html for more information on `pg_dump`.

The output from this script can be used to re-create the Cloud Spanner database, or to copy the
Cloud Spanner database to another PostgreSQL-compatible database system.

The script must be executed using the `postgres` Docker container:

```shell
docker exec -it postgres /run-pg-dump.sh
```

The backup data can be found in 

### pg_restore

The `run-pg-restore.sh` script can be used to restore a previously dumped Cloud Spanner
database. The script is intended for restoring databases that have been dumped using the
`run-pg-dump.sh` script in this folder. It can also be used to restore any other valid
`pg_dump`, but it requires an accompanying `schema.sql` script that can be used to
create the schema of the Cloud Spanner database.

The `run-pg-restore.sh` script executes the following steps:
1. Run `pg_restore` on the local PostgreSQL database. This will replace the existing
   local PostgreSQL database. The `pg_dump` files are expected to be in the folder
   `./backup/data` in the `postgres` Docker container.
2. Create the schema of the Cloud Spanner database by running the SQL statements in the
   file `./backup/schema.sql` in the `postgres` container.
3. (Temporarily) drop all foreign key constraints in the Cloud Spanner database.
4. Copy all data from the local PostgreSQL database that was restored to the Cloud Spanner
   database.
5. Re-create all the foreign key constraints in the Cloud Spanner database.

### Example

Follow these steps to try a full `pg_dump` and `pg_restore` cycle:

1. Modify the `.env` file in this directory, so it points to an existing Cloud Spanner database.
2. Run `docker compose --env-file .env up -d` to start the Docker containers.
3. Run `docker exec -it postgres /run-pg-dump.sh`. This will copy all data from your Cloud Spanner
   database to the local PostgreSQL database, and then run `pg_dump` on the local database.
4. Stop the Docker containers by executing `docker compose down`.
5. Create a new Cloud Spanner PostgreSQL database with the command
   `gcloud spanner databases create new-database --instance=my-instance --database-dialect=POSTGRESQL`
6. Modify the `SPANNER_DATABASE` variable in the `.env` file so it references the new database you just created.
7. Bring the containers back up with `docker compose --env-file .env up -d`.
8. Run `docker exec -it postgres /run-pg-restore.sh`. This will restore the Cloud Spanner database
   that was dumped into the new Cloud Spanner database. This is done by first running `pg_restore` on
   the local PostgreSQL database, and then copying the data to the Cloud Spanner database.

__TIP__: You can also run the restore script to a new database without bringing down the Docker containers
and modifying the `.env` file by setting the Cloud Spanner database name in the `spanner_restore_database`
variable like this:

```shell
docker exec -it postgres /bin/bash -c "spanner_restore_database=new-database /run-pg-restore.sh"
```

## CloudSQL / Other PostgreSQL Databases

The scripts in this directory can also be used to copy Cloud Spanner databases to/from CloudSQL
PostgreSQL databases or any other PostgreSQL database. For this, you do not need to start the
Docker containers in this directory with `docker compose`. Instead, the examples in this section
assumes that you have PostgreSQL client tools installed on your local host.

Follow these steps to copy a Cloud Spanner database to a CloudSQL PostgreSQL database:

1. Set these environment variables to point to your CloudSQL database and Cloud Spanner database.

```shell
export PGADAPTER_HOST=localhost
export PGADAPTER_PORT=9001

export GOOGLE_APPLICATION_CREDENTIALS=/local/path/to/credentials.json
export GOOGLE_CLOUD_PROJECT=my-project
export SPANNER_INSTANCE=my-instance
export SPANNER_DATABASE=my-database

export POSTGRES_HOST=/cloudsql/my-project:my-cloudsql-zone:my-cloudsql-instance
export POSTGRES_PORT=5432

export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret
export POSTGRES_DB=my-database
```

2. Start PGAdapter.

```shell
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  --rm -d -p 9001:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p ${GOOGLE_CLOUD_PROJECT} -i ${SPANNER_INSTANCE} \
  -c /credentials.json \
  -x
```

3. Copy the schema and data of the Cloud Spanner database to CloudSQL.

```shell
./copy-schema.sh
./copy-data.sh
```

__NOTE__: The PostgreSQL user must have the `REPLICATION` privilege to run this script. If you get an
error saying `permission denied to set parameter "session_replication_role"`, you need to run the
following command to grant the privilege:

```
PGPASSWORD=$POSTGRES_PASSWORD psql \
  -U $POSTGRES_USER \
  -h $POSTGRES_HOST \
  -d $POSTGRES_DB \
  -c "alter role $POSTGRES_USER with replication"
```

### pg_dump

You can also run `pg_dump` via any PostgreSQL database. This will first copy the schema and all data
from your Cloud Spanner database to the PostgreSQL database that you have specified in the environment
variables, and then run `pg_dump` on the PostgreSQL database. It will also export a `schema.sql` file
for your Cloud Spanner database. The combination of the `pg_dump` files and the `schema.sql` file can
be used to re-create the Cloud Spanner database.

This will create the `pg_dump` files and `schema.sql` file in the `./backup` folder:

```shell
./run-pg-dump.sh
```


### pg_restore

__NOTE__: The `run-pg-restore.sh` script is not supported with CloudSQL and other hosted
PostgreSQL solutions, as the script adds PGAdapter as a foreign server. CloudSQL cannot
access PGAdapter as a foreign server. Instead, you should use a local PostgreSQL server
to execute the `run-pg-restore.sh` script.

You can also run `pg_restore` via any PostgreSQL database that __runs on the same network
as PGAdapter__. This will first restore the dump into the PostgreSQL database, and then copy
the data from the PostgreSQL database into the Cloud Spanner database. The schema in the
Cloud Spanner database is created from the`./backup/schema.sql` file. This ensures that
all Cloud Spanner specific extensions to the PostgreSQL dialect are preserved, such as 
`INTERLEAVE IN` clauses.

This will restore the dump in the `./backup/data` directory to the PostgreSQL database,
create the schema in the Cloud Spanner database named `new-database`, and copy all data
from the PostgreSQL database to Cloud Spanner:

```shell
gcloud spanner databases create new-database \
  --instance="$SPANNER_INSTANCE" \
  --database-dialect=POSTGRESQL
spanner_restore_database="new-database" ./run-pg-restore.sh
```

## Troubleshooting

### Port Conflicts

If the `docker compose --env-file .env up -d` command seems to hang while starting the containers,
it is possible that you have a port mapping conflict. The default port mapping is to map
PGAdapter to port `9001` on the local machine and  PostgreSQL to port `9002`. Change these defaults
in the `.env` file if you already have other services running on these port numbers.
