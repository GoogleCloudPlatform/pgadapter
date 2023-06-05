# Importing Data from a MySQL Database using COPY

You can import (test) data from a MySQL database using the `COPY` command in PGAdapter by importing
the MySQL schema as a `FOREIGN SCHEMA` in a PostgreSQL database, and then use the `COPY` command to
copy data from the MySQL database to a Cloud Spanner database. Note that the PostgreSQL database
that you need for this can be a transient database that is discarded after the import has finished.

This document takes you through the steps that are needed to import a (test) schema from a MySQL
database. For this, we will use a sample MySQL database. The steps needed for this are:

1. Create a sample MySQL database (skip this step if you have an existing MySQL database)
2. Import the MySQL database as a `FOREIGN SCHEMA` in a PostgreSQL database. Note that this does
   not import any data into the PostgreSQL database, it only registers the tables in the MySQL
   database as `FOREIGN TABLE`s in the PostgreSQL database.
3. Create a DDL script from the foreign tables that can be used to create the tables in the
   Cloud Spanner database.
4. Execute `COPY` statements to copy data from MySQL to Cloud Spanner.

## Create a MySQL Sample Database

Create a MySQL sample database using the [standard Employees database](https://dev.mysql.com/doc/employee/en/employees-introduction.html).
The script does the following:
1. Clones the standard Employees MySQL sample database into the folder 'employees'.
2. Creates a network that we will use for the different Docker containers.
3. Starts MySQL in a Docker container.
4. Imports the sample database into the MySQL instance.

```shell
git clone git@github.com:datacharmer/test_db.git employees
docker network create -d bridge mysql-network
docker run --name employees-mysql \
  --network mysql-network \
  -e MYSQL_ROOT_PASSWORD=my-secret-pw \
  -v `pwd`/employees:/employees:ro \
  -d mysql
docker exec -it -w /employees employees-mysql \
  /bin/sh -c 'mysql -hemployees-mysql -uroot -pmy-secret-pw < employees.sql'
```

## Start PGAdapter and PostgreSQL

Start both PGAdapter and a real PostgreSQL instance using Docker.

First set some environment variables that tells the scripts where to find the Cloud Spanner database.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export GOOGLE_CLOUD_PROJECT=my-project
export SPANNER_INSTANCE=my-instance
export SPANNER_DATABASE=my-database
```

Create the Cloud Spanner database if it does not yet exist.

```shell
gcloud config set project ${GOOGLE_CLOUD_PROJECT}
gcloud spanner databases create ${SPANNER_DATABASE} --instance ${SPANNER_INSTANCE} \
  --database-dialect POSTGRESQL
```

Create a Docker Compose file that will start both PGAdapter and PostgreSQL in the same Docker network.

```shell
cat <<EOT >> docker-compose.yml
version: "3.9"
networks:
  mysql-network:
    name: mysql-network
    external: true
services:
  pgadapter:
    image: "gcr.io/cloud-spanner-pg-adapter/pgadapter"
    networks:
      - mysql-network
    pull_policy: always
    container_name: pgadapter
    volumes:
      - "${GOOGLE_APPLICATION_CREDENTIALS}:/credentials.json:ro"
    command:
      - "-p ${GOOGLE_CLOUD_PROJECT}"
      - "-i ${SPANNER_INSTANCE}"
      - "-d ${SPANNER_DATABASE}"
      - "-c /credentials.json"
      - "-x"
  postgres:
    image: "postgres"
    networks:
      - mysql-network
    container_name: postgres
    environment:
      POSTGRES_PASSWORD: mysecret
EOT
```

Start PGAdapter and PostgreSQL and then import the MySQL database as a foreign schema in the
PostgreSQL database.

Note that the example database that we use contains a user-defined type that must be created
manually in the PostgreSQL database before we can import the MySQL schema.

```shell
docker compose up -d
docker exec -it postgres /bin/sh -c 'apt-get update'
docker exec -it postgres /bin/sh -c 'apt-get install -y postgresql-15-mysql-fdw'
docker exec -it --env PGPASSWORD=mysecret postgres psql -h postgres -U postgres -c 'CREATE EXTENSION mysql_fdw;'
docker exec -it --env PGPASSWORD=mysecret postgres psql -h postgres -U postgres -c "CREATE SERVER mysql_server FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host 'employees-mysql', port '3306');"
docker exec -it --env PGPASSWORD=mysecret postgres psql -h postgres -U postgres -c "CREATE USER MAPPING FOR postgres SERVER mysql_server OPTIONS (username 'root', password 'my-secret-pw');"
docker exec -it --env PGPASSWORD=mysecret postgres psql -h postgres -U postgres -c "DO \$\$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typname = 'employees_gender_t') THEN CREATE TYPE employees_gender_t AS enum('M','F'); END IF; END\$\$;"
docker exec -it --env PGPASSWORD=mysecret postgres psql -h postgres -U postgres -c 'IMPORT FOREIGN SCHEMA employees FROM SERVER mysql_server INTO public;'
docker exec -it --env PGPASSWORD=mysecret postgres psql -h postgres -U postgres -c 'select * from employees limit 1;'
```

The last command should return one record from the employees table and shows that the MySQL schema
has been successfully imported into the PostgreSQL database.

We now need to create the actual tables in the Cloud Spanner database. For this, the original table
definitions for MySQL have to be modified slightly to be compatible with Cloud Spanner's PostgreSQL
dialect:

```shell
docker exec -it --env PGPASSWORD=mysecret postgres pg_dump -h postgres -U postgres \
  --schema 'public' \
  --no-owner \
  --no-privileges \
  --no-comments \
  --schema-only \
  postgres \
  > create_tables.sql
```

The (manually created) `CREATE TABLE` statements that we will use for this specific example database
can be executed on PGAdapter using the following command:

```

```

```shell
docker container stop employees-mysql
docker container rm employees-mysql
docker network rm mysql-network
```

