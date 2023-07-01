# Importing Data from a MySQL Database using COPY

You can import (test) data from a MySQL database using the `COPY` command in PGAdapter by importing
the MySQL schema as a `FOREIGN SCHEMA` in a PostgreSQL database, and then use the `COPY` command to
copy data from the MySQL database to a Cloud Spanner database. Note that the PostgreSQL database
that you need for this can be a temporary database, for example in form of a simple Docker container,
that is discarded after the import has finished.

This document takes you through the steps that are needed to import a (test) schema from a MySQL
database. For this, we will use a sample MySQL database. The steps needed for this are:

1. Create a sample MySQL database (skip this step if you have an existing MySQL database)
2. Import the MySQL database as a `FOREIGN SCHEMA` in a PostgreSQL database. Note that this does
   not import any data into the PostgreSQL database, it only links the tables in the MySQL
   database as `FOREIGN TABLE`s in the PostgreSQL database.
3. Create a DDL script from the foreign tables that can be used to create the tables in the
   Cloud Spanner database.
4. Execute `COPY` statements to copy data from MySQL to Cloud Spanner.

## Create a MySQL Sample Database

This step creates a sample MySQL database that will be imported into Cloud Spanner. Skip this step
if you have an existing MySQL database that you want to import into Cloud Spanner.

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
  /bin/sh -c 'mysql -uroot -pmy-secret-pw < employees.sql'
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

Create a Docker Compose file that will start both PGAdapter and PostgreSQL in the same Docker network
as the MySQL instance that we created above.

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
    container_name: pgadapter-mysql
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
    container_name: postgres-mysql
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
docker exec -it postgres-mysql /bin/sh -c 'apt-get update'
docker exec -it postgres-mysql /bin/sh -c 'apt-get install -y postgresql-15-mysql-fdw'
docker exec -it --env PGPASSWORD=mysecret postgres-mysql psql -U postgres -c 'CREATE EXTENSION mysql_fdw;'
docker exec -it --env PGPASSWORD=mysecret postgres-mysql psql -U postgres -c "CREATE SERVER mysql_server FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host 'employees-mysql', port '3306');"
docker exec -it --env PGPASSWORD=mysecret postgres-mysql psql -U postgres -c "CREATE USER MAPPING FOR postgres SERVER mysql_server OPTIONS (username 'root', password 'my-secret-pw');"
docker exec -it --env PGPASSWORD=mysecret postgres-mysql psql -U postgres -c "DO \$\$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typname = 'employees_gender_t') THEN CREATE TYPE employees_gender_t AS enum('M','F'); END IF; END\$\$;"
docker exec -it --env PGPASSWORD=mysecret postgres-mysql psql -U postgres -c 'IMPORT FOREIGN SCHEMA employees FROM SERVER mysql_server INTO public;'
docker exec -it --env PGPASSWORD=mysecret postgres-mysql psql -U postgres -c 'select * from employees limit 1;'
```

The last command should return one record from the employees table and shows that the MySQL schema
has been successfully imported into the PostgreSQL database.

We now need to create the actual tables in the Cloud Spanner database. For this, the original table
definitions for MySQL have to be modified slightly to be compatible with Cloud Spanner's PostgreSQL
dialect:

```shell
docker exec -it postgres-mysql psql -h pgadapter-mysql -c " \
   CREATE TABLE employees (
       emp_no      BIGINT          NOT NULL,
       birth_date  DATE            NOT NULL,
       first_name  VARCHAR(14)     NOT NULL,
       last_name   VARCHAR(16)     NOT NULL,
       gender      VARCHAR(1)      NOT NULL,    
       hire_date   DATE            NOT NULL,
       PRIMARY KEY (emp_no)
   );
   
   CREATE TABLE departments (
       dept_no     VARCHAR(4)      NOT NULL,
       dept_name   VARCHAR(40)     NOT NULL,
       PRIMARY KEY (dept_no)
   );
   
   CREATE UNIQUE INDEX idx_departments_dept_name on departments (dept_name);
   
   CREATE TABLE dept_manager (
      emp_no       BIGINT          NOT NULL,
      dept_no      VARCHAR(4)      NOT NULL,
      from_date    DATE            NOT NULL,
      to_date      DATE            NOT NULL,
      FOREIGN KEY (emp_no)  REFERENCES employees (emp_no),
      FOREIGN KEY (dept_no) REFERENCES departments (dept_no),
      PRIMARY KEY (emp_no,dept_no)
   ); 
   
   CREATE TABLE dept_emp (
       emp_no      BIGINT          NOT NULL,
       dept_no     VARCHAR(4)      NOT NULL,
       from_date   DATE            NOT NULL,
       to_date     DATE            NOT NULL,
       FOREIGN KEY (emp_no)  REFERENCES employees   (emp_no),
       FOREIGN KEY (dept_no) REFERENCES departments (dept_no),
       PRIMARY KEY (emp_no,dept_no)
   );
   
   CREATE TABLE titles (
       emp_no      BIGINT          NOT NULL,
       title       VARCHAR(50)     NOT NULL,
       from_date   DATE            NOT NULL,
       to_date     DATE,
       FOREIGN KEY (emp_no) REFERENCES employees (emp_no),
       PRIMARY KEY (emp_no,title, from_date)
   ) 
   ; 
   
   CREATE TABLE salaries (
       emp_no      BIGINT          NOT NULL,
       salary      BIGINT          NOT NULL,
       from_date   DATE            NOT NULL,
       to_date     DATE            NOT NULL,
       FOREIGN KEY (emp_no) REFERENCES employees (emp_no),
       PRIMARY KEY (emp_no, from_date)
   ) 
   ;"
```

## Copy Data From MySQL to Cloud Spanner

We can now use the standard PostgreSQL `COPY` command to copy data from MySQL to Cloud Spanner. This
example uses the PostgreSQL `BINARY` copy protocol, as it ensures that the data is sent in a format
that cannot cause any data loss between PostgreSQL and Cloud Spanner. This however requires that we
make sure that the data that is copied has the correct type. This is achieved by:
1. Executing a `SELECT` on the source tables to get the specific columns that we want to copy.
2. Cast each column to the specific data type that we use on Cloud Spanner. This also includes
   casting for example `INT` values to `BIGINT`, as the binary format of these types differ.

The output of the `COPY ... TO STDOUT BINARY` command is piped into a `COPY ... FROM STDIN BINARY`
command on PGAdapter. Note that we execute the command
`set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'` on PGAdapter. This instructs PGAdapter to
use a non-atomic `COPY` to Cloud Spanner in order to ensure that the transaction mutation limit of
is not exceeded.

```shell
docker exec -it --env PGPASSWORD=mysecret postgres-mysql /bin/bash \
  -c "psql -U postgres \
           -c 'COPY (select emp_no::bigint,
                            birth_date::date,
                            first_name::varchar,
                            last_name::varchar,
                            gender::varchar,
                            hire_date::date
                     from employees) TO STDOUT BINARY' \
  | psql -h pgadapter-mysql \
         -c \"set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'\" \
         -c 'COPY employees FROM STDIN BINARY'"

docker exec -it --env PGPASSWORD=mysecret postgres-mysql /bin/bash \
  -c "psql -U postgres \
           -c 'COPY (select dept_no::varchar,
                            dept_name::varchar
                     from departments) TO STDOUT BINARY' \
  | psql -h pgadapter-mysql \
         -c \"set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'\" \
         -c 'COPY departments FROM STDIN BINARY'"

docker exec -it --env PGPASSWORD=mysecret postgres-mysql /bin/bash \
  -c "psql -U postgres \
           -c 'COPY (select emp_no::bigint,
                            dept_no::varchar,
                            from_date::date,
                            to_date::date
                     from dept_manager) TO STDOUT BINARY' \
  | psql -h pgadapter-mysql \
         -c \"set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'\" \
         -c 'COPY dept_manager FROM STDIN BINARY'"

docker exec -it --env PGPASSWORD=mysecret postgres-mysql /bin/bash \
  -c "psql -U postgres \
           -c 'COPY (select emp_no::bigint,
                            dept_no::varchar,
                            from_date::date,
                            to_date::date
                     from dept_emp) TO STDOUT BINARY' \
  | psql -h pgadapter-mysql \
         -c \"set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'\" \
         -c 'COPY dept_emp FROM STDIN BINARY'"

docker exec -it --env PGPASSWORD=mysecret postgres-mysql /bin/bash \
  -c "psql -U postgres \
           -c 'COPY (select emp_no::bigint,
                            title::varchar,
                            from_date::date,
                            to_date::date
                     from titles) TO STDOUT BINARY' \
  | psql -h pgadapter-mysql \
         -c \"set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'\" \
         -c 'COPY titles FROM STDIN BINARY'"

docker exec -it --env PGPASSWORD=mysecret postgres-mysql /bin/bash \
  -c "psql -U postgres \
           -c 'COPY (select emp_no::bigint,
                            salary::bigint,
                            from_date::date,
                            to_date::date
                     from salaries) TO STDOUT BINARY' \
  | psql -h pgadapter-mysql \
         -c \"set spanner.autocommit_dml_mode='PARTITIONED_NON_ATOMIC'\" \
         -c 'COPY salaries FROM STDIN BINARY'"
```

## Stop the Containers

You can stop all the containers once all data has been copied.

```shell
docker compose down
docker container stop employees-mysql
docker container rm employees-mysql
docker network rm mysql-network
```
