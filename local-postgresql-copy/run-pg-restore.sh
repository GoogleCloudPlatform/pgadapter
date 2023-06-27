#!/bin/bash
set -e

# This script runs pg_restore to restore a PostgreSQL database and then copies the restored data
# to a Cloud Spanner database. This is done using the following steps:
# 1. Run pg_restore using the data in ./backup/data to restore the database to a PostgreSQL server.
# 2. (Re-)create the schema of a Cloud Spanner database by running the ./backup/schema.sql file on
#    Cloud Spanner.
# 3. Copy all data from the PostgreSQL database to Cloud Spanner
#
# The destination Cloud Spanner database must exist and be empty.

if [ -z "$spanner_restore_database" ]; then spanner_restore_database="$SPANNER_DATABASE"; fi;
if [ -z "$schema_file" ]; then schema_file="./backup/schema.sql"; fi;
if [ -z "$data_dir" ]; then data_dir="./backup/data"; fi;

echo "RESTORING CLOUD SPANNER DATABASE"
echo "Restoring to Cloud Spanner database $spanner_restore_database. This database must be empty."
echo "Restoring schema file $schema_file."
echo "Restoring data from $data_dir."
echo "--- WARNING: pg_restore is executed with the options --clean --create. ---"
echo "--- ANY EXISTING DATA IN THE PostgreSQL DATABASE WILL BE LOST! ---"

echo
read -p "Continue? [Yn]" -n 1 -r
echo
if [[ $REPLY =~ [Nn]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Check whether the Cloud Spanner database is empty. That is; no user-tables.
table_count=$(psql -v ON_ERROR_STOP=1 -h "$PGADAPTER_HOST" -p "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -qAtX \
  -c "select count(1) from information_schema.tables where table_schema in (select schema_name from information_schema.schemata where not schema_owner='spanner_system')")
if [[ ! $table_count = "0" ]]
then
  echo "Number of tables found in the restore database: $table_count"
  echo "The destination Cloud Spanner database is not empty."
  echo "This script only supports restoring into an empty database."
  [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Restore the database. --clean and --create means 'drop-and-recreate' if the database exists.
# We 'connect' to the postgres database here, because we need to connect to a database in order
# to drop another database.
# Note that we do a `|| true` to ensure that the script continues even if pg_restore returns
# a non-zero exit code. That is because pg_restore will often return some warnings, and that
# again will cause it to return a non-zero exit code.
PGPASSWORD="$POSTGRES_PASSWORD" pg_restore -v \
  -U "$POSTGRES_USER" \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -d postgres \
  --create --clean \
  --format=directory "$data_dir" || true

# Create the schema on the Cloud Spanner database. We run the script between
# START BATCH DDL and RUN BATCH to run the entire script as one batch.
echo "Creating schema on Cloud Spanner database $spanner_restore_database"
psql -v ON_ERROR_STOP=1 -a \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -c "start batch ddl" \
  -f "$schema_file" \
  -c "run batch"

# Re-import the Cloud Spanner database as foreign schemas in the PostgreSQL database.
# This will allow us to query the Cloud Spanner information_schema using the PostgreSQL query engine,
# which again allows us to execute a recursive query on the information_schema.tables table in Cloud Spanner.
echo "Importing the Cloud Spanner schema as a foreign schema in the PostgreSQL database"
PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -h "$POSTGRES_HOST" \
  -p "$POSGRES_PORT" \
  -d "$POSTGRES_DB" \
  -c "drop extension if exists postgres_fdw cascade;"
temp_spanner_db="$SPANNER_DATABASE"
SPANNER_DATABASE="$spanner_restore_database"
./import-foreign-schema.sh
SPANNER_DATABASE="$temp_spanner_db"

# Generate a script to drop all foreign keys on Cloud Spanner.
psql -v ON_ERROR_STOP=1 \
  -h "$PGADAPTER_HOST" \
  -p "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -qAtX \
  -c "select 'alter table \"' || tc.table_schema || '\".\"' || tc.table_name || '\"'
                 || ' drop constraint \"' || fk.constraint_name || '\";'
      from information_schema.referential_constraints fk
      inner join information_schema.table_constraints tc
          on  fk.constraint_catalog=tc.constraint_catalog
          and fk.constraint_schema=tc.constraint_schema
          and fk.constraint_name=tc.constraint_name;" > drop_foreign_keys.sql

# Generate a script that can be used to re-create all foreign key constraints
# on the database.
psql -v ON_ERROR_STOP=1 \
  -h "$PGADAPTER_HOST" \
  -p "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -qAtX \
  -c "with constraint_columns as (
          select constraint_catalog, constraint_schema, constraint_name,
                 array_to_string(array_agg('\"' || kcu.column_name || '\"'), ',') as column_names
          from (
                   select constraint_catalog, constraint_schema, constraint_name, column_name
                   from information_schema.key_column_usage
                   order by ordinal_position
               ) kcu
          group by constraint_catalog, constraint_schema, constraint_name
      )
      select 'alter table \"' || tc.table_schema || '\".\"' || tc.table_name || '\"'
                 || ' add constraint \"' || fk.constraint_name
                 || '\" foreign key (' || fk_cc.column_names || ') references '
                 || '\"' || pk_tc.table_schema || '\".\"' || pk_tc.table_name || '\"'
                 || ' (' || pk_cc.column_names || ');'
      from information_schema.referential_constraints fk
      inner join information_schema.table_constraints tc
          on  fk.constraint_catalog=tc.constraint_catalog
          and fk.constraint_schema=tc.constraint_schema
          and fk.constraint_name=tc.constraint_name
      inner join constraint_columns fk_cc
          on  fk.constraint_catalog=fk_cc.constraint_catalog
          and fk.constraint_schema=fk_cc.constraint_schema
          and fk.constraint_name=fk_cc.constraint_name
      inner join constraint_columns pk_cc
          on  fk.unique_constraint_catalog=pk_cc.constraint_catalog
          and fk.unique_constraint_schema=pk_cc.constraint_schema
          and fk.unique_constraint_name=pk_cc.constraint_name
      inner join information_schema.table_constraints pk_tc
          on  fk.unique_constraint_catalog=pk_tc.constraint_catalog
          and fk.unique_constraint_schema=pk_tc.constraint_schema
          and fk.unique_constraint_name=pk_tc.constraint_name;" > create_foreign_keys.sql

# Drop all foreign keys in the Cloud Spanner database.
echo "Dropping all foreign keys in the Cloud Spanner database before importing data"
psql -v ON_ERROR_STOP=1 \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -c "start batch ddl" \
  -f "drop_foreign_keys.sql" \
  -c "run batch"

# Copy data from the local PostgreSQL database to Cloud Spanner.
# We need to copy the data in hierarchical order:
# 1. First all tables without a parent.
# 2. Then all tables at hierarchical level 1, etc.
# So we'll do a recursive query on Cloud Spanner using the foreign schema that we imported above.
# Note the use of _information_schema in the query. This is the imported foreign schema from
# Cloud Spanner.
PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -d "$POSTGRES_DB" -qAtX \
  -c "with recursive table_hierarchy as (
          select table_catalog, table_schema, table_name, table_type, parent_table_name, 1 as depth
          from _information_schema.tables
          where parent_table_name is null
          union all
          select t.table_catalog, t.table_schema, t.table_name, t.table_type, t.parent_table_name, h.depth + 1 as depth
          from _information_schema.tables t
          inner join table_hierarchy h
              on  h.table_catalog=t.table_catalog
              and h.table_schema=t.table_schema
              and h.table_name=t.parent_table_name
      )
      select table_name
      from table_hierarchy
      where table_schema='public'
      and table_type='BASE TABLE';" \
| while read table_name ; do
  column_names=$(PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -U "$POSTGRES_USER" \
    -h "$POSTGRES_HOST" \
    -p "$POSTGRES_PORT" \
    -d "$POSTGRES_DB" \
    -qAtX \
    -c "select array_to_string(array_agg(column_name), ',') as c from _information_schema.columns where table_name='$table_name' and table_schema='public' and not is_generated='ALWAYS';")

  echo ""
  echo "Copying data for $table_name"
  PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 \
       -U "$POSTGRES_USER" \
       -h "$POSTGRES_HOST" \
       -p "$POSTGRES_PORT" \
       -d "$POSTGRES_DB" \
       -c "copy $table_name ($column_names) to stdout binary" \
  | PGOPTIONS="-c SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'"\
      psql -v ON_ERROR_STOP=1 -a \
         -h "$PGADAPTER_HOST" \
         -p "$PGADAPTER_PORT" \
         -d "$spanner_restore_database" \
         -c "copy $table_name ($column_names) from stdin binary"
done

# Re-create all foreign keys in the Cloud Spanner database.
echo "Re-creating all foreign keys in the Cloud Spanner database"
psql -v ON_ERROR_STOP=1 -a \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -c "start batch ddl" \
  -f "create_foreign_keys.sql" \
  -c "run batch"
