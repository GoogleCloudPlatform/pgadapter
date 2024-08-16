#!/bin/bash

# This script imports the schema and data from a local file to a Spanner
# database.

set -e

if [ -z "$spanner_restore_database" ]; then spanner_restore_database="$SPANNER_DATABASE"; fi;
if [ -z "$schema_file" ]; then schema_file="./backup/schema.sql"; fi;
if [ -z "$data_dir" ]; then data_dir="./backup/data"; fi;

echo
echo "--- RESTORING SPANNER DATABASE ---"
echo "Schema file will be loaded from $schema_file."
echo "Data will be loaded from $data_dir."

echo
read -p "Continue? [Yn]" -n 1 -r
echo
if [[ $REPLY =~ [Nn]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Check whether the Spanner database is empty. That is; no user-tables.
table_count=$(psql -v ON_ERROR_STOP=1 -h "$PGADAPTER_HOST" -p "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -qAtX \
  -c "select count(1) from information_schema.tables where table_schema not in ('information_schema', 'spanner_sys', 'pg_catalog')")
if [[ ! $table_count = "0" ]]
then
  echo "Number of tables found in the restore database: $table_count"
  echo "The destination Spanner database is not empty."
  echo "This script only supports restoring into an empty database."
  [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Create the schema on the Spanner database. We run the script between
# START BATCH DDL and RUN BATCH to run the entire script as one batch.
echo "Creating schema on Spanner database $spanner_restore_database"
psql -v ON_ERROR_STOP=1 -a \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -c "start batch ddl" \
  -f "$schema_file" \
  -c "run batch"

# Generate a script to drop all foreign keys on Spanner.
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
  -c "with fk_constraint_columns as (
          select constraint_catalog, constraint_schema, constraint_name,
                 array_to_string(array_agg('\"' || kcu.column_name || '\"'), ',') as column_names
          from (
                   select constraint_catalog, constraint_schema, constraint_name, column_name
                   from information_schema.key_column_usage
                   order by ordinal_position
               ) kcu
          group by constraint_catalog, constraint_schema, constraint_name
      ),
      pk_constraint_columns as (
          select constraint_catalog, constraint_schema, constraint_name,
                 array_to_string(array_agg('\"' || kcu.column_name || '\"'), ',') as column_names
          from (
                   select constraint_catalog, constraint_schema, constraint_name, column_name
                   from information_schema.key_column_usage
                   order by position_in_unique_constraint
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
      inner join fk_constraint_columns fk_cc
          on  fk.constraint_catalog=fk_cc.constraint_catalog
          and fk.constraint_schema=fk_cc.constraint_schema
          and fk.constraint_name=fk_cc.constraint_name
      inner join pk_constraint_columns pk_cc
          on  fk.unique_constraint_catalog=pk_cc.constraint_catalog
          and fk.unique_constraint_schema=pk_cc.constraint_schema
          and fk.unique_constraint_name=pk_cc.constraint_name
      inner join information_schema.table_constraints pk_tc
          on  fk.unique_constraint_catalog=pk_tc.constraint_catalog
          and fk.unique_constraint_schema=pk_tc.constraint_schema
          and fk.unique_constraint_name=pk_tc.constraint_name;" > create_foreign_keys.sql

# Drop all foreign keys in the Spanner database.
echo "Dropping all foreign keys in the Spanner database before importing data"
psql -v ON_ERROR_STOP=1 \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -c "start batch ddl" \
  -f "drop_foreign_keys.sql" \
  -c "run batch"

# We need to copy the data in hierarchical order:
# 1. First all tables without a parent.
# 2. Then all tables at hierarchical level 1, etc.

# Get all tables without a parent.
table_names=$(psql -v ON_ERROR_STOP=1 \
    --host "$PGADAPTER_HOST" \
    --port "$PGADAPTER_PORT" \
    -d "$spanner_restore_database" \
    -qAtX \
    -c "select table_name from information_schema.tables
            where table_schema = 'public'
	    and parent_table_name is NULL
	    and table_type='BASE TABLE'")

while : ; do
  delim=""
  parent_table_str=""
  # Copy data for each table at this level.
  for table_name in $table_names ; do
    sql_file="$data_dir/$table_name.sql"

    echo ""
    echo "Copying data for $table_name"
    psql -v ON_ERROR_STOP=1 \
      --host "$PGADAPTER_HOST" \
      --port "$PGADAPTER_PORT" \
      -d "$spanner_restore_database" \
      < $sql_file

    parent_table_str="$parent_table_str$delim'$table_name'"
    delim=", "
  done

  table_names=""
  if [[ ! $parent_table_str = "" ]] ; then
    # Get the tables that have the current set of tables as a parent.
    table_names=$(psql -v ON_ERROR_STOP=1 \
      --host "$PGADAPTER_HOST" \
      --port "$PGADAPTER_PORT" \
      -d "$spanner_restore_database" \
      -qAtX \
      -c "select table_name from information_schema.tables
              where table_schema = 'public'
        and parent_table_name in ($parent_table_str)
        and table_type='BASE TABLE'")
  fi

  # No further child tables left to copy.
  if [[ $table_names = "" ]] ; then
    break
  fi
done

# Re-create all foreign keys in the Spanner database.
echo "Re-creating all foreign keys in the Spanner database"
psql -v ON_ERROR_STOP=1 -a \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  -d "$spanner_restore_database" \
  -c "start batch ddl" \
  -f "create_foreign_keys.sql" \
  -c "run batch"

echo "Finished restoring database $spanner_restore_database"
