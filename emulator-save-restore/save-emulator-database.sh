#!/bin/bash

# This script exports the schema and data from a Cloud Spanner database to a
# local file. It also exports the DDL statements that are needed to re-create
# the Cloud Spanner database. The combination of the pg_dump files and the
# schema files can be used to re-create the Cloud Spanner database.

set -e

if [ -z "$schema_file" ]; then schema_file="./backup/schema.sql"; fi;
if [ -z "$data_dir" ]; then data_dir="./backup/data"; fi;

echo
echo "--- BACKING UP CLOUD SPANNER DATABASE ---"
echo "Schema file will be written to $schema_file."
echo "Data will be written to $data_dir. Any existing data in that directory will be overwritten."

echo
read -p "Continue? [Yn]" -n 1 -r
echo
if [[ $REPLY =~ [Nn]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Create the output folder if it does not already exist.
mkdir -p "$data_dir"

# Export the current schema of the Cloud Spanner database.
echo "Exporting schema of the Cloud Spanner database $SPANNER_DATABASE into $schema_file"
psql -v ON_ERROR_STOP=1 \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  --dbname "$SPANNER_DATABASE" \
  -qAtX \
  -c "show database ddl" > "$schema_file"

echo "COPYING DATA FROM CLOUD SPANNER TO LOCAL FILE"

# Then copy all data from Cloud Spanner to the local file.
# We will do that at a fixed timestamp to ensure that we copy a consistent set of data.
READ_TIMESTAMP=$(psql -h "$PGADAPTER_HOST" \
                      -p "$PGADAPTER_PORT" \
                      -d "$SPANNER_DATABASE" \
                      -c "set time zone utc; select now()" -qAtX)
READ_TIMESTAMP="${READ_TIMESTAMP/ /T}"
READ_TIMESTAMP="${READ_TIMESTAMP/+00/Z}"
echo "Reading data from Cloud Spanner using timestamp $READ_TIMESTAMP"
echo "set spanner.read_only_staleness='read_timestamp $READ_TIMESTAMP'"
echo ""
echo " --- COPYING DATA WITH MAX_PARALLELISM $max_parallelism. --- "
echo "Set the 'max_parallelism' variable to increase the number of COPY operations that will run in parallel."
echo ""

table_names=()

psql -v ON_ERROR_STOP=1 -h "$PGADAPTER_HOST" -p "$PGADAPTER_PORT" -d "$SPANNER_DATABASE" -qAtX \
  -c "select tablename from pg_catalog.pg_tables where schemaname='public';" \
| while read table_name ; do
  column_names=$(psql -h "$PGADAPTER_HOST" -p "$PGADAPTER_PORT" -d "$SPANNER_DATABASE" -qAtX \
    -c "select array_to_string(array_agg(column_name), ',') as c from information_schema.columns where table_name='$table_name' and table_schema='public' and not is_generated='ALWAYS';")

  sql_file="$data_dir/$table_name.sql"

  echo ""
  echo "Copying data for $table_name"
  echo "COPY $table_name ($column_names) FROM stdin;" > $sql_file
  # Set the read_only_staleness to use in PGOPTIONS. This will automatically be included by psql
  # when connecting to PGAdapter.
  PGOPTIONS="-c spanner.read_only_staleness='read_timestamp $READ_TIMESTAMP'" \
    psql -v ON_ERROR_STOP=1 \
       -h "$PGADAPTER_HOST" \
       -p "$PGADAPTER_PORT" \
       -d "$SPANNER_DATABASE" \
       -c "copy $table_name ($column_names) to stdout" \
       >> $sql_file
done

echo "Finished exporting database $SPANNER_DATABASE"
