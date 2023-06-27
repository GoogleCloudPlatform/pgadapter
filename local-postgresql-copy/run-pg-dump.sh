#!/bin/bash
set -e

# This script copies the schema and data from a Cloud Spanner database to a PostgreSQL database
# and then runs pg_dump on the PostgreSQL database. It also exports the DDL statements that are
# needed to re-create the Cloud Spanner database. The combination of the pg_dump files and the
# schema files can be used to re-create the Cloud Spanner database.

if [ -z "$schema_file" ]; then schema_file="./backup/schema.sql"; fi;
if [ -z "$data_dir" ]; then data_dir="./backup/data"; fi;
if [ -z "$copy_data" ]; then copy_data="true"; fi;
if [ -z "$max_parallelism" ]; then max_parallelism=1; fi;

echo
echo "--- BACKING UP CLOUD SPANNER DATABASE ---"
echo "Schema file will be written to $schema_file."
echo "Data will be written to $data_dir. Any existing data in that directory will be overwritten."
if [ "$copy_data" = "true" ];
then
  echo "The script will first re-import data from Cloud Spanner to the PostgreSQL database $POSTGRES_DB."
  echo "--- WARNING: THIS WILL OVERWRITE ALL EXISTING DATA IN THE PostgreSQL DATABASE! ---"
else
  echo "The script will use the existing data in the PostgreSQL database $POSTGRES_DB."
fi;
echo "Copying data with max_parallelism: $max_parallelism "

echo
read -p "Continue? [Yn]" -n 1 -r
echo
if [[ $REPLY =~ [Nn]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Export the current schema of the Cloud Spanner database.
echo "Exporting schema of the Cloud Spanner database $SPANNER_DATABASE into $schema_file"
psql -v ON_ERROR_STOP=1 \
  --host "$PGADAPTER_HOST" \
  --port "$PGADAPTER_PORT" \
  --dbname "$SPANNER_DATABASE" \
  -qAtX \
  -c "show database ddl" > "$schema_file"

# Check if we should copy the data from Cloud Spanner to the PostgreSQL database first.
if [ "$copy_data" = "true" ];
then
  echo "Copying schema from Cloud Spanner to the PostgreSQL database $POSTGRES_DB"
  ./copy-schema.sh
  echo "Copying data from Cloud Spanner to the PostgreSQL database $POSTGRES_DB"
  ./copy-data.sh
fi;

# Dump the database in directory format.
echo "Executing pg_dump on the PostgreSQL database $POSTGRES_DB. The dump will be written to $data_dir"
rm -rf "$data_dir"
PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -v --format=directory \
  --file="$data_dir" \
  -U "$POSTGRES_USER" \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -d "$POSTGRES_DB"

echo "Finished running pg_dump on the PostgreSQL database $POSTGRES_DB. The dump is in $data_dir"
