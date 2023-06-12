#!/bin/bash
set -e

echo "$@"

if [ -z "$schema_file" ]; then schema_file="/backup/schema.sql"; fi;
if [ -z "$data_dir" ]; then data_dir="/backup/data"; fi;
if [ -z "$copy_data" ]; then copy_data="false"; fi;

echo
echo "--- BACKING UP CLOUD SPANNER DATABASE ---"
echo "Schema file will be written to $schema_file."
echo "Data will be written to $data_dir. Any existing data will be overwritten."
if [ "$copy_data" = "true" ];
then
  echo "The script will first re-import data from Cloud Spanner"
else
  echo "The script will use the existing data in the local PostgreSQL database"
fi;

echo
read -p "Continue? [yN]" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

# Export the current schema of the Cloud Spanner database.
echo "Exporting schema of the Cloud Spanner database $SPANNER_DATABASE into $schema_file"
psql -v ON_ERROR_STOP=1 \
  --host "$PGADAPTER_CONTAINER_NAME" \
  --dbname "$SPANNER_DATABASE" \
  -qAtX \
  -c "show database ddl" > "$schema_file"

# Check if we should copy the data from Cloud Spanner to the local database first.
if [ "$copy_data" = "true" ];
then
  echo "Copying data from Cloud Spanner to the local database $POSTGRES_DB"
  ./copy-data.sh
fi;

# Dump the database in directory format. Note that we skip any schema that starts with '_'.
# Those are the schemas that contain the foreign table definitions.
echo "Executing pg_dump on the local database $POSTGRES_DB. The dump will be written to $data_dir"
rm -rf "$data_dir"
pg_dump -v --format=directory \
  --file="$data_dir" \
  --exclude-schema=_ \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB"
