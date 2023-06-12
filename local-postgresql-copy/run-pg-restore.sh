#!/bin/bash
set -e

spanner_database="$SPANNER_DATABASE-restored"
postgres_db="$POSTGRES_DB-restored"
schema="/backup/schema.sql"
data="/backup/data"

for ARGUMENT in "$@"
do
  KEY=$(echo "$ARGUMENT" | cut -f1 -d=)

  KEY_LENGTH=${#KEY}
  VALUE="${ARGUMENT:$KEY_LENGTH+1}"

  "$KEY"="$VALUE"
done

echo "RESTORING CLOUD SPANNER DATABASE"
echo "Using temporary PostgreSQL database $postgres_db to run pg_restore"
echo "Restoring to Cloud Spanner database $spanner_database"
echo "Restoring schema file $schema"
echo "Restoring data from $data"

# Create the database that we will restore into.
echo "Restoring into local PostgreSQL database $postgres_db"
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "drop database if exists \"$postgres_db\""
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "create database \"$postgres_db\""
# Restore the database.
pg_restore -v --format=directory -U "$POSTGRES_USER" -d "$postgres_db" /backup/data

# Create the schema on the Cloud Spanner database.
echo "Creating schema on Cloud Spanner database $spanner_database"
psql -v ON_ERROR_STOP=1 --host "$PGADAPTER_CONTAINER_NAME" -d "$spanner_database" -f "$schema"
# Copy data from the local PostgreSQL database to Cloud Spanner.
