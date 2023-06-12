#!/bin/bash
set -e

# Drop and re-create the 'public' schema.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	drop schema if exists public cascade;
	create schema public;
EOSQL

# Copy all schema objects from Cloud Spanner to the local database.
(psql -v ON_ERROR_STOP=1 --host "$PGADAPTER_CONTAINER_NAME" --dbname "$SPANNER_DATABASE" -qAtX <<-EOSQL
  show database ddl for postgresql;
EOSQL
) | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"
