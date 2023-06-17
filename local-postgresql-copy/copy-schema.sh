#!/bin/bash
set -e

# Drop and re-create the 'public' schema on the PostgreSQL database.
PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --dbname "$POSTGRES_DB" <<-EOSQL
	drop schema if exists public cascade;
	create schema public;
EOSQL

# Copy all schema objects from Cloud Spanner to the PostgreSQL database.
(psql -v ON_ERROR_STOP=1 --host "$PGADAPTER_HOST" --port "$PGADAPTER_PORT" --dbname "$SPANNER_DATABASE" -qAtX <<-EOSQL
  -- Note: The 'for postgresql' clause will comment out all Cloud Spanner specific DDL clauses, like for example INTERLEAVE IN.
  show database ddl for postgresql;
EOSQL
) | PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --dbname "$POSTGRES_DB"
