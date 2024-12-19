#!/bin/bash

# Set the connection variables for psql.
# The following statements use the existing value of the variable if it has
# already been set, and otherwise assigns a default value.
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

# Create two tables in one batch.
psql << SQL
-- Create the singers table
CREATE TABLE singers (
  singer_id   bigint not null primary key,
  first_name  character varying(1024),
  last_name   character varying(1024),
  singer_info bytea,
  full_name   character varying(2048) GENERATED ALWAYS
          AS (first_name || ' ' || last_name) STORED
);

-- Create the albums table. This table is interleaved in the parent table
-- "singers".
CREATE TABLE albums (
  singer_id     bigint not null,
  album_id      bigint not null,
  album_title   character varying(1024),
  primary key (singer_id, album_id)
)
-- The 'interleave in parent' clause is a Spanner-specific extension to
-- open-source PostgreSQL.
INTERLEAVE IN PARENT singers ON DELETE CASCADE;
SQL

echo "Created Singers & Albums tables in database: [${PGDATABASE}]"
