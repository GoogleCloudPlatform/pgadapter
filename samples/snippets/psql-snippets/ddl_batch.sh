#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

# Use a single SQL command to batch multiple statements together.
# Executing multiple DDL statements as one batch is more efficient
# than executing each statement individually.
# Separate the statements with semicolons.
psql << SQL

CREATE TABLE venues (
  venue_id    bigint not null primary key,
  name        varchar(1024),
  description jsonb
);

CREATE TABLE concerts (
  concert_id bigint not null primary key ,
  venue_id   bigint not null,
  singer_id  bigint not null,
  start_time timestamptz,
  end_time   timestamptz,
  constraint fk_concerts_venues foreign key
    (venue_id) references venues (venue_id),
  constraint fk_concerts_singers foreign key
    (singer_id) references singers (singer_id)
);

SQL
