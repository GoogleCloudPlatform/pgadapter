#!/bin/bash

# Set the connection variables for psql.
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

# Create two tables in one batch.
psql << SQL
CREATE TABLE Singers (
  SingerId   bigint NOT NULL,
  FirstName  character varying(1024),
  LastName   character varying(1024),
  SingerInfo bytea,
  FullName character varying(2048) GENERATED ALWAYS
           AS (FirstName || ' ' || LastName) STORED,
  PRIMARY KEY (SingerId)
);

CREATE TABLE Albums (
  SingerId     bigint NOT NULL,
  AlbumId      bigint NOT NULL,
  AlbumTitle   character varying(1024),
  PRIMARY KEY (SingerId, AlbumId)
)
INTERLEAVE IN PARENT Singers ON DELETE CASCADE;
SQL
