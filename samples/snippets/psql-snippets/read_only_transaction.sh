#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

psql << SQL
  -- Begin a transaction.
  begin;
  -- Change the current transaction to a read-only transaction.
  -- This statement can only be executed at the start of a transaction.
  set transaction read only;
  
  -- The following two queries use the same read-only transaction.
  select singer_id, album_id, album_title
  from albums
  order by singer_id, album_id;
  
  select singer_id, album_id, album_title
  from albums
  order by album_title;
  
  -- Read-only transactions must also be committed or rolled back to mark
  -- the end of the transaction. There is no semantic difference between
  -- rolling back or committing a read-only transaction.
  commit;  
SQL
