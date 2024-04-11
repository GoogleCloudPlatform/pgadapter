#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

psql << SQL
  -- Transfer marketing budget from one album to another.
  -- We do it in a transaction to ensure that the transfer is atomic.
  -- Begin a read/write transaction.
  begin;
  
  -- Increase the marketing budget of album 1 if album 2 has enough budget.
  -- The condition that album 2 has enough budget is guaranteed for the
  -- duration of the transaction, as read/write transactions in Spanner use
  -- external consistency as the default isolation level.
  update albums set
    marketing_budget = marketing_budget + 200000
  where singer_id = 1
    and  album_id = 1
    and exists (
      select album_id
      from albums
      where singer_id = 2
        and  album_id = 2
        and marketing_budget > 200000
      );

  -- Decrease the marketing budget of album 2.      
  update albums set
    marketing_budget = marketing_budget - 200000
  where singer_id = 2
    and  album_id = 2
    and marketing_budget > 200000;
  
  -- Commit the transaction to make the changes to both marketing budgets
  -- durably stored in the database and visible to other transactions.
  commit;  
SQL
