#!/bin/bash

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

psql << SQL
  -- Start a transaction.
  begin;
  -- Set the TRANSACTION_TAG session variable to set a transaction tag
  -- for the current transaction. This can only be executed at the start
  -- of the transaction.
  set spanner.transaction_TAG='example-tx-tag';
  
  -- Set the STATEMENT_TAG session variable to set the request tag
  -- that should be included with the next SQL statement.
  set spanner.statement_tag='query-marketing-budget';
  
  select marketing_budget
  from albums
  where singer_id = 1
    and album_id  = 1;
  
  -- Reduce the marketing budget by 10% if it is more than 1,000.
  -- Set a statement tag for the update statement.
  set spanner.statement_tag='reduce-marketing-budget';
  
  update albums
    set marketing_budget = marketing_budget - (marketing_budget * 0.1)::bigint
  where singer_id = 1
    and album_id  = 1
    and marketing_budget > 1000;
  
  commit;  
SQL
