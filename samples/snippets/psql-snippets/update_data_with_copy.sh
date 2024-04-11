#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

# Instruct PGAdapter to use insert-or-update for COPY statements.
# This enables us to use COPY to update data.
psql -c "set spanner.copy_upsert=true" \
     -c "COPY albums (singer_id, album_id, marketing_budget) FROM STDIN
         WITH (DELIMITER ';')" \
<< DATA
1;1;100000
2;2;500000
DATA
