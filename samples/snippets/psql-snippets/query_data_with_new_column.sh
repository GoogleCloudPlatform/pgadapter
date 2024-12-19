#!/bin/bash

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

psql -c "SELECT singer_id, album_id, marketing_budget
         FROM albums
         ORDER BY singer_id, album_id"
