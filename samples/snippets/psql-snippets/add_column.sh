#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

psql -c "ALTER TABLE albums ADD COLUMN marketing_budget bigint"
