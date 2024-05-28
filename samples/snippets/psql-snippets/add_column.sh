#!/bin/bash

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

psql -c "ALTER TABLE albums ADD COLUMN marketing_budget bigint"
