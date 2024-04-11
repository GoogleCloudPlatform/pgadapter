#!/bin/bash

# Get the source directory of this script.
directory=${BASH_SOURCE%/*}/

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

# Copy data to Spanner from a tab-separated text file using the COPY command.
psql -c "COPY singers (singer_id, first_name, last_name) FROM STDIN" \
  < "${directory}singers_data.txt"
psql -c "COPY albums FROM STDIN" \
  < "${directory}albums_data.txt"
