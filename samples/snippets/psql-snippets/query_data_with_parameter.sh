#!/bin/bash

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

# Create a prepared statement to use a query parameter.
# Using a prepared statement for executing the same SQL string multiple
# times increases the execution speed of the statement.
psql -c "PREPARE select_singer AS
         SELECT singer_id, first_name, last_name
         FROM singers
         WHERE last_name = \$1" \
     -c "EXECUTE select_singer ('Garcia')"
