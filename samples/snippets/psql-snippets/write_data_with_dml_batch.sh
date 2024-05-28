#!/bin/bash

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

# Create a prepared insert statement and execute this prepared
# insert statement three times in one SQL string. The single
# SQL string with three insert statements will be executed as
# a single DML batch on Spanner.
psql -c "PREPARE insert_singer AS
           INSERT INTO singers (singer_id, first_name, last_name)
           VALUES (\$1, \$2, \$3)" \
     -c "EXECUTE insert_singer (16, 'Sarah', 'Wilson');
         EXECUTE insert_singer (17, 'Ethan', 'Miller');
         EXECUTE insert_singer (18, 'Maya', 'Patel');"

echo "3 records inserted"
