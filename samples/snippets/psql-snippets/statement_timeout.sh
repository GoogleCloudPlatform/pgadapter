#!/bin/bash

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-example-db}"

# Set the statement timeout that should be used for all statements
# on this connection to 5 seconds.
# Supported time units are 's' (seconds), 'ms' (milliseconds),
# 'us' (microseconds), and 'ns' (nanoseconds).
psql -c "set statement_timeout='5s'" \
     -c "SELECT singer_id, album_id, album_title
         FROM albums 
         WHERE album_title in (
           SELECT first_name 
           FROM singers 
           WHERE last_name LIKE '%a%'
              OR last_name LIKE '%m%'
         )"
