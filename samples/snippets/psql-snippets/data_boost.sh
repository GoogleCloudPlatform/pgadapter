#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

# 'set spanner.data_boost_enabled=true' enables Data Boost for
# all partitioned queries on this connection.

# 'run partitioned query' is a shortcut for partitioning the query
# that follows and executing each of the partitions that is returned
# by Spanner.

psql -c "set spanner.data_boost_enabled=true" \
     -c "run partitioned query
         select singer_id, first_name, last_name
         from singers"
