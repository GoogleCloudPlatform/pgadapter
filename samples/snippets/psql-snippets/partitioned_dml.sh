#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

# Change the DML mode that is used by this connection to Partitioned
# DML. Partitioned DML is designed for bulk updates and deletes.
# See https://cloud.google.com/spanner/docs/dml-partitioned for more
# information.
psql -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'" \
     -c "update albums
         set marketing_budget=0
         where marketing_budget is null"
