#!/bin/bash
if ! shopt -s lastpipe; then
  # Parallel copy requires support for shopt lastpipe. This is means bash version 4.2 or higher.
  # MacOS by default uses an ancient version of bash, which means that this is by default not
  # supported on MacOS.
  max_parallelism=1
fi
set -e

# Set this variable to a higher number to increase the amount of parallelism. That can increase the
# speed of the COPY operation.
if [ -z "$max_parallelism" ]; then max_parallelism=1; fi;

function wait_for_copy() {
  pid=${pids[${current_wait_index}]}
  waiting_for_table=${table_names[${current_wait_index}]}
  echo "Waiting for COPY of table $waiting_for_table (PID $pid)"
  wait "$pid"
  current_wait_index=$((current_wait_index+1))
}

echo "COPYING DATA FROM CLOUD SPANNER TO POSTGRESQL"

# First truncate all tables.
PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -d "$POSTGRES_DB" -qAtX \
  -c "select table_name from information_schema.tables where table_type='BASE TABLE' and table_schema='public';" \
| while read table_name ; do
  echo ""
  echo "Truncating data for $table_name"
  PGPASSWORD="$POSTGRES_PASSWORD" psql -a -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -d "$POSTGRES_DB" -c "truncate $table_name cascade"
done

# Then copy all data from Cloud Spanner to the PostgreSQL database.
# We will do that at a fixed timestamp to ensure that we copy a consistent set of data.
READ_TIMESTAMP=$(psql -h "$PGADAPTER_HOST" \
                      -p "$PGADAPTER_PORT" \
                      -d "$SPANNER_DATABASE" \
                      -c "set time zone utc; select now()" -qAtX)
READ_TIMESTAMP="${READ_TIMESTAMP/ /T}"
READ_TIMESTAMP="${READ_TIMESTAMP/+00/Z}"
echo "Reading data from Cloud Spanner using timestamp $READ_TIMESTAMP"
echo "set spanner.read_only_staleness='read_timestamp $READ_TIMESTAMP'"
echo ""
echo " --- COPYING DATA WITH MAX_PARALLELISM $max_parallelism. --- "
echo "Set the 'max_parallelism' variable to increase the number of COPY operations that will run in parallel."
echo ""

# Keep track of the PIDs of the COPY processes that we start.
pids=()
table_names=()
current_index=0
current_wait_index=0

PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -d "$POSTGRES_DB" -qAtX \
  -c "select table_name from information_schema.tables where table_type='BASE TABLE' and table_schema='public';" \
| while read table_name ; do
  column_names=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -d "$POSTGRES_DB" -qAtX \
    -c "select array_to_string(array_agg(column_name), ',') as c from information_schema.columns where table_name='$table_name' and table_schema='public' and not is_generated='ALWAYS';")

  echo ""
  echo "Copying data for $table_name"
  # Set the read_only_staleness to use in PGOPTIONS. This will automatically be included by psql
  # when connecting to PGAdapter.
  # We also set the PostgreSQL session in 'session_replication_mode='replica' to disable all triggers
  # during the import. Foreign keys are implemented as triggers in PostgreSQL, so this automatically
  # also disables foreign key constraint checks during the import.
  PGOPTIONS="-c spanner.read_only_staleness='read_timestamp $READ_TIMESTAMP'" \
    psql -v ON_ERROR_STOP=1 \
       -h "$PGADAPTER_HOST" \
       -p "$PGADAPTER_PORT" \
       -d "$SPANNER_DATABASE" \
       -c "copy $table_name ($column_names) to stdout binary" \
  | PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -a -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -d "$POSTGRES_DB" \
      -c "set session_replication_role='replica'" \
      -c "copy $table_name ($column_names) from stdin binary" \
      -c "set session_replication_role='origin'" &
  if [ $max_parallelism -gt 1 ]; then
    pids[${current_index}]=$!
    table_names[${current_index}]="$table_name"
    current_index=$((current_index+1))
    # Make sure we don't execute too many COPY operations in parallel.
    if [ $current_index -ge $max_parallelism ]; then
      wait_for_copy
    fi
  else
    wait $!
  fi
done

if [ $max_parallelism -gt 1 ]; then
  # Wait for the remaining COPY commands.
  while [ $current_wait_index -lt $current_index ]
  do
    wait_for_copy
  done
fi
