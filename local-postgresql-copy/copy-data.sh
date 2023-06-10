#!/bin/bash
set -e

echo "COPYING DATA FROM CLOUD SPANNER TO LOCAL POSTGRESQL"

# First truncate all tables.
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -qAtX \
  -c "select table_name from information_schema.tables where table_type='BASE TABLE' and table_schema='public';" \
| while read table_name ; do
  echo ""
  echo "Truncating data for $table_name"
  psql -a -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "truncate $table_name cascade"
done

# Then copy all data from Cloud Spanner to the local PostgreSQL database.
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -qAtX \
  -c "select table_name from information_schema.tables where table_type='BASE TABLE' and table_schema='public';" \
| while read table_name ; do
  column_names=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -qAtX \
    -c "select array_to_string(array_agg(column_name), ',') as c from information_schema.columns where table_name='$table_name' and table_schema='public' and not is_generated='ALWAYS';")

  echo ""
  echo "Copying data for $table_name"
  psql -h pgadapter -c "copy $table_name ($column_names) to stdout binary" \
  | psql -a -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
      -c "set session_replication_role='replica'" \
      -c "copy $table_name ($column_names) from stdin binary" \
      -c "set session_replication_role='origin'"
done
