#!/bin/bash

RESULT=0
MESSAGE=
CONTAINER=$(docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator)
HOST_AND_PORT=$(docker port "$CONTAINER" 5432)

export PGHOST=localhost
export PGPORT="${HOST_AND_PORT##*:}"
export PGDATABASE="example-db"
sleep 2

OUTPUT=$(source ../create_tables.sh)
EXPECTED_OUTPUT=$(
  echo "CREATE"
  echo "CREATE"
)
if [ "$OUTPUT" != "$EXPECTED_OUTPUT" ]; then
  RESULT=1
  MESSAGE="Received unexpected output: $OUTPUT"
fi

STOPPED_CONTAINER=$(docker container stop "$CONTAINER")
if [ "$STOPPED_CONTAINER" != "$CONTAINER" ]; then
  RESULT=1
  MESSAGE="Failed to stop container"
fi

if [ "$MESSAGE" != "" ]; then
  echo "$MESSAGE"
  exit "$RESULT"
fi
