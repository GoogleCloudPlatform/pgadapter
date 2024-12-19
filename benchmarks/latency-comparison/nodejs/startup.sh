#!/bin/bash

# Start PGAdapter.
java -jar /home/pgadapter/pgadapter.jar > /pgadapter.log 2>&1 &
sleep 2
echo "Started PGAdapter in the background"

# Run the benchmark.
exec "node" "build/index.js" "$@"
