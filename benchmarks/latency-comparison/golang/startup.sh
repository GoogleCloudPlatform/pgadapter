#!/bin/bash

# Start PGAdapter in the background.
java -jar /home/pgadapter/pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb &

# Run the benchmark.
exec "/app/benchmark" -embedded=false
