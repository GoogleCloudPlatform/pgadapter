#!/bin/bash

# Run both PGAdapter and the web server when the Docker container is started.
# Start PGAdapter in plain TCP only mode, that is:
# 1. Do not specify any credentials. This will make sure PGAdapter picks the credentials from the current environment.
#    You must ensure that the service account that is used by Cloud Run is allowed to access the Cloud Spanner database
#    that your application is using.
# 2. Do not specify any specific project, instance or database. This will require the application to specify a fully
#    qualified database name when connecting to PGAdapter.
# 3. Set the Unix domain socket directory to the empty string to disable domain sockets. Domain sockets are not
#    supported on Cloud Run.

# Start PGAdapter in the background.
java -jar /home/pgadapter/pgadapter.jar -p cloud-spanner-pg-adapter -i pgadapter-ycsb-regional-test &

# Run the benchmark.
exec "/app/benchmark"
