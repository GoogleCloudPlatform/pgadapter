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
java -jar /pgadapter/pgadapter.jar -dir= &

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
port="${PORT:-8080}"
exec gunicorn --bind ":$port" --workers 1 --threads 8 --timeout 0 main:app
