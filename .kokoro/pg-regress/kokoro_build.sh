#!/bin/bash

# Fail on any error.
set -e

sudo apt update && sudo apt install uuid-runtime

uuid=$(uuidgen -r | cut -c1-6)
GCP_PROJECT_ID="span-cloud-testing"
INSTANCE_ID="pgregress-testing"
DATABASE_ID="pg_regress_$uuid"
BQ_TABLE="spanner_pg_regress_results.cloud_prod_results"
TARGET_ENV="cloud_prod"
GCS_BUCKET_PATH="gs://pgadapter-pg-regress/cloud-prod-results"

echo "DATABASE_ID: ${DATABASE_ID}"

gcloud config set project $GCP_PROJECT_ID
export GOOGLE_CLOUD_PROJECT=$GCP_PROJECT_ID

# Display commands being run.
# WARNING: please only enable 'set -x' if necessary for debugging, and be very
#  careful if you handle credentials (e.g. from Keystore) with 'set -x':
#  statements like "export VAR=$(cat /tmp/keystore/credentials)" will result in
#  the credentials being printed in build logs.
#  Additionally, recursive invocation with credentials as command-line
#  parameters, will print the full command, with credentials, in the build logs.
# set -x

# Code under repo is checked out to ${KOKORO_ARTIFACTS_DIR}/github.
# The final directory name in this path is determined by the scm name specified
# in the job configuration.
cd "${KOKORO_ARTIFACTS_DIR}/github/"

# Start pgadapter in a background process
#wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz \
#  && tar -xzvf pgadapter.tar.gz

# Build pgadapter from source
cd pgadapter/
mvn package -P assembly
cd target/pgadapter
java -jar pgadapter.jar -p $GCP_PROJECT_ID -i $INSTANCE_ID -d $DATABASE_ID &

cd "${KOKORO_ARTIFACTS_DIR}/github/"

# Install python deps
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.12 -y
python --version
pip install google-cloud-bigquery

# Install psql
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install --yes --no-install-recommends postgresql-client-17
psql --version

# Get postgresql source code
git clone https://github.com/postgres/postgres.git

cp pgadapter/benchmarks/pg_regress/*.patch postgres/src/test/regress/
cp pgadapter/benchmarks/pg_regress/*.py postgres/src/test/regress/

cd postgres/src/test/regress/

git checkout REL_16_0

git apply expected.patch
git apply sql.patch
git apply code.patch

# Temporarily enable timing
#(echo '\timing on' | cat - sql/alter_table.sql) > temp.sql
#mv temp.sql sql/alter_table.sql

# Run pg-regress test
python start_test.py spanner_prod --skip-container \
                --project $GCP_PROJECT_ID \
                --instance $INSTANCE_ID \
                --database $DATABASE_ID
#                --testcases='alter_table'
python compare_results.py expected/ results/

#cat results/alter_table.out
cat results.json

ts=$(date +%s)
python upload_bigquery.py results.json $BQ_TABLE $TARGET_ENV $ts
gcloud storage cp results.json $GCS_BUCKET_PATH/results_$ts.json
gcloud storage cp regression.diffs $GCS_BUCKET_PATH/regression_$ts.diffs

