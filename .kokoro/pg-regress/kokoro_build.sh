#!/bin/bash

# Fail on any error.
set -e

GCP_PROJECT_ID="span-cloud-testing"
INSTANCE_ID="pgadapter-testing"
DATABASE_ID="pg_regress"

gcloud config set project $GCP_PROJECT_ID

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
wget https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz \
  && tar -xzvf pgadapter.tar.gz
java -jar pgadapter.jar -p span-cloud-testing -i pgadapter-testing -d pg_regress &

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

# Run pg-regress test
python start_test.py spanner_prod --skip-container \
                --project $GCP_PROJECT_ID \
                --instance $INSTANCE_ID \
                --database $DATABASE_ID
python compare_results.py expected/ results/

