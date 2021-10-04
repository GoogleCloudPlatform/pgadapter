#!/bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

if [ "$#" -ne 1 ]; then
    echo "You must specify the action you'd like to run"
fi

echo "${JSON_SERVICE_ACCOUNT_CREDENTIALS}" > /tmp/service_account_credentials.json
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/service_account_credentials.json

JOB_TYPE="$1"
echo "running action ${JOB_TYPE}"

RETURN_CODE=0
set +e

case ${JOB_TYPE} in
units)
  mvn verify -Dclirr.skip=true -DskipITs=true
  ;;
lint)
  mvn com.coveo:fmt-maven-plugin:check
  ;;
clirr)
  mvn clirr:check
  ;;
integration)
  mvn verify -Dclirr.skip=true -DskipITs=false
  ;;
release)
  mvn package org.apache.maven.plugins:maven-deploy-plugin:deploy -DskipTests -DskipITs
  ;;
uber-jar-build)
  mvn package -Pshade -DskipTests
  ;;
uber-jar-release)
  PGADAPTER_VERSION="$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)"
  UBER_JAR="google-cloud-spanner-pgadapter-${PGADAPTER_VERSION}.jar"
  gsutil cp target/"${UBER_JAR}" "gs://${UBER_JAR_GCS_BUCKET}/${UBER_JAR_GCS_PATH}/${UBER_JAR}"
  ;;
docker-configure)
  gcloud auth configure-docker "${DOCKER_HOSTNAME}" -q
  ;;
docker-build)
  PGADAPTER_VERSION="$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)"
  UBER_JAR="google-cloud-spanner-pgadapter-${PGADAPTER_VERSION}.jar"
  docker build -f "${DOCKERFILE}" -t "${DOCKER_HOSTNAME}/${GOOGLE_CLOUD_PROJECT}/${DOCKER_REPOSITORY}/${DOCKER_IMAGE}" --build-arg UBER_JAR_PATH="target/${UBER_JAR}" .
  ;;
docker-push)
  docker push "${DOCKER_HOSTNAME}/${GOOGLE_CLOUD_PROJECT}/${DOCKER_REPOSITORY}/${DOCKER_IMAGE}"
  ;;
e2e-psql)
  PSQL_VERSION="$2"
  GOOGLE_CLOUD_DATABASE_WITH_VERSION="${GOOGLE_CLOUD_DATABASE}_v${PSQL_VERSION}"
#  create testing database
  gcloud config set api_endpoint_overrides/spanner "https://${GOOGLE_CLOUD_ENDPOINT}/"
  gcloud alpha spanner databases create "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" --instance="${GOOGLE_CLOUD_INSTANCE}" --database-dialect=POSTGRESQL
  gcloud spanner databases ddl update "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" --instance="${GOOGLE_CLOUD_INSTANCE}" --ddl='CREATE TABLE users (id bigint PRIMARY KEY, age bigint, name text);'
  gcloud spanner databases execute-sql "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" --instance="${GOOGLE_CLOUD_INSTANCE}" --sql="INSERT INTO users (id, age, name) VALUES (1, 1, 'John'), (2, 20, 'Joe'), (3, 23, 'Jack');"
#  start PgAdaptor
  PGADAPTER_VERSION="$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)"
  UBER_JAR="google-cloud-spanner-pgadapter-${PGADAPTER_VERSION}.jar"
  (java -jar target/"${UBER_JAR}" -p "${GOOGLE_CLOUD_PROJECT}" -i "${GOOGLE_CLOUD_INSTANCE}" -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" -e "${GOOGLE_CLOUD_ENDPOINT}" -s 4242 -q > /dev/null 2>&1) &
  BACK_PID=$!
  sleep 1
#  execute psql and evaluate result
  mkdir .ci/e2e-result
  RETURN_CODE=0
  . .ci/evaluate-with-psql.sh
#  cleanup and exit
  rm -r .ci/e2e-result
  kill ${BACK_PID}
  sleep 1
  gcloud spanner databases delete "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" --instance=${GOOGLE_CLOUD_INSTANCE} --quiet
  echo "exiting with ${RETURN_CODE}"
  exit ${RETURN_CODE}
  ;;
*)
  echo "Job type not found ${JOB_TYPE}"
  RETURN_CODE=1
  echo "exiting with ${RETURN_CODE}"
  exit ${RETURN_CODE}
  ;;
esac
RETURN_CODE=$?

echo "exiting with ${RETURN_CODE}"
exit ${RETURN_CODE}
