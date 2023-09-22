#!/bin/bash
set -euo pipefail

PGADAPTER_YCSB_RUNNER=pgadapter-ycsb-runner
PGADAPTER_YCSB_JOB=pgadapter-ycsb-job
PGADAPTER_YCSB_REGION=europe-north1

SPANNER_INSTANCE=pgadapter-ycsb-regional-test
SPANNER_DATABASE=pgadapter-ycsb-test

gcloud config set run/region $PGADAPTER_YCSB_REGION
gcloud config set builds/region $PGADAPTER_YCSB_REGION

gcloud auth configure-docker gcr.io --quiet
docker build --platform=linux/amd64 . -f benchmarks/ycsb/Dockerfile \
  -t gcr.io/$(gcloud config get project --quiet)/$PGADAPTER_YCSB_RUNNER
docker push gcr.io/$(gcloud config get project --quiet)/$PGADAPTER_YCSB_RUNNER

gcloud beta run jobs delete $PGADAPTER_YCSB_JOB --quiet || true
sleep 3
gcloud beta run jobs create $PGADAPTER_YCSB_JOB \
    --image gcr.io/$(gcloud config get project --quiet)/$PGADAPTER_YCSB_RUNNER \
    --tasks 1 \
    --set-env-vars SPANNER_INSTANCE=$SPANNER_INSTANCE \
    --set-env-vars SPANNER_DATABASE=$SPANNER_DATABASE \
    --max-retries 0 \
    --cpu 8 \
    --memory 4Gi \
    --task-timeout 60m \
    --binary-authorization=projects/bcid-cloud-run-policy/platforms/cloudRun/policies/bcidManagedPolicy
gcloud beta run jobs execute $PGADAPTER_YCSB_JOB
