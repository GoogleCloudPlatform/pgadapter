#!/bin/bash
set -euo pipefail

PGADAPTER_YCSB_RUNNER=pgadapter-ycsb-runner
PGADAPTER_YCSB_JOB=pgadapter-ycsb-job
PGADAPTER_YCSB_REGION=europe-west3

SPANNER_INSTANCE=test-instance
SPANNER_DATABASE=pgadapter_ycsb

gcloud config set run/region $PGADAPTER_YCSB_REGION
gcloud config set builds/region $PGADAPTER_YCSB_REGION

gcloud auth configure-docker gcr.io --quiet
docker build --platform=linux/amd64 . -t gcr.io/$(gcloud config get project --quiet)/$PGADAPTER_YCSB_RUNNER
docker push gcr.io/$(gcloud config get project --quiet)/$PGADAPTER_YCSB_RUNNER

gcloud beta run jobs delete $PGADAPTER_YCSB_JOB --quiet || true
sleep 3
gcloud beta run jobs create $PGADAPTER_YCSB_JOB \
    --image gcr.io/$(gcloud config get project --quiet)/$PGADAPTER_YCSB_RUNNER \
    --tasks 1 \
    --set-env-vars SPANNER_INSTANCE=$SPANNER_INSTANCE \
    --set-env-vars SPANNER_DATABASE=$SPANNER_DATABASE \
    --max-retries 0 \
    --cpu 4 \
    --memory 2Gi \
    --task-timeout 60m
gcloud beta run jobs execute $PGADAPTER_YCSB_JOB
