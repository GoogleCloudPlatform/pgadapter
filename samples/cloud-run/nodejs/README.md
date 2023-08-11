# PGAdapter Cloud Run Sidecar Sample for Node.js

This sample application shows how to build and deploy a Node.js application with PGAdapter as a sidecar
to Google Cloud Run. The Node.js application connects to PGAdapter using a Unix domain socket using an
in-memory volume. This gives the lowest possible latency between your application and PGAdapter.

The sample is based on the [Cloud Run Quickstart Guide for Node.js](https://cloud.google.com/run/docs/quickstarts/build-and-deploy/deploy-nodejs-service).
Refer to that guide for more in-depth information on how to work with Cloud Run.

## Configure

Modify the `service.yaml` file to match your Cloud Run project and region, and your Cloud Spanner database:

```shell
# TODO: Modify MY-REGION and MY-PROJECT to match your application container image.
image: MY-REGION.pkg.dev/MY-PROJECT/cloud-run-source-deploy/pgadapter-sidecar-example
...
# TODO: Modify these environment variables to match your Cloud Spanner database.
env:
  - name: SPANNER_PROJECT
    value: my-project
  - name: SPANNER_INSTANCE
    value: my-instance
  - name: SPANNER_DATABASE
    value: my-database
```

## Optional - Build and Run Locally

You can test the application locally to verify that the Cloud Spanner project, instance, and database
configuration is correct. For this, you first need to start PGAdapter on your local machine and then
run the application.

```shell
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  --name pgadapter-cloud-run-example \
  --rm -d -p 5432:5432 \
  -v /path/to/credentials.json:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -c /credentials.json -x
  
export SPANNER_PROJECT=my-project
export SPANNER_INSTANCE=my-instance
export SPANNER_DATABASE=my-database
npm install
node index.js
```

This will start a web server on port 8080. Run the following command to verify that it works:

```shell
curl localhost:8080
```

Stop the PGAdapter Docker container again with:

```shell
docker container stop pgadapter-cloud-run-example
```

## Deploying to Cloud Run

First make sure that you have authentication set up for pushing Docker images.

```shell
gcloud auth configure-docker
```

Build the application from source and deploy it to Cloud Run. Replace the generated service
file with the one from this directory. The latter will add PGAdapter as a sidecar container to the
service.

```shell
gcloud run deploy pgadapter-sidecar-example --source .
gcloud run services replace service.yaml
```

__NOTE__: This example does not specify any credentials for PGAdapter when it is run on Cloud Run. This means that
PGAdapter will use the default credentials that is used by Cloud Run. This is by default the default compute engine
service account. See https://cloud.google.com/run/docs/securing/service-identity for more information on how service
accounts work on Google Cloud Run.

Test the service (replace URL with your actual service URL):

```shell
curl https://my-service-xyz.run.app
```

### Authenticated Cloud Run Service

If your Cloud Run service requires authentication, then first add an IAM binding for your own account and include
an authentication header with the request:

```shell
gcloud run services add-iam-policy-binding my-service \
  --member='user:your-email@gmail.com' \
  --role='roles/run.invoker'
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" https://my-service-xyz.run.app
```
