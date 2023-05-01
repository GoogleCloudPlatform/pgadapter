# PGAdapter Cloud Run Sample for Go

This sample application shows how to build and deploy a Go application with PGAdapter to Google Cloud Run.

## Build

First build the Docker image locally and tag it. Replace the `IMAGE_URL` in the script with your own repository.
Make sure that the repository that you are referencing exists.
See https://cloud.google.com/artifact-registry/docs/repositories/create-repos for more information on how to set up
Google Cloud artifact repositories.

```shell
export IMAGE_URL=my-location-docker.pkg.dev/my-project/my-repository/my-image:latest
docker build . --tag $IMAGE_URL
```

Example: `export IMAGE_URL=europe-north1-docker.pkg.dev/my-project/sample-test/pgadapter-sample:latest`

Test the Docker image locally:

```shell
docker run \
  -p 8080:8080 \
  -v /path/to/local_credentials.json:/credentials.json:ro \
  --env GOOGLE_APPLICATION_CREDENTIALS=/credentials.json \
  --env SPANNER_PROJECT=my-project \
  --env SPANNER_INSTANCE=my-instance \
  --env SPANNER_DATABASE=my-database \
  $IMAGE_URL
```

Verify that the connection to Cloud Spanner works correctly by opening a new shell and executing:

```shell
curl http://localhost:8080
```

## Deploying to Cloud Run

Push the Docker image to Artifact Registry:

```shell
gcloud auth configure-docker
docker push $IMAGE_URL
```

Then deploy the image as a service on Cloud Run:

```shell
export SERVICE=my-service
gcloud run deploy $SERVICE \
  --image $IMAGE_URL \
  --update-env-vars SPANNER_PROJECT=my-project,SPANNER_INSTANCE=my-instance,SPANNER_DATABASE=my-database
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

