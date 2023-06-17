# PGAdapter Cloud Run Sample for Java

This sample application shows how to build and deploy a Java application with PGAdapter to Google Cloud Run.

The sample is based on the [Cloud Run Quickstart Guide for Java](https://cloud.google.com/run/docs/quickstarts/build-and-deploy/deploy-java-service).
Refer to that guide for more in-depth information on how to work with Cloud Run.

## Configure

Modify the `application.properties` file to point to your Cloud Spanner database:

```shell
spanner.project=my-project
spanner.instance=my-instance
spanner.database=my-database
```

## Build and Run Locally

First test the application locally to verify that the Cloud Spanner project, instance, and database
configuration is correct.

```shell
mvn spring-boot:run
```

This will start a web server on port 8080. Run the following command to verify that it works:

```shell
curl localhost:8080
```

## Deploying to Cloud Run

First make sure that you have authentication set up for pushing Docker images.

```shell
gcloud auth configure-docker
```

Build and deploy the application to Cloud Run with this command:

```shell
gcloud run deploy pgadapter-example
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
