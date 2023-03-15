# Using PGAdapter on Kubernetes

PGAdapter can be used as a sidecar proxy on Kubernetes. This directory contains a small sample
application that shows how to set this up.

## Creating a GKE cluster

These steps create a GKE cluster where the sample app can be deployed. You can skip these
steps if you already have a GKE cluster, and instead modify your existing deployment to
include PGAdapter.

See https://cloud.google.com/kubernetes-engine/docs/deploy-app-cluster for more details on
how to set up a GKE cluster.

1. Create an Autopilot cluster named example-cluster and update it to use Workload Identity.

```shell
export PROJECT_ID=<YOUR_PROJECT_ID>
export REGION=<YOUR_COMPUTE_REGION>

gcloud config set project $PROJECT_ID
gcloud container clusters create-auto example-cluster \
    --region=$REGION
gcloud container clusters update example-cluster \
    --region=$REGION \
    --workload-pool=$PROJECT_ID.svc.id.goog
```

2. Get authentication credentials for the cluster.

```shell
gcloud container clusters get-credentials example-cluster \
    --region $REGION
```

This command configures `kubectl` to use the cluster you created.

3. Create a repository and build the sample application

```shell
gcloud artifacts repositories create example-repo \
   --repository-format=docker \
   --location=$REGION \
   --description="Example Docker repository"
docker build --platform linux/amd64 \
    -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/example-repo/example-app .
```

4. Authenticate and push the Docker image to Artifact Registry

```shell
gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/example-repo/example-app
```

## Setting up a service account

The first step to running PGAdapter in Kubernetes is creating a Google Cloud IAM
service account (GSA) to represent your application. It is recommended that you create
a service account unique to each application, instead of using the same service
account everywhere. This model is more secure since it allows you to limit
permissions on a per-application basis.

The service account must be granted access to the Cloud Spanner database that the
application will be connecting to. It is enough to assign the `Cloud Spanner Database User`
role for this example to the service account.

```shell
export GSA_NAME=pgadapter-example-gsa
gcloud iam service-accounts create $GSA_NAME \
    --description="PGAdapter Example Service Account" \
    --display-name="PGAdapter Example Service Account"
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/spanner.databaseUser"
```

## Providing the service account to PGAdapter

Next, you need to configure Kubernetes to provide the service account to PGAdapter.
This example uses GKE's [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
for this.  This method allows you to bind a [Kubernetes Service Account (KSA)](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/)
to a Google Service Account (GSA). The GSA will then be accessible to applications
using the matching KSA.

1. Create a Kubernetes Service Account (KSA)

```shell
export KSA_NAME=pgadapter-example-ksa
kubectl create serviceaccount $KSA_NAME --namespace default
```

2. Enable the IAM binding between your `$GSA_NAME` and `$KSA_NAME`:

```sh
gcloud iam service-accounts add-iam-policy-binding \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:${PROJECT_ID}.svc.id.goog[default/$KSA_NAME]" \
    ${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com
```

3. Add an annotation to `$KSA_NAME` to complete the binding:

```sh
kubectl annotate serviceaccount \
    $KSA_NAME \
    iam.gke.io/gcp-service-account=${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com
```

## Deploying the application

1. Deploy the example application and PGAdapter to Kubernetes. The script below replaces the
   placeholders in the deployment manifest with your values.

```shell
export SPANNER_PROJECT_ID=$PROJECT_ID
export SPANNER_INSTANCE_ID=<YOUR_SPANNER_INSTANCE_ID>
export SPANNER_DATABASE_ID=<YOUR_SPANNER_DATABASE_ID>

sed -e 's|<YOUR_KSA_NAME>|'"${KSA_NAME}"'|g' \
    -e 's|<YOUR_REGION>|'"${REGION}"'|g' \
    -e 's|<YOUR_PROJECT_ID>|'"${PROJECT_ID}"'|g' \
    -e 's|<YOUR_SPANNER_PROJECT_ID>|'"${SPANNER_PROJECT_ID}"'|g' \
    -e 's|<YOUR_SPANNER_INSTANCE_ID>|'"${SPANNER_INSTANCE_ID}"'|g' \
    -e 's|<YOUR_SPANNER_DATABASE_ID>|'"${SPANNER_DATABASE_ID}"'|g' \
    manifests/example-app-deployment.yaml > manifests/my-app-deployment.yaml

kubectl apply -f manifests/my-app-deployment.yaml
```

2. Expose the deployment to the Internet

```shell
kubectl expose deployment pgadapter-gke-example \
    --type LoadBalancer --port 80 --target-port 8080
```

## Inspect and view the application

1. Inspect the running Pods by using `kubectl get pods`

```shell
kubectl get pods
```

2. Inspect the pgadapter-gke-example Service by using `kubectl get service`

```shell
kubectl get service pgadapter-gke-example
```

From this command's output, copy the Service's external IP address from the EXTERNAL-IP column.

3. View the output of the application

Use the EXTERNAL-IP address from the previous command.

```shell
curl http://<EXTERNAL-IP>
```
