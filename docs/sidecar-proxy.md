# Google Cloud Spanner PGAdapter - Sidecar Proxy

PGAdapter can be used as a sidecar proxy in for example a Kubernetes cluster. Kubernetes sidecar
containers are those containers that run parallel with the main container in the pod.

Running PGAdapter in a "sidecar" pattern is recommend over running as a separate service for several reasons:

* Prevents a single point of failure - each application's access to your database is independent
  of the others, making it more resilient.
* Allows you to scope resource requests more accurately - because PGAdapter consumes resources
  linearly to usage, this pattern allows you to more accurately scope and request resources to match
  your applications as it scales.

## Configuration

Add PGAdapter to the pod configuration under `containers`:

```yaml
  containers:
  - name: pgadapter
    image: gcr.io/cloud-spanner-pg-adapter/pgadapter
    ports:
      - containerPort: 5432
    args:
      - "-p my-project"
      - "-i my-instance"
      - "-d my-database"
      - "-x"
    resources:
      requests:
        # PGAdapter's memory use scales linearly with the number of active
        # connections. Fewer open connections will use less memory. Adjust
        # this value based on your application's requirements.
        # A minimum of 384MiB + 2MiB per open connection is recommended.
        memory: "512Mi"
        # PGAdapter's CPU use scales linearly with the amount of IO between
        # the database and the application. Adjust this value based on your
        # application's requirements.
        cpu: "1"
```

[This sample application](../samples/sidecar-proxy) contains a step-by-step guide for how to set up
PGAdapter as a sidecar proxy in a Google Kubernetes cluster. This example also includes setting up
a Workload Identity with a Kubernetes Service Account.
