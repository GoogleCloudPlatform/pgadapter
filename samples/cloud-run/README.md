# PGAdapter Cloud Run Sidecar

This directory contains samples for deploying an application to Cloud Run with PGAdapter as a
[sidecar container](https://cloud.google.com/run/docs/deploying#sidecars). This is the recommended
setup on Cloud Run for PGAdapter. Take a look at the concrete examples in the subdirectories for a
working example in a specific programming language:
- [Java JDBC](./java)
- [Go pgx](./golang)
- [Node.js node-postgres](./nodejs)
- [Python psycopg3](./python)
- [.NET C# npgsql](./dotnet)
- [Ruby ruby-pg](./ruby)

The main application and PGAdapter communicate over a Unix domain socket using an
[in-memory shared volume](https://cloud.google.com/run/docs/configuring/in-memory-volumes).
This guarantees the lowest possible latency between your application and PGAdapter.

The [`service.yaml`](./service.yaml) file in this directory contains an example setup for a Cloud
Run application that uses PGAdapter as a sidecar proxy. The following sections explains the
different parts of the configuration that is used to set up PGAdapter as a sidecar.

## Sidecar Configuration

PGAdapter is added to the service file as a separate container:

```yaml
spec:
  template:
    # ...
    spec:
      # ...
      containers:
        # This is the PGAdapter sidecar container.
        - name: pgadapter
          image: gcr.io/cloud-spanner-pg-adapter/pgadapter
          volumeMounts:
            - mountPath: /sockets
              name: sockets-dir
          args:
            - -dir /sockets
            - -x
```

This container specification adds the pre-built PGAdapter Docker image
`gcr.io/cloud-spanner-pg-adapter/pgadapter` as a sidecar container to the setup. Cloud Run
recognizes the container as a sidecar, because it does not specify a `containerPort`. Only the main
container of a Cloud Run service may specify a `containerPort`.

The configuration also adds a volume that is used for Unix domain sockets.
See [Unix Domain Sockets](#unix-domain-sockets) for more information on this.

PGAdapter is configured with two command line arguments in this example:
1. `-dir /sockets`: This instructs PGAdapter to use the directory `/sockets` for setting up a
   listener for incoming Unix Domain Socket connection requests.
2. `-x`: This instructs PGAdapter to accept TCP connection requests from other hosts than localhost.
   This is necessary for the TCP prober in the service file. _NOTE_: This will not make PGAdapter
   accessible from the Internet. Cloud Run only allows ingress traffic to the main container.

## Unix Domain Sockets

The example Cloud Run service sets up a shared in-memory volume that is used for Unix Domain Sockets
connections between the main application and PGAdapter. Unix Domain Socket connections have a lower
latency than TCP connections. The configuration that is used for this is listed below.

```yaml
spec:
  template:
     # ...
    spec:
      # Create an in-memory volume that can be used for Unix domain sockets.
      volumes:
        - name: sockets-dir
          emptyDir:
            sizeLimit: 50Mi
            medium: Memory
      containers:
        # This is the main application container.
        - name: app
          # ...
          volumeMounts:
            - mountPath: /sockets
              name: sockets-dir
        # This is the PGAdapter sidecar container.
        - name: pgadapter
          # ...
          volumeMounts:
            - mountPath: /sockets
              name: sockets-dir
          args:
            - -dir /sockets
```

This configuration sets up a shared in-memory volume with the name `sockets-dir` with a maximum size
of 50MiB. The volume is mapped to both the main application container and to the PGAdapter sidecar
container, in both cases as `/sockets`.  The PGAdapter sidecar container is started with the command
line argument `-dir /sockets` to listen for incoming Unix domain socket connections on this shared
volume.

## Startup Probe

The `service.yaml` file defines the order in which the containers must start and a startup probe
for PGAdapter. The startup probe verifies that it can establish a TCP connection with port 5432 on
localhost. Note that this does not mean that PGAdapter is accessible from the Internet. Ingress
network traffic is only possible for the main application container.

```yaml
spec:
  template:
    metadata:
      annotations:
        # This registers 'pgadapter' as a dependency of 'app' and will ensure that pgadapter starts
        # before the app container.
        run.googleapis.com/container-dependencies: '{"app":["pgadapter"]}'
    spec:
      containers:
        # ...
        - name: pgadapter
          # ...
          # Add a startup probe that checks that PGAdapter is listening on port 5432.
          startupProbe:
            initialDelaySeconds: 10
            timeoutSeconds: 10
            periodSeconds: 10
            failureThreshold: 3
            tcpSocket:
              port: 5432
```

The TCP probe will cause PGAdapter to log an EOF warning. This warning can be ignored. The
warning is caused by the TCP probe, which will open a TCP connection to PGAdapter, but not send a
PostgreSQL startup message, and instead just close the connection.
