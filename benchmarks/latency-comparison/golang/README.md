# Latency Comparison - pgx driver vs Cloud Spanner Go Client Library

This benchmark tests the latency of executing a simple, single-row query using the PostgreSQL
`pgx` driver with PGAdapter compared to executing the same query using the [native Cloud Spanner
Go client library](https://pkg.go.dev/cloud.google.com/go/spanner).

## Setup

You must first create a database with a test table before you can run this benchmark.
See [the setup instructions here](../README.md#setup) for how to do that.

## Run directly the host machine

You can run the benchmark as a native Go application directly on your host machine if you have Go
and Java installed on your system. The benchmark will automatically start PGAdapter in an embedded Docker container.

```shell
go build benchmark.go
./benchmark
```

## Run in Docker

You can also run the benchmark in a Docker container if you do not have Go installed on your
system.

```
docker build . --tag benchmark

```

## Arguments

The benchmark application accepts the following command line arguments:
* database: The fully qualified database name to use for the benchmark. Defaults to `projects/$GOOGLE_CLOUD_PROJECT/instances/$SPANNER_INSTANCE/databases/$SPANNER_DATABASE`.
* clients: The number of parallel clients that execute queries. Defaults to 16.
* operations: The number of operations (queries) that each client executes. Defaults to 1,000.
* embedded (true/false): Whether to start PGAdapter as an embedded test container together with the
  benchmark application. Defaults to true. Set to false if you want to start and configure PGAdapter
  manually.
* host: The host name where PGAdapter runs. This argument is only used if `embedded=false`.
* port: The port number where PGAdapter runs. This argument is only used if `embedded=false`.
* uds (true/false): Also execute benchmarks using Unix Domain Sockets. Defaults to false.
* dir: The directory where PGAdapter listens for Unix Domain Socket connections. Defaults to `/tmp`. This argument is only used if `embedded=false`.
* udsport: The port number where PGAdapter listens for Unix Domain Socket connections. Defaults to
  the same port number as for TCP.  This argument is only used if `embedded=false`.

## Examples

Run a benchmark with 32 parallel clients each executing 5,000 operations:

```shell
./benchmark -clients=32 -operations=5000
```