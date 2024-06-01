# Latency Comparison - PostgreSQL JDBC driver vs Cloud Spanner JDBC driver

This benchmark tests the latency of executing a simple, single-row query using the PostgreSQL
JDBC driver with PGAdapter compared to executing the same query using the Cloud Spanner JDBC driver.
It also executes the same query using the native Cloud Spanner Java client library.

## Setup

You must first create a database with a test table before you can run this benchmark.
See [the setup instructions here](../README.md#setup-test-database) for how to do that.

Also make sure that you have set the following environment variables:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export GOOGLE_CLOUD_PROJECT=my-project
export SPANNER_INSTANCE=my-instance
export SPANNER_DATABASE=my-database
```

## Running

The benchmark application includes PGAdapter as a dependency and automatically starts PGAdapter as
an in-process service.

```shell
mvn clean compile exec:java -Dexec.args="--clients=16 --operations=1000"
```

## Arguments

The benchmark application accepts the following command line arguments:
* --database: The fully qualified database name to use for the benchmark. Defaults to `projects/$GOOGLE_CLOUD_PROJECT/instances/$SPANNER_INSTANCE/databases/$SPANNER_DATABASE`.
* --clients: The number of parallel clients that execute queries. Defaults to 16.
* --operations: The number of operations (queries) that each client executes. Defaults to 1,000.
* --transaction: The type of transaction to execute. Must be either READ_ONLY or READ_WRITE. Defaults to READ_ONLY.
* --wait: The wait time in milliseconds between each operation. Defaults to 0.
* --warmup: The number of warmup iterations to run before executing the actual benchmark. Java uses a
  Just-in-Time compiler that compiles and optimizes code based on the actual usage pattern. Running a
  warmup script before the actual benchmark is therefore recommended to get results that are comparable
  to an actual application that runs for a longer period of time. Defaults to 12,000 per CPU core.

## Examples

Run a benchmark with 32 parallel clients each executing 5,000 operations:

```shell
mvn clean compile exec:java -Dexec.args="--clients=32 --operations=5000"
```


Run a benchmark with 32 parallel clients each executing 1 query per second.
Each client executes 100 queries:

```shell
mvn clean compile exec:java -Dexec.args="--clients=32 --operations=100 --wait 1000"
```


Run a benchmark with 32 parallel clients executing read/write transactions.
Each client executes 1,000 transactions:

```shell
mvn clean compile exec:java -Dexec.args="--clients=32 --operations=1000 --transaction=read_write"
```
