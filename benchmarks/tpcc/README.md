# TPC-C Benchmark Application for PGAdapter and Cloud Spanner

This application implements the standard TPC-C benchmark and runs it against PGAdapter and a Cloud
Spanner database. It is a Java Spring Boot application. See [src/main/resources/application.properties](src/main/resources/application.properties)
for a full list of configuration options.

## Running

The application automatically starts PGAdapter together with the benchmark application. Supply the
Cloud Spanner database and your credentials using the below command. The `tpcc.benchmark-threads`
argument determines the number of threads that will execute test transactions in parallel.

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
   --tpcc.benchmark-threads=8
   --spanner.project=my-project
   --spanner.instance=my-instance
   --spanner.database=my-database
   --pgadapter.credentials=/path/to/credentials.json
   "
```

### Load data

Load data into a PostgreSQL-dialect DB:

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-duration=PT600s
  --tpcc.warehouses=10
  --tpcc.benchmark-threads=1
  --tpcc.load-data=true
  --tpcc.truncate-before-load=false
  --tpcc.run-benchmark=false
  --tpcc.use-read-only-transactions=false
  --tpcc.lock-scanned-ranges=false
  --spanner.project=my-project
  --spanner.instance=my-instance
  --spanner.database=my-database
  --pgadapter.credentials=/path/to/credentials.json
  "
```

To load data into a GoogleSQL-dialect DB, you need to specify `client_lib_gsql` as the `benchmark-runner`. This instructs the benchmark application that it should use the Spanner Java client to connect to the database, and that it should use the GoogleSQL dialect:

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-duration=PT600s
  --tpcc.warehouses=10
  --tpcc.benchmark-threads=1
  --tpcc.load-data=true
  --tpcc.truncate-before-load=false
  --tpcc.run-benchmark=false
  --tpcc.benchmark-runner=client_lib_gsql
  --tpcc.use-read-only-transactions=false
  --tpcc.lock-scanned-ranges=false
  --spanner.project=my-project
  --spanner.instance=my-instance
  --spanner.database=my-database
  --pgadapter.credentials=/path/to/credentials.json
  "
```

### Run benchmark

Currently, we support the following benchmark runners: `pgadapter`, `spanner_jdbc`, `client_lib_pg`, and `client_lib_gsql`.

Run with the default benchmark runner (PGAdapter with PG JDBC):

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-duration=PT600s
  --tpcc.warehouses=10
  --tpcc.benchmark-threads=1
  --tpcc.load-data=false
  --tpcc.truncate-before-load=false
  --tpcc.run-benchmark=true
  --tpcc.benchmark-runner=pgadapter
  --tpcc.use-read-only-transactions=true
  --tpcc.lock-scanned-ranges=false
  --spanner.project=my-project
  --spanner.instance=my-instance
  --spanner.database=my-database
  --pgadapter.credentials=/path/to/credentials.json
  "
```

Run with the benchmark runner (Spanner JDBC):

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-duration=PT600s
  --tpcc.warehouses=10
  --tpcc.benchmark-threads=1
  --tpcc.load-data=false
  --tpcc.truncate-before-load=false
  --tpcc.run-benchmark=true
  --tpcc.benchmark-runner=spanner_jdbc
  --tpcc.use-read-only-transactions=true
  --tpcc.lock-scanned-ranges=false
  --spanner.project=my-project
  --spanner.instance=my-instance
  --spanner.database=my-database
  --pgadapter.credentials=/path/to/credentials.json
  "
```

Run with the benchmark runner (Client library with PostgreSQL dialect):

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-duration=PT600s
  --tpcc.warehouses=10
  --tpcc.benchmark-threads=1
  --tpcc.load-data=false
  --tpcc.truncate-before-load=false
  --tpcc.run-benchmark=true
  --tpcc.benchmark-runner=client_lib_pg
  --tpcc.use-read-only-transactions=true
  --tpcc.lock-scanned-ranges=false
  --spanner.project=my-project
  --spanner.instance=my-instance
  --spanner.database=my-database
  --pgadapter.credentials=/path/to/credentials.json
  "
```

Run with the benchmark runner (Client library with GoogleSQL dialect):

```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-duration=PT600s
  --tpcc.warehouses=10
  --tpcc.benchmark-threads=1
  --tpcc.load-data=false
  --tpcc.truncate-before-load=false
  --tpcc.run-benchmark=true
  --tpcc.benchmark-runner=client_lib_gsql
  --tpcc.use-read-only-transactions=true
  --tpcc.lock-scanned-ranges=false
  --spanner.project=my-project
  --spanner.instance=my-instance
  --spanner.database=my-database
  --pgadapter.credentials=/path/to/credentials.json
  "
```
