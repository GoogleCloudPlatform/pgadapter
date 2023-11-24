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


```shell
mvn spring-boot:run -Dspring-boot.run.arguments="
  --tpcc.benchmark-threads=8
  --tpcc.use-read-only-transactions=true
  --spanner.project=appdev-soda-spanner-staging
  --spanner.instance=knut-test-ycsb
  --spanner.database=tpcc
  --pgadapter.credentials=/home/loite/appdev-soda-spanner-staging.json
  "
```