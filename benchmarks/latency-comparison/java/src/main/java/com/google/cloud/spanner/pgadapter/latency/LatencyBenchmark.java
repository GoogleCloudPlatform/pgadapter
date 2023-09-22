// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.latency;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LatencyBenchmark {
  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("d", "database", true, "The database to use for benchmarking.");
    options.addOption(
        "c", "clients", true, "The number of clients that will be executing queries in parallel.");
    options.addOption(
        "o",
        "operations",
        true,
        "The number of operations that each client will execute. Defaults to 1000.");
    options.addOption("skip_pg", false, "Skip PostgreSQL JDBC benchmarks.");
    options.addOption("skip_jdbc", false, "Skip Cloud Spanner JDBC benchmarks.");
    options.addOption("skip_spanner", false, "Skip Cloud Spanner client library benchmarks.");
    options.addOption(
        "create_results_table",
        false,
        "Create the results table in the test database if it does not exist.");
    options.addOption("s", "store_results", false, "Store results in the test database.");
    options.addOption("name", true, "Name of this test run");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    String project = System.getenv("GOOGLE_CLOUD_PROJECT");
    String instance = System.getenv("SPANNER_INSTANCE");
    String database = System.getenv("SPANNER_DATABASE");
    String fullyQualifiedDatabase;
    if (cmd.hasOption('d')) {
      fullyQualifiedDatabase = cmd.getOptionValue('d');
    } else if (project != null && instance != null && database != null) {
      fullyQualifiedDatabase =
          String.format("projects/%s/instances/%s/databases/%s", project, instance, database);
    } else {
      throw new IllegalArgumentException(
          "You must either set all the environment variables GOOGLE_CLOUD_PROJECT, SPANNER_INSTANCE and SPANNER_DATABASE, or specify a value for the command line argument --database");
    }

    LatencyBenchmark benchmark = new LatencyBenchmark(DatabaseId.of(fullyQualifiedDatabase));
    if (cmd.hasOption("create_results_table")) {
      benchmark.createResultsTableIfNotExists();
    }
    benchmark.run(cmd);
  }

  private final DatabaseId databaseId;

  LatencyBenchmark(DatabaseId databaseId) {
    this.databaseId = databaseId;
  }

  public void run(CommandLine commandLine) {
    Instant runTime = Instant.now();
    int clients =
        commandLine.hasOption('c') ? Integer.parseInt(commandLine.getOptionValue('c')) : 16;
    int operations =
        commandLine.hasOption('o') ? Integer.parseInt(commandLine.getOptionValue('o')) : 1000;
    boolean runPg = !commandLine.hasOption("skip_pg");
    boolean runJdbc = !commandLine.hasOption("skip_jdbc");
    boolean runSpanner = !commandLine.hasOption("skip_spanner");
    String name = commandLine.getOptionValue("name");

    System.out.println();
    System.out.println("Running benchmark with the following options");
    System.out.printf("Database: %s\n", databaseId);
    System.out.printf("Clients: %d\n", clients);
    System.out.printf("Operations: %d\n", operations);

    List<Duration> pgJdbcResults = null;
    if (runPg) {
      System.out.println();
      System.out.println("Running benchmark for PostgreSQL JDBC driver");
      PgJdbcRunner pgJdbcRunner = new PgJdbcRunner(databaseId);
      pgJdbcResults =
          pgJdbcRunner.execute(
              "select col_varchar from latency_test where col_bigint=?", clients, operations);
    }

    List<Duration> jdbcResults = null;
    if (runJdbc) {
      System.out.println();
      System.out.println("Running benchmark for Cloud Spanner JDBC driver");
      JdbcRunner jdbcRunner = new JdbcRunner(databaseId);
      jdbcResults =
          jdbcRunner.execute(
              "select col_varchar from latency_test where col_bigint=?", clients, operations);
    }

    List<Duration> javaClientResults = null;
    if (runSpanner) {
      System.out.println();
      System.out.println("Running benchmark for Java Client Library");
      JavaClientRunner javaClientRunner = new JavaClientRunner(databaseId);
      javaClientResults =
          javaClientRunner.execute(
              "select col_varchar from latency_test where col_bigint=$1", clients, operations);
    }

    printResults("PostgreSQL JDBC Driver", pgJdbcResults);
    printResults("Cloud Spanner JDBC Driver", jdbcResults);
    printResults("Java Client Library", javaClientResults);

    if (commandLine.hasOption('s')) {
      saveResults(
          runTime, name, clients, operations, pgJdbcResults, jdbcResults, javaClientResults);
    }
  }

  private void printResults(String header, List<Duration> results) {
    if (results == null) {
      return;
    }
    List<Duration> orderedResults = new ArrayList<>(results);
    Collections.sort(orderedResults);
    System.out.println();
    System.out.println(header);
    System.out.printf("Total number of queries: %d\n", orderedResults.size());
    System.out.printf("Avg: %.2fms\n", avg(results));
    System.out.printf("P50: %.2fms\n", percentile(50, orderedResults));
    System.out.printf("P95: %.2fms\n", percentile(95, orderedResults));
    System.out.printf("P99: %.2fms\n", percentile(99, orderedResults));
  }

  private void saveResults(
      Instant runTime,
      String name,
      int clients,
      int operations,
      List<Duration> pgResults,
      List<Duration> jdbcResults,
      List<Duration> javaResults) {
    System.out.println("Saving results to the database");
    Mutation.WriteBuilder builder =
        Mutation.newInsertBuilder("latency_test_results")
            .set("run_time")
            .to(runTime.toString())
            .set("name")
            .to(name)
            .set("clients")
            .to(clients)
            .set("operations")
            .to(operations);
    builder = addResults("pg", pgResults, builder);
    builder = addResults("jdbc", jdbcResults, builder);
    builder = addResults("java", javaResults, builder);

    try (Spanner spanner = createSpanner()) {
      DatabaseClient client = spanner.getDatabaseClient(databaseId);
      client.write(ImmutableList.of(builder.build()));
    }
  }

  private Mutation.WriteBuilder addResults(
      String prefix, List<Duration> results, Mutation.WriteBuilder builder) {
    if (results == null) {
      return builder;
    }
    return builder
        .set(prefix + "_avg")
        .to(avg(results))
        .set(prefix + "_p50")
        .to(percentile(50, results))
        .set(prefix + "_p95")
        .to(percentile(95, results))
        .set(prefix + "_p99")
        .to(percentile(99, results));
  }

  private double percentile(int percentile, List<Duration> orderedResults) {
    return orderedResults.get(percentile * orderedResults.size() / 100).get(ChronoUnit.NANOS)
        / 1_000_000.0f;
  }

  private double avg(List<Duration> results) {
    return results.stream()
        .collect(Collectors.averagingDouble(result -> result.get(ChronoUnit.NANOS) / 1_000_000.0f));
  }

  private Spanner createSpanner() {
    return SpannerOptions.newBuilder()
        .setProjectId(databaseId.getInstanceId().getProject())
        .setSessionPoolOption(
            SessionPoolOptions.newBuilder().setMinSessions(1).setMaxSessions(10).build())
        .build()
        .getService();
  }

  private void createResultsTableIfNotExists() {
    System.out.println("Creating results table if it does not already exist");
    try (Spanner spanner = createSpanner()) {
      DatabaseAdminClient client = spanner.getDatabaseAdminClient();
      client
          .updateDatabaseDdl(
              databaseId.getInstanceId().getInstance(),
              databaseId.getDatabase(),
              ImmutableList.of(
                  "create table if not exists latency_test_results (\n"
                      + "run_time varchar primary key,\n"
                      + "name varchar,\n"
                      + "clients bigint,\n"
                      + "operations bigint,\n"
                      + "pg_avg float8,\n"
                      + "pg_p50 float8,\n"
                      + "pg_p95 float8,\n"
                      + "pg_p99 float8,\n"
                      + "jdbc_avg float8,\n"
                      + "jdbc_p50 float8,\n"
                      + "jdbc_p95 float8,\n"
                      + "jdbc_p99 float8,\n"
                      + "java_avg float8,\n"
                      + "java_p50 float8,\n"
                      + "java_p95 float8,\n"
                      + "java_p99 float8\n"
                      + ")"),
              null)
          .get();
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }
}
