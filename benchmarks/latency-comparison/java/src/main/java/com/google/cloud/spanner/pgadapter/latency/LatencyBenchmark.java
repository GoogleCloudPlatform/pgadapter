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

import com.google.cloud.spanner.DatabaseId;
import java.time.Duration;
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
        "The number of operations that will be executed by each client.");
    options.addOption(
        "t",
        "transactions",
        true,
        "The number of transactions that will be executed by each client.");
    options.addOption(
        "q",
        "queries",
        true,
        "The number of queries that will be executed in each transaction.");
    options.addOption(
        "u",
        "updates",
        true,
        "The number of updates that will be executed in each transaction.");
    options.addOption(
        null,
        "delayBeginTransaction",
        false,
        "Enables the 'Delay transaction start until first write' option.");
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
    benchmark.run(cmd);
  }

  private final DatabaseId databaseId;

  LatencyBenchmark(DatabaseId databaseId) {
    this.databaseId = databaseId;
  }

  public void run(CommandLine commandLine) {
    int clients =
        commandLine.hasOption('c') ? Integer.parseInt(commandLine.getOptionValue('c')) : 16;
    int operations =
        commandLine.hasOption('o') ? Integer.parseInt(commandLine.getOptionValue('o')) : 1000;
    int transactions =
        commandLine.hasOption('t') ? Integer.parseInt(commandLine.getOptionValue('t')) : 100;
    int queriesInTransaction = commandLine.hasOption('q') ? Integer.parseInt(commandLine.getOptionValue('q')) : 10;
    int updatesInTransaction = commandLine.hasOption('u') ? Integer.parseInt(commandLine.getOptionValue('u')) : 5;
    boolean delayBeginTransaction = commandLine.hasOption("delayBeginTransaction");

    System.out.println();
    System.out.println("Running benchmark with the following options");
    System.out.printf("Database: %s\n", databaseId);
    System.out.printf("Clients: %d\n", clients);
    System.out.printf("Operations: %d\n", operations);
    System.out.printf("Transactions: %d\n", transactions);
    System.out.printf("Queries in transaction: %d\n", queriesInTransaction);
    System.out.printf("Updates in transaction: %d\n", updatesInTransaction);
    System.out.printf("Delay begin transaction: %s\n", delayBeginTransaction);
    
    String jdbcQuery = "select col_varchar from latency_test where col_bigint=?";
    String jdbcUpdate = "update latency_test set col_varchar=? where col_bigint=?";

//    System.out.println();
//    System.out.println("Running benchmark for PostgreSQL JDBC driver");
//    PgJdbcRunner pgJdbcRunner = new PgJdbcRunner(databaseId, delayBeginTransaction);
//    List<Duration> pgJdbcResults = pgJdbcRunner.execute(jdbcQuery, clients, operations);
//    List<Duration> pgJdbcTransactionResults = pgJdbcRunner.executeTransaction(jdbcQuery, jdbcUpdate, clients, transactions, queriesInTransaction, updatesInTransaction);
//    pgJdbcRunner.shutdown();

    System.out.println();
    System.out.println("Running benchmark for Cloud Spanner JDBC driver");
    JdbcRunner jdbcRunner = new JdbcRunner(databaseId, delayBeginTransaction);
    List<Duration> jdbcResults =
        jdbcRunner.execute(
            "select col_varchar from latency_test where col_bigint=?", clients, operations);
    List<Duration> jdbcTransactionResults = jdbcRunner.executeTransaction(jdbcQuery, jdbcUpdate, clients, transactions, queriesInTransaction, updatesInTransaction);
    jdbcRunner.shutdown();

//    System.out.println();
//    System.out.println("Running benchmark for Java Client Library");
//    JavaClientRunner javaClientRunner = new JavaClientRunner(databaseId);
//    List<Duration> javaClientResults =
//        javaClientRunner.execute(
//            "select col_varchar from latency_test where col_bigint=$1", clients, operations);
//    javaClientRunner.shutdown();

//    printResults("PostgreSQL JDBC Driver", pgJdbcResults);
    printResults("Cloud Spanner JDBC Driver", jdbcResults);
//    printResults("Java Client Library", javaClientResults);
    
//    printTransactionResults("PostgreSQL JDBC Driver - Transactions", pgJdbcTransactionResults, queriesInTransaction, updatesInTransaction);
    printTransactionResults("Cloud Spanner JDBC Driver - Transactions", jdbcTransactionResults, queriesInTransaction, updatesInTransaction);
  }

  public void printResults(String header, List<Duration> results) {
    System.out.println();
    System.out.println(header);
    System.out.printf("Total number of queries: %d\n", results.size());
    printDurations(results);
  }

  public void printTransactionResults(String header, List<Duration> results, int queriesInTransaction, int updatesInTransaction) {
    System.out.println();
    System.out.println(header);
    System.out.printf("Total number of transactions: %d\n", results.size());
    System.out.printf("Reads per transaction: %d\n", queriesInTransaction);
    System.out.printf("Writes per transaction: %d\n", updatesInTransaction);
    printDurations(results);
  }
  
  private void printDurations(List<Duration> results) {
    List<Duration> orderedResults = new ArrayList<>(results);
    Collections.sort(orderedResults);
    System.out.printf("Avg: %.2fms\n", avg(results));
    System.out.printf(
        "P50: %.2fms\n",
        orderedResults.get(orderedResults.size() / 2).toNanos() / 1_000_000.0f);
    System.out.printf(
        "P95: %.2fms\n",
        orderedResults.get(95 * orderedResults.size() / 100).toNanos() / 1_000_000.0f);
    System.out.printf(
        "P99: %.2fms\n",
        orderedResults.get(99 * orderedResults.size() / 100).toNanos() / 1_000_000.0f);
  }

  private double avg(List<Duration> results) {
    return results.stream()
        .collect(Collectors.averagingDouble(result -> result.toNanos() / 1_000_000.0f));
  }
}
