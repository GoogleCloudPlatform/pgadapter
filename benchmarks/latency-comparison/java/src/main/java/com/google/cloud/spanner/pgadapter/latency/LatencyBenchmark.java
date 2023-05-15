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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LatencyBenchmark {
  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("d", "database", true, "The database to use for benchmarking.");
    options.addOption("c", "clients", true, "The number of clients that will be executing queries in parallel.");
    options.addOption("o", "operations", true, "The number of clients that will be executing queries in parallel.");
    options.addOption("e", "embedded", true, "Starts an embedded PGAdapter container along with the benchmark. Setting this option will ignore any host or port settings for PGAdapter.");
    options.addOption("h", "host", true, "The host name where PGAdapter is running. Only used if embedded=false.");
    options.addOption("p", "port", true, "The port number where PGAdapter is running. Only used if embedded=false.");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);
    
    String project = System.getenv("GOOGLE_CLOUD_PROJECT");
    String instance = System.getenv("SPANNER_INSTANCE");
    String database = System.getenv("SPANNER_DATABASE");
    String fullyQualifiedDatabase;
    if (cmd.hasOption('d')) {
      fullyQualifiedDatabase = cmd.getOptionValue('d');
    } else if (project != null && instance != null && database != null) {
      fullyQualifiedDatabase = String.format("projects/%s/instances/%s/databases/%s", project, instance, database);
    } else {
      throw new IllegalArgumentException("You must either set all the environment variables GOOGLE_CLOUD_PROJECT, SPANNER_INSTANCE and SPANNER_DATABASE, or specify a value for the command line argument --database");
    }
    
    LatencyBenchmark benchmark = new LatencyBenchmark(DatabaseId.of(fullyQualifiedDatabase));
    benchmark.run(cmd);
  }
  
  private final DatabaseId databaseId;

  LatencyBenchmark(DatabaseId databaseId) {
    this.databaseId = databaseId;
  }

  public void run(CommandLine commandLine) {
    int clients = commandLine.hasOption('c') ? Integer.parseInt(commandLine.getOptionValue('c')) : 16;
    int operations = commandLine.hasOption('o') ? Integer.parseInt(commandLine.getOptionValue('o')) : 1000;
    
    JavaClientRunner javaClientRunner = new JavaClientRunner(databaseId);
    List<Duration> javaClientResults = javaClientRunner.execute("select col_varchar from latency_test where col_bigint=$1", clients, operations);
    printResults("Java Client Library", javaClientResults);

    JdbcRunner jdbcRunner = new JdbcRunner(databaseId);
    List<Duration> jdbcResults = jdbcRunner.execute("select col_varchar from latency_test where col_bigint=?", clients, operations);
    printResults("Cloud Spanner JDBC Driver", jdbcResults);

    JdbcRunner pgJdbcRunner = new JdbcRunner(databaseId);
    List<Duration> pgJdbcResults = pgJdbcRunner.execute("select col_varchar from latency_test where col_bigint=?", clients, operations);
    printResults("PostgreSQL JDBC Driver", pgJdbcResults);
  }
  
  public void printResults(String header, List<Duration> results) {
    List<Duration> orderedResults = new ArrayList<>(results);
    Collections.sort(orderedResults);
    System.out.println();
    System.out.println(header);
    System.out.printf("Number of queries: %d\n", orderedResults.size());
    System.out.printf(
        "P50: %.2fms\n", orderedResults.get(orderedResults.size() / 2).get(ChronoUnit.NANOS) / 1_000_000.0f);
    System.out.printf(
        "P95: %.2fms\n", orderedResults.get(95 * orderedResults.size() / 100).get(ChronoUnit.NANOS) / 1_000_000.0f);
    System.out.printf(
        "P99: %.2fms\n", orderedResults.get(99 * orderedResults.size() / 100).get(ChronoUnit.NANOS) / 1_000_000.0f);
  }

}
