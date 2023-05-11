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

public class LatencyBenchmark {
  private static final String PROJECT_ID = "cloud-spanner-pg-adapter";
  private static final String INSTANCE_ID = "pgadapter-ycsb-regional-test";
  private static final String DATABASE_ID = "latency-test";
  
  public static void main(String[] args) {
    LatencyBenchmark benchmark = new LatencyBenchmark();
    benchmark.run();
  }

  public void run() {
    DatabaseId databaseId = DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID);
    
    int numExecutions = 100;
    
    JavaClientRunner javaClientRunner = new JavaClientRunner(databaseId);
    List<Duration> javaClientResults = javaClientRunner.execute("select col_varchar from latency_test where col_bigint=$1", numExecutions);
    printResults("Java Client Library", javaClientResults);

    JdbcRunner jdbcRunner = new JdbcRunner(databaseId);
    List<Duration> jdbcResults = jdbcRunner.execute("select col_varchar from latency_test where col_bigint=?", numExecutions);
    printResults("Cloud Spanner JDBC Driver", jdbcResults);

    JdbcRunner pgJdbcRunner = new JdbcRunner(databaseId);
    List<Duration> pgJdbcResults = pgJdbcRunner.execute("select col_varchar from latency_test where col_bigint=?", numExecutions);
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
