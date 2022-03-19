// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.benchmarks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, warmups = 0)
@Measurement(batchSize = 1, iterations = 1, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(batchSize = 0, iterations = 0)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PgAdapterJdbcExtendedModeBenchmark {

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
   */
  private String createUrl(PgAdapterBenchmarkState server) {
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferQueryMode=simple", server.pgServer.getLocalPort());
  }

  @Benchmark
  public void testSelect1(PgAdapterBenchmarkState server) throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl(server))) {
      try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Benchmark
  public void testSelect2(PgAdapterBenchmarkState server) throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl(server))) {
      try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(PgAdapterJdbcExtendedModeBenchmark.class.getSimpleName())
        .forks(1)
        .jvmArgs("-ea")
        .build();

    new Runner(opt).run();
  }

}
