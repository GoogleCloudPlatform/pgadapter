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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.metadata.TestOptionsMetadataBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BenchmarkMockServerTest extends AbstractMockServerTest {
  static {
    try {
      LogManager.getLogManager().readConfiguration(BenchmarkMockServerTest.class.getResourceAsStream("/logging.properties"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class BenchmarkResult {
    final String name;
    final int parallelism;
    final boolean disableVirtualThreads;
    final Duration avg;
    final Duration p50;
    final Duration p90;
    final Duration p95;
    final Duration p99;

    BenchmarkResult(String name, int parallelism, boolean disableVirtualThreads, ConcurrentLinkedQueue<Duration> durations) {
      this.name = name;
      this.parallelism = parallelism;
      this.disableVirtualThreads = disableVirtualThreads;
      List<Duration> list = new ArrayList<>(durations);
      list.sort(Duration::compareTo);
      avg = list.stream().reduce(Duration::plus).orElse(Duration.ZERO).dividedBy(list.size());
      p50 = list.get(durations.size() / 2);
      p90 = list.get(durations.size() * 90 / 100);
      p95 = list.get(durations.size() * 95 / 100);
      p99 = list.get(durations.size() * 99 / 100);
    }

    @Override
    public String toString() {
      return String.format("name: %s\nparallelism: %d\nvirtual: %s\navg: %s\np50: %s\np90: %s\np95: %s\np99: %s\n\n", name, parallelism, !disableVirtualThreads, avg, p50, p90, p95, p99);
    }
  }

  @Parameter(0)
  public boolean disableVirtualThreads;

  @Parameter(1)
  public int parallelism;

  @Parameters(name = "disableVirtualThreads = {0}, parallelism = {1}")
  public static List<Object[]> parameters() {
    ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
    for (int parallelism : new int[] {1, 2, 4, 8, 16, 32, 50, 100, 150, 200, 300, 400}) {
      for (boolean disableVirtualThreads : new boolean[] {false, true}) {
        builder.add(new Object[]{disableVirtualThreads, parallelism});
      }
    }
    return builder.build();
  }

  private static final int NUM_ITERATIONS = 10000;

  private static final int NUM_RESULTS = 1000;

  private static final List<String> IDENTIFIERS = new ArrayList<>(NUM_RESULTS);

  private static final String SELECT_SINGLE_ROW_SQL = "select * from random where id=?";


  @BeforeClass
  public static void setupBenchmarkServer() throws Exception {
    assumeTrue(System.getProperty("pgadapter.benchmark") == null);

    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        "d",
        TestOptionsMetadataBuilder::disableDebugMode,
        OpenTelemetry.noop());

    mockSpanner.setExecuteStreamingSqlExecutionTime(SimulatedExecutionTime.ofMinimumAndRandomTime(3, 3));
    setupResults();
  }

  private static void setupResults() {
    String spannerSql = SELECT_SINGLE_ROW_SQL.replace("?", "$1");
    RandomResultSetGenerator generator = new RandomResultSetGenerator(1, Dialect.POSTGRESQL);

    for (int i=0; i < NUM_RESULTS; i++) {
      String id = UUID.randomUUID().toString();
      Statement spannerStatement = Statement.newBuilder(spannerSql).bind("p1").to(id).build();
      mockSpanner.putStatementResult(StatementResult.query(spannerStatement, generator.generate()));
      IDENTIFIERS.add(id);
    }
  }

  @Before
  public void restartServer() {
    pgServer.stopServer();
    pgServer.awaitTerminated();

    mockSpanner.clearRequests();

    if (disableVirtualThreads) {
      System.setProperty("pgadapter.disable_virtual_threads", "true");
    } else {
      System.clearProperty("pgadapter.disable_virtual_threads");
    }
    pgServer = new ProxyServer(pgServer.getOptions(), pgServer.getOpenTelemetry());
    pgServer.startServer();
    pgServer.awaitRunning();
  }

  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/db",
        pgServer.getLocalPort());
  }

  @Test
  public void testSelectOneRowAutoCommit() throws Exception {
    ConcurrentLinkedQueue<Duration> durations = new ConcurrentLinkedQueue<>();

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    for (int task = 0; task < parallelism; task++) {
      executor.submit(() -> runQuery(NUM_ITERATIONS, durations));
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(1L, TimeUnit.HOURS));
    assertEquals(parallelism * NUM_ITERATIONS, durations.size());
    BenchmarkResult result = new BenchmarkResult("SelectOneRowAutoCommit", parallelism, disableVirtualThreads, durations);
    System.out.print(result);
  }

  private void runQuery(int iterations, ConcurrentLinkedQueue<Duration> durations) {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      for (int n = 0; n < iterations; n++) {
        String id = IDENTIFIERS.get(ThreadLocalRandom.current().nextInt(IDENTIFIERS.size()));
        Stopwatch watch = Stopwatch.createStarted();
        try (PreparedStatement statement = connection.prepareStatement(SELECT_SINGLE_ROW_SQL)) {
          statement.setString(1, id);
          try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
              for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
                assertEquals(resultSet.getString(col),
                    resultSet.getString(resultSet.getMetaData().getColumnLabel(col)));
              }
            }
          }
        }
        durations.add(watch.elapsed());
      }
    } catch (SQLException exception) {
      exception.printStackTrace();
      throw new RuntimeException(exception);
    }
  }


}
