package com.google.cloud.pgadapter.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.common.base.Stopwatch;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BenchmarkRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

  private final Statistics statistics;

  private final String connectionUrl;

  private final BenchmarkConfiguration benchmarkConfiguration;

  private List<String> identifiers;

  private boolean failed;

  private final List<BenchmarkResult> results = new ArrayList<>();

  BenchmarkRunner(
      Statistics statistics, String connectionUrl, BenchmarkConfiguration benchmarkConfiguration) {
    this.statistics = statistics;
    this.connectionUrl = connectionUrl;
    this.benchmarkConfiguration = benchmarkConfiguration;
  }

  List<BenchmarkResult> getResults() {
    return this.results;
  }

  @Override
  public void run() {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      this.identifiers = loadIdentifiers(connection);

      for (int parallelism : benchmarkConfiguration.getParallelism()) {
        benchmarkSelectOneRowAutoCommit(parallelism);
        benchmarkSelect100RowsRowAutoCommit(parallelism);
        benchmarkSelectOneRowTransaction(parallelism);
        benchmarkSelect100RowsRowTransaction(parallelism);
      }

    } catch (Throwable throwable) {
      LOG.error("Benchmark runner failed", throwable);
      failed = true;
    }
  }

  private List<String> loadIdentifiers(Connection connection) throws SQLException {
    List<String> result = new ArrayList<>(benchmarkConfiguration.getRecordCount());
    try (ResultSet resultSet =
        connection.createStatement().executeQuery("select id from benchmark_all_types")) {
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
    }
    return result;
  }

  private void benchmarkSelect100RowsRowAutoCommit(int parallelism) throws Exception {
    benchmarkSelect(
        "Select100RowsAutoCommit",
        parallelism,
        "select * from benchmark_all_types where id>=? limit 100",
        true);
  }

  private void benchmarkSelectOneRowAutoCommit(int parallelism) throws Exception {
    benchmarkSelect(
        "SelectOneRowAutoCommit",
        parallelism,
        "select * from benchmark_all_types where id=?",
        true);
  }

  private void benchmarkSelect100RowsRowTransaction(int parallelism) throws Exception {
    benchmarkSelect(
        "Select100RowsTransaction",
        parallelism,
        "select * from benchmark_all_types where id>=? limit 100",
        false);
  }

  private void benchmarkSelectOneRowTransaction(int parallelism) throws Exception {
    benchmarkSelect(
        "SelectOneRowTransaction",
        parallelism,
        "select * from benchmark_all_types where id=?",
        false);
  }

  private void benchmarkSelect(String name, int parallelism, String sql, boolean autoCommit)
      throws Exception {
    int totalOperations = parallelism * benchmarkConfiguration.getIterations();
    statistics.reset(name, parallelism, totalOperations);

    ConcurrentLinkedQueue<Duration> durations = new ConcurrentLinkedQueue<>();

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    for (int task = 0; task < parallelism; task++) {
      executor.submit(
          () -> runQuery(sql, autoCommit, benchmarkConfiguration.getIterations(), durations));
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(1L, TimeUnit.HOURS));
    assertEquals(totalOperations, durations.size());
    BenchmarkResult result = new BenchmarkResult(name, parallelism, durations);
    results.add(result);
  }

  private void runQuery(
      String sql, boolean autoCommit, int iterations, ConcurrentLinkedQueue<Duration> durations) {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      connection.setAutoCommit(autoCommit);
      for (int n = 0; n < iterations; n++) {
        String id = identifiers.get(ThreadLocalRandom.current().nextInt(identifiers.size()));
        Stopwatch watch = Stopwatch.createStarted();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          statement.setString(1, id);
          try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
              for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
                assertEquals(
                    resultSet.getString(col),
                    resultSet.getString(resultSet.getMetaData().getColumnLabel(col)));
              }
            }
          }
        }
        if (!autoCommit) {
          connection.commit();
        }
        statistics.incOperations();
        durations.add(watch.elapsed());
      }
    } catch (SQLException exception) {
      throw new RuntimeException(exception);
    }
  }

  public boolean isFailed() {
    return failed;
  }
}