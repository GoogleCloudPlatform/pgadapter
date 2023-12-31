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

  BenchmarkRunner(
      Statistics statistics, String connectionUrl, BenchmarkConfiguration benchmarkConfiguration) {
    this.statistics = statistics;
    this.connectionUrl = connectionUrl;
    this.benchmarkConfiguration = benchmarkConfiguration;
  }

  @Override
  public void run() {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      this.identifiers = loadIdentifiers(connection);

      benchmarkSelectOneRowAutoCommit(1);
      benchmarkSelectOneRowAutoCommit(2);

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

  private void benchmarkSelectOneRowAutoCommit(int parallelism) throws Exception {
    int totalOperations = parallelism * benchmarkConfiguration.getIterations();
    statistics.reset("SelectOneRowAutoCommit", parallelism, totalOperations);

    ConcurrentLinkedQueue<Duration> durations = new ConcurrentLinkedQueue<>();

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    for (int task = 0; task < parallelism; task++) {
      executor.submit(() -> runQuery(benchmarkConfiguration.getIterations(), durations));
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(1L, TimeUnit.HOURS));
    assertEquals(totalOperations, durations.size());
    BenchmarkResult result = new BenchmarkResult("SelectOneRowAutoCommit", parallelism, durations);
    System.out.print(result);
  }

  private void runQuery(int iterations, ConcurrentLinkedQueue<Duration> durations) {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      for (int n = 0; n < iterations; n++) {
        String id = identifiers.get(ThreadLocalRandom.current().nextInt(identifiers.size()));
        Stopwatch watch = Stopwatch.createStarted();
        try (PreparedStatement statement =
            connection.prepareStatement("select * from benchmark_all_types where id=?")) {
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
        statistics.incOperations();
        durations.add(watch.elapsed());
      }
    } catch (SQLException exception) {
      exception.printStackTrace();
      throw new RuntimeException(exception);
    }
  }

  public boolean isFailed() {
    return failed;
  }
}
