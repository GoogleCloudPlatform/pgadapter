package com.google.cloud.pgadapter.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractBenchmarkRunner implements Runnable {

  @FunctionalInterface
  interface BenchmarkMethod {
    void run(String name, int parallelism) throws Exception;
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcBenchmarkRunner.class);

  private final String name;

  private final Map<String, JdbcBenchmarkRunner.BenchmarkMethod> benchmarks = new LinkedHashMap<>();

  final Statistics statistics;

  final BenchmarkConfiguration benchmarkConfiguration;

  List<String> identifiers;

  private final List<BenchmarkResult> results = new ArrayList<>();

  AbstractBenchmarkRunner(
      String name, Statistics statistics, BenchmarkConfiguration benchmarkConfiguration) {
    this.name = name;
    this.statistics = statistics;
    this.benchmarkConfiguration = benchmarkConfiguration;
    this.benchmarks.put("SelectOneValueAutoCommit", this::benchmarkSelectOneValueAutoCommit);
    this.benchmarks.put("SelectOneRowAutoCommit", this::benchmarkSelectOneRowAutoCommit);
    this.benchmarks.put("Select100RowsAutoCommit", this::benchmarkSelect100RowsRowAutoCommit);
    this.benchmarks.put("SelectOneRowTransaction", this::benchmarkSelectOneRowTransaction);
    this.benchmarks.put("Select100RowsTransaction", this::benchmarkSelect100RowsRowTransaction);
    for (String benchmark : benchmarkConfiguration.getBenchmarks()) {
      if (!this.benchmarks.containsKey(benchmark)) {
        throw new IllegalArgumentException(
            "Unknown benchmark: "
                + benchmark
                + "\nPossible values:\n"
                + String.join("\n", this.benchmarks.keySet()));
      }
    }
  }

  String getName() {
    return this.name;
  }

  List<BenchmarkResult> getResults() {
    return this.results;
  }

  @Override
  public void run() {
    try {
      this.identifiers = loadIdentifiers();

      for (int parallelism : benchmarkConfiguration.getParallelism()) {
        for (String benchmarkName : benchmarkConfiguration.getBenchmarks()) {
          benchmarks.get(benchmarkName).run(benchmarkName, parallelism);
        }
      }

    } catch (Throwable throwable) {
      throwable.printStackTrace();
      LOG.error("Benchmark runner failed", throwable);
    }
  }

  abstract List<String> loadIdentifiers();

  abstract String getParameterName(int index);

  void benchmarkSelectOneValueAutoCommit(String name, int parallelism) throws Exception {
    benchmarkSelect(
        name,
        parallelism,
        "select col_varchar from benchmark_all_types where id=" + getParameterName(1),
        true);
  }

  void benchmarkSelectOneRowAutoCommit(String name, int parallelism) throws Exception {
    benchmarkSelect(
        name,
        parallelism,
        "select * from benchmark_all_types where id=" + getParameterName(1),
        true);
  }

  void benchmarkSelect100RowsRowAutoCommit(String name, int parallelism) throws Exception {
    benchmarkSelect(
        name,
        parallelism,
        "select * from benchmark_all_types where id>=" + getParameterName(1) + " limit 100",
        true);
  }

  void benchmarkSelectOneRowTransaction(String name, int parallelism) throws Exception {
    benchmarkSelect(
        name,
        parallelism,
        "select * from benchmark_all_types where id=" + getParameterName(1),
        false);
  }

  void benchmarkSelect100RowsRowTransaction(String name, int parallelism) throws Exception {
    benchmarkSelect(
        name,
        parallelism,
        "select * from benchmark_all_types where id>=" + getParameterName(1) + " limit 100",
        false);
  }

  void benchmarkSelect(String benchmarkName, int parallelism, String sql, boolean autoCommit)
      throws Exception {
    int totalOperations = parallelism * benchmarkConfiguration.getIterations();
    statistics.reset(this.name, benchmarkName, parallelism, totalOperations);

    ConcurrentLinkedQueue<Duration> durations = new ConcurrentLinkedQueue<>();

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    for (int task = 0; task < parallelism; task++) {
      executor.submit(
          () -> runQuery(sql, autoCommit, benchmarkConfiguration.getIterations(), durations));
    }
    executor.shutdown();
    assertTrue(executor.awaitTermination(1L, TimeUnit.HOURS));
    assertEquals(totalOperations, durations.size());
    BenchmarkResult result = new BenchmarkResult(benchmarkName, parallelism, durations);
    results.add(result);
  }

  abstract void runQuery(
      String sql, boolean autoCommit, int iterations, ConcurrentLinkedQueue<Duration> durations);
}
