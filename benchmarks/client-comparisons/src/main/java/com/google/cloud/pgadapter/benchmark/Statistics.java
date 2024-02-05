package com.google.cloud.pgadapter.benchmark;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class Statistics {
  private final BenchmarkConfiguration tpccConfiguration;

  private final AtomicReference<String> runnerName = new AtomicReference<>("(unknown)");

  private final AtomicReference<String> benchmarkName = new AtomicReference<>("(unknown)");

  private final AtomicInteger totalOperations = new AtomicInteger();

  private final AtomicInteger parallelism = new AtomicInteger();

  private final AtomicLong operations = new AtomicLong();

  private final AtomicReference<Instant> startTime = new AtomicReference<>(Instant.now());

  Statistics(BenchmarkConfiguration tpccConfiguration) {
    this.tpccConfiguration = tpccConfiguration;
  }

  void print(Duration totalRuntime) {
    Duration runtime = getRuntime();
    System.out.print("\033[2J\033[1;1H");
    System.out.printf(
        """
                \rRunner:         %s\t
                \rBenchmark:      %s\t
                \rNum iterations: %d\t
                \rTotal runtime:  %s\t
                \rParallelism:    %d\t
                \rNum GCs:        %d\t
                \rGC time:        %s\t

                \rRuntime:        %s\t
                \rOperations:     %d/%d (%.2f/s)\t
                """,
        getRunnerName(),
        getBenchmarkName(),
        tpccConfiguration.getIterations(),
        totalRuntime,
        getParallelism(),
        getGarbageCollections(),
        getGarbageCollectionTime(),
        runtime,
        getOperations(),
        getTotalOperations(),
        getOperationsPerSecond(runtime));
  }

  void reset(String runnerName, String benchmarkName, int parallelism, int totalOperations) {
    setRunnerName(runnerName);
    setBenchmarkName(benchmarkName);
    setParallelism(parallelism);
    setTotalOperations(totalOperations);
    operations.set(0L);
    startTime.set(Instant.now());
  }

  Duration getRuntime() {
    return Duration.between(startTime.get(), Instant.now());
  }

  String getRunnerName() {
    return runnerName.get();
  }

  private void setRunnerName(String name) {
    this.runnerName.set(name);
  }

  String getBenchmarkName() {
    return benchmarkName.get();
  }

  private void setBenchmarkName(String name) {
    this.benchmarkName.set(name);
  }

  int getTotalOperations() {
    return totalOperations.get();
  }

  private void setTotalOperations(int totalOperations) {
    this.totalOperations.set(totalOperations);
  }

  int getParallelism() {
    return parallelism.get();
  }

  private void setParallelism(int parallelism) {
    this.parallelism.set(parallelism);
  }

  long getOperations() {
    return operations.get();
  }

  double getOperationsPerSecond(Duration runtime) {
    return ((double) operations.get()) / runtime.getSeconds();
  }

  void incOperations() {
    operations.incrementAndGet();
  }

  long getGarbageCollections() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .map(GarbageCollectorMXBean::getCollectionCount)
        .reduce(Long::sum)
        .orElse(0L);
  }

  Duration getGarbageCollectionTime() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .map(GarbageCollectorMXBean::getCollectionTime)
        .reduce(Long::sum)
        .map(Duration::ofMillis)
        .orElse(Duration.ZERO);
  }
}
