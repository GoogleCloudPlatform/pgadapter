package com.google.cloud.pgadapter.benchmark;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class Statistics {
  private final BenchmarkConfiguration tpccConfiguration;

  private final AtomicReference<String> name = new AtomicReference<>("(unknown)");

  private final AtomicInteger totalOperations = new AtomicInteger();

  private final AtomicInteger parallelism = new AtomicInteger();

  private final AtomicLong operations = new AtomicLong();

  Statistics(BenchmarkConfiguration tpccConfiguration) {
    this.tpccConfiguration = tpccConfiguration;
  }

  void print(Duration runtime) {
    System.out.print("\033[2J\033[1;1H");
    System.out.printf(
        """
                \rBenchmark:      %s\t
                \rNum iterations: %d\t
                \rDuration:       %s\t
                \rParallelism:    %d\t
                \r
                \rOperations:     %d/%d (%.2f/s)\t
                """,
        getName(),
        tpccConfiguration.getIterations(),
        runtime,
        getParallelism(),
        getOperations(),
        getTotalOperations(),
        getOperationsPerSecond(runtime));
  }

  void reset(String name, int parallelism, int totalOperations) {
    setName(name);
    setParallelism(parallelism);
    setTotalOperations(totalOperations);
    operations.set(0L);
  }

  String getName() {
    return name.get();
  }

  private void setName(String name) {
    this.name.set(name);
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
}
