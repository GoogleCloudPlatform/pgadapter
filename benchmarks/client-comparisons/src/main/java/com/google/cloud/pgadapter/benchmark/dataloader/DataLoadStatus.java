package com.google.cloud.pgadapter.benchmark.dataloader;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DataLoadStatus {
  private final BenchmarkConfiguration benchmarkConfiguration;
  private final AtomicBoolean truncatedAllTypes = new AtomicBoolean();

  private final AtomicLong allTypes = new AtomicLong();

  public DataLoadStatus(BenchmarkConfiguration benchmarkConfiguration) {
    this.benchmarkConfiguration = benchmarkConfiguration;
  }

  public void print(Duration runtime) {
    System.out.printf(
        """
            \033[2J\033[1;1H
                \rNum threads:    %d\t
                \rDuration:       %s\t
                \r
                \rAll types:      %d/%d (%.2f%%) \t

                \r
                \rTotal:          %d/%d (%.2f%%) \t
                """,
        benchmarkConfiguration.getLoadDataThreads(),
        runtime,
        allTypes.get(),
        benchmarkConfiguration.getRecordCount(),
        ((double) allTypes.get() / benchmarkConfiguration.getRecordCount()) * 100,
        getCurrentTotal(),
        getTotal(),
        ((double) getCurrentTotal() / getTotal()) * 100);
  }

  void setTruncatedAllTypes() {
    truncatedAllTypes.set(true);
  }

  void incAllTypes() {
    allTypes.incrementAndGet();
  }

  long getCurrentTotal() {
    return allTypes.get();
  }

  long getTotal() {
    return benchmarkConfiguration.getRecordCount();
  }
}
