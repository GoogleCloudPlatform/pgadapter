package com.google.cloud.pgadapter.benchmark.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "benchmark")
public class BenchmarkConfiguration {
  private boolean loadData;

  private int loadDataThreads;

  private boolean truncateBeforeLoad;

  private boolean runBenchmark;

  private Duration benchmarkDuration;

  private int recordCount;

  private int iterations;

  private List<Integer> parallelism;

  /** --- Optimizations --- */
  private boolean useReadOnlyTransactions;

  private boolean lockScannedRanges;

  public boolean isLoadData() {
    return loadData;
  }

  public void setLoadData(boolean loadData) {
    this.loadData = loadData;
  }

  public int getLoadDataThreads() {
    return loadDataThreads;
  }

  public void setLoadDataThreads(int loadDataThreads) {
    this.loadDataThreads = loadDataThreads;
  }

  public boolean isTruncateBeforeLoad() {
    return truncateBeforeLoad;
  }

  public void setTruncateBeforeLoad(boolean truncateBeforeLoad) {
    this.truncateBeforeLoad = truncateBeforeLoad;
  }

  public boolean isRunBenchmark() {
    return runBenchmark;
  }

  public void setRunBenchmark(boolean runBenchmark) {
    this.runBenchmark = runBenchmark;
  }

  public Duration getBenchmarkDuration() {
    return benchmarkDuration;
  }

  public void setBenchmarkDuration(Duration benchmarkDuration) {
    this.benchmarkDuration = benchmarkDuration;
  }

  public int getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  public int getIterations() {
    return iterations;
  }

  public void setIterations(int iterations) {
    this.iterations = iterations;
  }

  public List<Integer> getParallelism() {
    return parallelism;
  }

  public void setParallelism(List<Integer> parallelism) {
    this.parallelism = parallelism;
  }

  public boolean isUseReadOnlyTransactions() {
    return useReadOnlyTransactions;
  }

  public void setUseReadOnlyTransactions(boolean useReadOnlyTransactions) {
    this.useReadOnlyTransactions = useReadOnlyTransactions;
  }

  public boolean isLockScannedRanges() {
    return lockScannedRanges;
  }

  public void setLockScannedRanges(boolean lockScannedRanges) {
    this.lockScannedRanges = lockScannedRanges;
  }
}