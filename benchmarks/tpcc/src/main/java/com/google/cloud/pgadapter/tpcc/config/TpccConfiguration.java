// Copyright 2024 Google LLC
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
package com.google.cloud.pgadapter.tpcc.config;

import java.time.Duration;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "tpcc")
public class TpccConfiguration {
  public static final String PGADAPTER_JDBC_RUNNER = "pgadapter";
  public static final String SPANNER_JDBC_RUNNER = "spanner_jdbc";
  public static final String CLIENT_LIB_PG_RUNNER = "client_lib_pg";
  public static final Set<String> RUNNERS =
      Set.of(PGADAPTER_JDBC_RUNNER, SPANNER_JDBC_RUNNER, CLIENT_LIB_PG_RUNNER);

  private boolean loadData;

  private int loadDataThreads;

  private boolean truncateBeforeLoad;

  private String benchmarkRunner;

  private boolean runBenchmark;

  private int benchmarkThreads;

  private Duration benchmarkDuration;

  private int warehouses;

  private int districtsPerWarehouse;

  private int customersPerDistrict;

  private int itemCount;

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

  public String getBenchmarkRunner() {
    return benchmarkRunner;
  }

  public void setBenchmarkRunner(String benchmarkRunner) {
    this.benchmarkRunner = benchmarkRunner;
  }

  public boolean isRunBenchmark() {
    return runBenchmark;
  }

  public void setRunBenchmark(boolean runBenchmark) {
    this.runBenchmark = runBenchmark;
  }

  public int getBenchmarkThreads() {
    return benchmarkThreads;
  }

  public void setBenchmarkThreads(int benchmarkThreads) {
    this.benchmarkThreads = benchmarkThreads;
  }

  public Duration getBenchmarkDuration() {
    return benchmarkDuration;
  }

  public void setBenchmarkDuration(Duration benchmarkDuration) {
    this.benchmarkDuration = benchmarkDuration;
  }

  public int getWarehouses() {
    return warehouses;
  }

  public void setWarehouses(int warehouses) {
    this.warehouses = warehouses;
  }

  public int getDistrictsPerWarehouse() {
    return districtsPerWarehouse;
  }

  public void setDistrictsPerWarehouse(int districtsPerWarehouse) {
    this.districtsPerWarehouse = districtsPerWarehouse;
  }

  public int getCustomersPerDistrict() {
    return customersPerDistrict;
  }

  public void setCustomersPerDistrict(int customersPerDistrict) {
    this.customersPerDistrict = customersPerDistrict;
  }

  public int getItemCount() {
    return itemCount;
  }

  public void setItemCount(int itemCount) {
    this.itemCount = itemCount;
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
