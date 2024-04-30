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
package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class Statistics {
  private final TpccConfiguration tpccConfiguration;

  private final AtomicReference<String> runnerName = new AtomicReference<>("(unknown)");

  private final AtomicLong newOrder = new AtomicLong();

  private final AtomicLong payment = new AtomicLong();

  private final AtomicLong orderStatus = new AtomicLong();

  private final AtomicLong delivery = new AtomicLong();

  private final AtomicLong stockLevel = new AtomicLong();

  private final AtomicLong aborted = new AtomicLong();

  private final AtomicLong failed = new AtomicLong();

  Statistics(TpccConfiguration tpccConfiguration) {
    this.tpccConfiguration = tpccConfiguration;
  }

  void print(Duration runtime) {
    System.out.print("\033[2J\033[1;1H");
    System.out.printf(
        """
                \rRunner:         %s\t
                \rNum threads:    %d\t
                \rDuration:       %s\t
                \rRead-only tx:   %s\t
                \rExclusive lock: %s\t
                \r
                \rNew orders:     %d (%.2f/s)\t
                \rPayments:       %d (%.2f/s)\t
                \rOrder status:   %d (%.2f/s)\t
                \rDelivery:       %d (%.2f/s)\t
                \rStock level:    %d (%.2f/s)\t
                \r
                \rAborted:        %d (%.1f%% - %.2f/s)\t
                \rFailed:         %d (%.1f%% - %.2f/s)\t
                \rSuccessful:     %d (%.1f%% - %.2f/s)\t
                \r
                \rTotal:          %d (%.2f/s)\t
                """,
        getRunnerName(),
        tpccConfiguration.getBenchmarkThreads(),
        runtime,
        tpccConfiguration.isUseReadOnlyTransactions(),
        tpccConfiguration.isLockScannedRanges(),
        getNewOrder(),
        getNewOrderPerSecond(runtime),
        getPayment(),
        getPaymentPerSecond(runtime),
        getOrderStatus(),
        getOrderStatusPerSecond(runtime),
        getDelivery(),
        getDeliveryPerSecond(runtime),
        getStockLevel(),
        getStockLevelPerSecond(runtime),
        getAborted(),
        getTotal() == 0 ? 0d : ((double) getAborted() / getTotal()) * 100,
        getAbortedPerSecond(runtime),
        getFailed(),
        getTotal() == 0 ? 0d : ((double) getFailed() / getTotal()) * 100,
        getFailedPerSecond(runtime),
        getSuccessful(),
        getTotal() == 0 ? 0d : ((double) getSuccessful() / getTotal()) * 100,
        getSuccessfulPerSecond(runtime),
        getTotal(),
        getTotalPerSecond(runtime));
  }

  String getRunnerName() {
    return runnerName.get();
  }

  void setRunnerName(String name) {
    this.runnerName.set(name);
  }

  long getNewOrder() {
    return newOrder.get();
  }

  double getNewOrderPerSecond(Duration runtime) {
    return ((double) newOrder.get()) / runtime.getSeconds();
  }

  void incNewOrder() {
    newOrder.incrementAndGet();
  }

  long getPayment() {
    return payment.get();
  }

  double getPaymentPerSecond(Duration runtime) {
    return ((double) payment.get()) / runtime.getSeconds();
  }

  void incPayment() {
    payment.incrementAndGet();
  }

  long getOrderStatus() {
    return orderStatus.get();
  }

  double getOrderStatusPerSecond(Duration runtime) {
    return ((double) orderStatus.get()) / runtime.getSeconds();
  }

  void incOrderStatus() {
    orderStatus.incrementAndGet();
  }

  long getDelivery() {
    return delivery.get();
  }

  double getDeliveryPerSecond(Duration runtime) {
    return ((double) delivery.get()) / runtime.getSeconds();
  }

  void incDelivery() {
    delivery.incrementAndGet();
  }

  long getStockLevel() {
    return stockLevel.get();
  }

  double getStockLevelPerSecond(Duration runtime) {
    return ((double) stockLevel.get()) / runtime.getSeconds();
  }

  void incStockLevel() {
    stockLevel.incrementAndGet();
  }

  long getAborted() {
    return aborted.get();
  }

  double getAbortedPerSecond(Duration runtime) {
    return ((double) aborted.get()) / runtime.getSeconds();
  }

  void incAborted() {
    aborted.incrementAndGet();
  }

  long getFailed() {
    return failed.get();
  }

  double getFailedPerSecond(Duration runtime) {
    return ((double) failed.get()) / runtime.getSeconds();
  }

  void incFailed() {
    failed.incrementAndGet();
  }

  long getTotal() {
    return getNewOrder()
        + getPayment()
        + getDelivery()
        + getOrderStatus()
        + getStockLevel()
        + getAborted()
        + getFailed();
  }

  double getTotalPerSecond(Duration runtime) {
    return ((double) getTotal()) / runtime.getSeconds();
  }

  long getSuccessful() {
    return getNewOrder() + getPayment() + getDelivery() + getOrderStatus() + getStockLevel();
  }

  double getSuccessfulPerSecond(Duration runtime) {
    return ((double) getTotal() - getAborted() - getFailed()) / runtime.getSeconds();
  }
}
