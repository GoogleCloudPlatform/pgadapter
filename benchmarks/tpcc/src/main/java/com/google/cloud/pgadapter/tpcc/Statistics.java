package com.google.cloud.pgadapter.tpcc;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

class Statistics {
  private final AtomicLong newOrder = new AtomicLong();

  private final AtomicLong payment = new AtomicLong();

  private final AtomicLong orderStatus = new AtomicLong();

  private final AtomicLong delivery = new AtomicLong();

  private final AtomicLong stockLevel = new AtomicLong();

  private final AtomicLong aborted = new AtomicLong();

  private final AtomicLong failed = new AtomicLong();

  void print(Duration runtime) {
    System.out.print("\033[2J\033[1;1H");
    System.out.printf(
        """
                \rNew orders:   %d (%.2f/s)
                \rPayments:     %d (%.2f/s)
                \rOrder status: %d (%.2f/s)
                \rDelivery:     %d (%.2f/s)
                \rStock level:  %d (%.2f/s)
                \r
                \rAborted:      %d (%.1f%% - %.2f/s)
                \rFailed:       %d (%.1f%% - %.2f/s)
                \rSuccessful:   %d (%.1f%% - %.2f/s)
                \r
                \rTotal:        %d (%.2f/s)
                """,
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
