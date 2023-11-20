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

  String toString(Duration runtime) {
    return String.format(
        """
            \r\r\r\r\r\r\r
            New orders: %d (%.2f/s)
            Payments: %d (%.2f/s)
            Order status: %d (%.2f/s)
            Delivery: %d (%.2f/s)
            Stock level: %d (%.2f/s)
            Aborted: %d (%.2f/s)
            Total: %d (%.2f/s)
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
        getAbortedPerSecond(runtime),
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

  long getTotal() {
    return getNewOrder() + getPayment() + getDelivery() + getOrderStatus() + getStockLevel();
  }

  double getTotalPerSecond(Duration runtime) {
    return ((double) getTotal()) / runtime.getSeconds();
  }
}
