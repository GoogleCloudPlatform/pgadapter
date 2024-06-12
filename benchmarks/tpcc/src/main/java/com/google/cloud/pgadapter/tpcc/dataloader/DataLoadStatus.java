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
package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DataLoadStatus {
  private final TpccConfiguration tpccConfiguration;
  private final AtomicBoolean truncatedNewOrders = new AtomicBoolean();
  private final AtomicBoolean truncatedOrderLine = new AtomicBoolean();
  private final AtomicBoolean truncatedOrders = new AtomicBoolean();
  private final AtomicBoolean truncatedHistory = new AtomicBoolean();
  private final AtomicBoolean truncatedCustomer = new AtomicBoolean();
  private final AtomicBoolean truncatedStock = new AtomicBoolean();
  private final AtomicBoolean truncatedDistrict = new AtomicBoolean();
  private final AtomicBoolean truncatedWarehouse = new AtomicBoolean();
  private final AtomicBoolean truncatedItem = new AtomicBoolean();

  private final AtomicLong warehouse = new AtomicLong();
  private final AtomicLong district = new AtomicLong();
  private final AtomicLong customer = new AtomicLong();
  private final AtomicLong orders = new AtomicLong();
  private final AtomicLong orderLine = new AtomicLong();
  private final AtomicLong newOrder = new AtomicLong();
  private final AtomicLong history = new AtomicLong();
  private final AtomicLong stock = new AtomicLong();
  private final AtomicLong item = new AtomicLong();

  public DataLoadStatus(TpccConfiguration tpccConfiguration) {
    this.tpccConfiguration = tpccConfiguration;
  }

  public void print(Duration runtime) {
    System.out.printf(
        """
            \033[2J\033[1;1H
                \rNum threads:    %d\t
                \rDuration:       %s\t
                \r
                \rWarehouses:     %d/%d (%.2f%%) \t
                \rDistricts:      %d/%d (%.2f%%) \t
                \rItems:          %d/%d (%.2f%%) \t
                \rCustomers:      %d/%d (%.2f%%) \t
                \rStock:          %d/%d (%.2f%%) \t
                \rOrders:         %d/%d (%.2f%%) \t
                \rHistory:        %d/%d (%.2f%%) \t
                \rOrder lines:    %d/%d (%.2f%%) \t
                \rNew orders:     %d/%d (%.2f%%) \t

                \r
                \rTotal:          %d/%d (%.2f%%) \t
                """,
        tpccConfiguration.getLoadDataThreads(),
        runtime,
        warehouse.get(),
        tpccConfiguration.getWarehouses(),
        ((double) warehouse.get() / tpccConfiguration.getWarehouses()) * 100,
        district.get(),
        getTotalDistricts(),
        ((double) district.get() / getTotalDistricts()) * 100,
        item.get(),
        tpccConfiguration.getItemCount(),
        ((double) item.get() / tpccConfiguration.getItemCount()) * 100,
        customer.get(),
        getTotalCustomers(),
        ((double) customer.get() / getTotalCustomers()) * 100,
        stock.get(),
        getTotalStock(),
        ((double) stock.get() / getTotalStock()) * 100,
        orders.get(),
        getTotalOrders(),
        ((double) orders.get() / getTotalOrders()) * 100,
        history.get(),
        getTotalHistory(),
        ((double) history.get() / getTotalHistory()) * 100,
        orderLine.get(),
        getTotalOrderLines(),
        ((double) orderLine.get() / getTotalOrderLines()) * 100,
        newOrder.get(),
        getTotalNewOrder(),
        ((double) newOrder.get() / getTotalNewOrder()) * 100,
        getCurrentTotal(),
        getTotal(),
        ((double) getCurrentTotal() / getTotal()) * 100);
  }

  void setTruncatedNewOrders() {
    truncatedNewOrders.set(true);
  }

  void setTruncatedOrderLine() {
    truncatedOrderLine.set(true);
  }

  void setTruncatedOrders() {
    truncatedOrders.set(true);
  }

  void setTruncatedHistory() {
    truncatedHistory.set(true);
  }

  void setTruncatedCustomer() {
    truncatedCustomer.set(true);
  }

  void setTruncatedStock() {
    truncatedStock.set(true);
  }

  void setTruncatedDistrict() {
    truncatedDistrict.set(true);
  }

  void setTruncatedWarehouse() {
    truncatedWarehouse.set(true);
  }

  void setTruncatedItem() {
    truncatedItem.set(true);
  }

  void incWarehouse() {
    warehouse.incrementAndGet();
  }

  void incDistrict() {
    district.incrementAndGet();
  }

  void incCustomer() {
    customer.incrementAndGet();
  }

  void incOrders() {
    orders.incrementAndGet();
  }

  void incOrderLine() {
    orderLine.incrementAndGet();
  }

  void incNewOrder() {
    newOrder.incrementAndGet();
  }

  void incHistory() {
    history.incrementAndGet();
  }

  void incStock() {
    stock.incrementAndGet();
  }

  void incItem() {
    item.incrementAndGet();
  }

  long getCurrentTotal() {
    return warehouse.get()
        + district.get()
        + customer.get()
        + orders.get()
        + orderLine.get()
        + newOrder.get()
        + history.get()
        + item.get()
        + stock.get();
  }

  long getTotal() {
    return tpccConfiguration.getWarehouses()
        + getTotalDistricts()
        + getTotalCustomers()
        + getTotalOrders()
        + getTotalOrderLines()
        + getTotalNewOrder()
        + getTotalHistory()
        + tpccConfiguration.getItemCount()
        + getTotalStock();
  }

  long getTotalDistricts() {
    return (long) tpccConfiguration.getDistrictsPerWarehouse() * tpccConfiguration.getWarehouses();
  }

  long getTotalCustomers() {
    return (long) tpccConfiguration.getCustomersPerDistrict()
        * tpccConfiguration.getDistrictsPerWarehouse()
        * tpccConfiguration.getWarehouses();
  }

  long getTotalOrders() {
    return (long) tpccConfiguration.getCustomersPerDistrict()
        * tpccConfiguration.getDistrictsPerWarehouse()
        * tpccConfiguration.getWarehouses();
  }

  long getTotalOrderLines() {
    return (long) tpccConfiguration.getCustomersPerDistrict()
        * tpccConfiguration.getDistrictsPerWarehouse()
        * tpccConfiguration.getWarehouses();
  }

  long getTotalNewOrder() {
    return (((long) tpccConfiguration.getCustomersPerDistrict()
            * tpccConfiguration.getDistrictsPerWarehouse()
            * tpccConfiguration.getWarehouses())
        / 3);
  }

  long getTotalHistory() {
    return (long) tpccConfiguration.getCustomersPerDistrict()
        * tpccConfiguration.getDistrictsPerWarehouse()
        * tpccConfiguration.getWarehouses();
  }

  long getTotalStock() {
    return (long) tpccConfiguration.getWarehouses() * tpccConfiguration.getItemCount();
  }
}
