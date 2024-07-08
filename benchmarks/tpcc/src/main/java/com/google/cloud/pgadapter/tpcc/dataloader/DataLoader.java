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

import com.google.cloud.Timestamp;
import com.google.cloud.pgadapter.tpcc.BenchmarkApplication;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ValueBinder;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkApplication.class);

  private final int MUTATION_LIMIT_PER_COMMIT = 60000;

  private final String connectionUrl;

  private final TpccConfiguration tpccConfiguration;

  private final ListeningExecutorService loadDataExecutor;

  private final ListeningExecutorService rowProducerExecutor;

  private final DataLoadStatus status;

  private final Dialect dialect;

  public DataLoader(
      DataLoadStatus status, String connectionUrl, TpccConfiguration tpccConfiguration) {
    this.connectionUrl = connectionUrl;
    this.tpccConfiguration = tpccConfiguration;
    this.loadDataExecutor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(tpccConfiguration.getLoadDataThreads()));
    this.rowProducerExecutor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(tpccConfiguration.getLoadDataThreads()));
    this.status = status;
    this.dialect =
        tpccConfiguration.getBenchmarkRunner().equals(TpccConfiguration.CLIENT_LIB_GSQL_RUNNER)
            ? Dialect.GOOGLE_STANDARD_SQL
            : Dialect.POSTGRESQL;
  }

  @Override
  public void close() {
    this.loadDataExecutor.shutdown();
    this.rowProducerExecutor.shutdown();
  }

  public long loadData() throws Exception {
    if (tpccConfiguration.isTruncateBeforeLoad()) {
      truncate();
    }

    long totalRowCount = 0L;
    int warehouseCount = tpccConfiguration.getWarehouses();
    totalRowCount += warehouseCount;
    ListenableFuture<Long> warehouseFuture =
        loadDataExecutor.submit(
            () -> {
              long rowCount = loadTable(new WarehouseRowProducer(status, warehouseCount));
              LOG.info("Loaded {} warehouse records", rowCount);
              return rowCount;
            });

    int itemCount = tpccConfiguration.getItemCount();
    totalRowCount += itemCount;
    ListenableFuture<Long> itemFuture =
        loadDataExecutor.submit(
            () -> {
              long rowCount = loadTable(new ItemRowProducer(status, itemCount));
              LOG.info("Loaded {} item records", rowCount);
              return rowCount;
            });

    // Wait for warehouse insert to finish before continuing.
    warehouseFuture.get();
    List<ListenableFuture<Long>> districtFutures = new ArrayList<>(warehouseCount);
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      long warehouseId = Long.reverse(warehouse);
      districtFutures.add(
          loadDataExecutor.submit(
              () -> {
                long rowCount =
                    loadTable(
                        new DistrictRowProducer(
                            status, warehouseId, tpccConfiguration.getDistrictsPerWarehouse()));
                LOG.info("Loaded {} district records for warehouse {}", rowCount, warehouseId);
                return rowCount;
              }));
      totalRowCount += tpccConfiguration.getDistrictsPerWarehouse();
    }
    // Wait for all districts and items to have been loaded before continuing with customers and
    // stock.
    Futures.allAsList(districtFutures).get();
    itemFuture.get();

    List<ListenableFuture<Long>> customerFutures =
        new ArrayList<>(warehouseCount * tpccConfiguration.getDistrictsPerWarehouse());
    List<ListenableFuture<Long>> stockFutures = new ArrayList<>(warehouseCount);
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      long warehouseId = Long.reverse(warehouse);
      stockFutures.add(
          loadDataExecutor.submit(
              () -> {
                long rowCount =
                    loadTable(
                        new StockRowProducer(
                            status, warehouseId, tpccConfiguration.getItemCount()));
                LOG.info("Loaded {} stock records for warehouse {}", rowCount, warehouseId);
                return rowCount;
              }));
      totalRowCount += tpccConfiguration.getItemCount();
      for (int district = 0; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
        long districtId = Long.reverse(district);
        customerFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new CustomerRowProducer(
                              status,
                              warehouseId,
                              districtId,
                              tpccConfiguration.getCustomersPerDistrict()));
                  LOG.info(
                      "Loaded {} customer records for warehouse {} and district {}",
                      rowCount,
                      warehouseId,
                      districtId);
                  return rowCount;
                }));
        totalRowCount += tpccConfiguration.getCustomersPerDistrict();
      }
    }
    // Wait for all customers to have been loaded before continuing with history and orders.
    Futures.allAsList(customerFutures).get();

    List<ListenableFuture<Long>> historyFutures =
        new ArrayList<>(warehouseCount * tpccConfiguration.getDistrictsPerWarehouse());
    List<ListenableFuture<Long>> orderFutures =
        new ArrayList<>(warehouseCount * tpccConfiguration.getDistrictsPerWarehouse());
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      long warehouseId = Long.reverse(warehouse);
      for (int district = 0; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
        long districtId = Long.reverse(district);
        historyFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new HistoryRowProducer(
                              status,
                              warehouseId,
                              districtId,
                              tpccConfiguration.getCustomersPerDistrict()));
                  LOG.info(
                      "Loaded {} history records for warehouse {} and district {}",
                      rowCount,
                      warehouseId,
                      districtId);
                  return rowCount;
                }));
        totalRowCount += tpccConfiguration.getCustomersPerDistrict();

        orderFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new OrderRowProducer(
                              status,
                              warehouseId,
                              districtId,
                              tpccConfiguration.getCustomersPerDistrict(),
                              tpccConfiguration.getCustomersPerDistrict()));
                  LOG.info(
                      "Loaded {} order records for warehouse {} and district {}",
                      rowCount,
                      warehouseId,
                      districtId);
                  return rowCount;
                }));
        totalRowCount += tpccConfiguration.getCustomersPerDistrict();
      }
    }

    // Wait for all orders and stock to have loaded before continuing with new_orders and
    // order_lines.
    Futures.allAsList(orderFutures).get();
    Futures.allAsList(stockFutures).get();

    List<ListenableFuture<Long>> newOrderFutures = new ArrayList<>();
    List<ListenableFuture<Long>> orderLineFutures = new ArrayList<>();
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      long warehouseId = Long.reverse(warehouse);
      for (int district = 0; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
        long districtId = Long.reverse(district);
        newOrderFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new NewOrderRowProducer(
                              status,
                              warehouseId,
                              districtId,
                              tpccConfiguration.getCustomersPerDistrict()));
                  LOG.info(
                      "Loaded {} new_order records for warehouse {} and district {}",
                      rowCount,
                      warehouseId,
                      districtId);
                  return rowCount;
                }));
        totalRowCount += tpccConfiguration.getCustomersPerDistrict() / 3;

        orderLineFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new OrderLineRowProducer(
                              status,
                              warehouseId,
                              districtId,
                              tpccConfiguration.getItemCount(),
                              tpccConfiguration.getCustomersPerDistrict()));
                  LOG.info(
                      "Loaded {} order_line records for warehouse {} and district {}",
                      rowCount,
                      warehouseId,
                      districtId);
                  return rowCount;
                }));
        totalRowCount += tpccConfiguration.getCustomersPerDistrict();
      }
    }

    // Wait for all remaining data loaders to finish.
    Futures.allAsList(historyFutures).get();
    Futures.allAsList(newOrderFutures).get();
    Futures.allAsList(orderLineFutures).get();

    loadDataExecutor.shutdown();
    if (!loadDataExecutor.awaitTermination(60L, TimeUnit.SECONDS)) {
      throw new TimeoutException("Loading data timed out while waiting for executor to shut down.");
    }
    return totalRowCount;
  }

  long loadTable(AbstractRowProducer rowProducer)
      throws RuntimeException, SQLException, IOException {
    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      return loadTableInGSQL(rowProducer);
    }
    return loadTableInPG(rowProducer);
  }

  String unquote(String input) {
    if (input.length() >= 2 && input.startsWith("'") && input.endsWith("'")) {
      return input.substring(1, input.length() - 1);
    }
    return input;
  }

  void setMutationColumn(WriteBuilder writeBuilder, String name, Object value)
      throws RuntimeException {
    ValueBinder valueBinder = writeBuilder.set(name);
    if (value instanceof Boolean) {
      valueBinder.to((Boolean) value);
    } else if (value instanceof Long) {
      valueBinder.to((Long) value);
    } else if (value instanceof Float) {
      valueBinder.to((Float) value);
    } else if (value instanceof Double) {
      valueBinder.to((Double) value);
    } else if (value instanceof BigDecimal) {
      valueBinder.to((BigDecimal) value);
    } else if (value instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) value;
      if (timestamp.equals(Timestamp.MIN_VALUE)) {
        // We use Timestamp.MIN_VALUE to represent NULL as ImmutableList does not permit
        // NULLs.
        valueBinder.to((Timestamp) null);
      } else {
        valueBinder.to(timestamp);
      }
    } else if (value instanceof String) {
      // Unquote the string because the generated strings are wrapped with single
      // quotes.
      String stringValue = unquote((String) value);
      if (stringValue.isEmpty()) {
        valueBinder.to((String) null);
      } else {
        valueBinder.to(stringValue);
      }
    } else {
      throw new RuntimeException(String.format("Unknown value for column %s: %s", name, value));
    }
  }

  long loadTableInGSQL(AbstractRowProducer rowProducer)
      throws RuntimeException, SQLException, IOException {
    long count = 0;
    try (Connection connection = createConnection();
        Statement statement = connection.createStatement()) {
      // Unwrap the Cloud Spanner specific interface to be able to access custom
      // methods.
      CloudSpannerJdbcConnection spannerConnection =
          connection.unwrap(CloudSpannerJdbcConnection.class);
      spannerConnection.setAutoCommit(false);

      // The smallest table `new_orders` only has 4 columns. So the largest number of
      // mutations that we may need to store in a batch can be the predefined mutation
      // limit divided by 4.
      List<Mutation> mutations = new ArrayList<>(Math.round(MUTATION_LIMIT_PER_COMMIT / 4));
      int columnCount = rowProducer.getColumnsAsList().size();
      for (long rowIndex = 0L; rowIndex < rowProducer.getRowCount(); rowIndex++) {
        List<ImmutableList> rows = rowProducer.createRowsAsList(rowIndex);
        if (rows == null) {
          continue;
        }
        for (int i = 0; i < rows.size(); i++) {
          ImmutableList row = rows.get(i);
          WriteBuilder writeBuilder = Mutation.newInsertBuilder(rowProducer.getTable());
          if (columnCount != row.size()) {
            // This should never happen.
            throw new RuntimeException(
                String.format(
                    "The number of generated columns does not match the target number - expected: %d, actual: %d",
                    columnCount, row.size()));
          }
          for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            setMutationColumn(
                writeBuilder,
                rowProducer.getColumnsAsList().get(columnIndex),
                row.get(columnIndex));
          }
          Mutation mutation = writeBuilder.build();
          mutations.add(mutation);
        }

        if ((mutations.size() * columnCount) >= MUTATION_LIMIT_PER_COMMIT) {
          spannerConnection.bufferedWrite(mutations);
          spannerConnection.commit();
          count += mutations.size();
          mutations.clear();
        }

        // We need to put this increase here because `rows` only has multiple records
        // for `order_line` table. The number of rows in the `order_line` table is
        // greater than the computed total number:
        //     count(`order_line` table) = warehouses * districts
        //                                 * customers per district
        //                                 * a pseudo-random value
        // When we show the rows in the status, we only a number without the
        // pseudo-random value.
        rowProducer.incRowCounterIncrementer();
      }

      // This is needed because there are some uncommitted mutations.
      if (mutations.size() != 0) {
        spannerConnection.bufferedWrite(mutations);
        spannerConnection.commit();
        count += mutations.size();
      }

      return count;
    }
  }

  long loadTableInPG(AbstractRowProducer rowProducer) throws SQLException, IOException {
    PipedWriter writer = new PipedWriter();
    try (PipedReader reader = new PipedReader(writer, 30_000);
        Connection connection = createConnection()) {
      CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
      rowProducer.asyncWriteRows(this.rowProducerExecutor, writer);
      return copyManager.copyIn(
          String.format(
              "copy \"%s\" (%s) from stdin (format csv, delimiter ',', quote '''')",
              rowProducer.getTable(), rowProducer.getColumns()),
          reader);
    }
  }

  void truncateTable(Statement statement, String table) throws SQLException {
    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      statement.execute(String.format("delete from %s where true", table));
    } else {
      statement.execute("truncate table " + table);
    }
  }

  void truncate() throws SQLException {
    try (Connection connection = createConnection();
        Statement statement = connection.createStatement()) {
      if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
        statement.execute("set autocommit=true");
      }
      LOG.info("truncating new_orders");
      truncateTable(statement, "new_orders");
      status.setTruncatedNewOrders();
      LOG.info("truncating order_line");
      truncateTable(statement, "order_line");
      status.setTruncatedOrderLine();
      LOG.info("truncating orders");
      truncateTable(statement, "orders");
      status.setTruncatedOrders();
      LOG.info("truncating history");
      truncateTable(statement, "history");
      status.setTruncatedHistory();
      LOG.info("truncating customer");
      truncateTable(statement, "customer");
      status.setTruncatedCustomer();
      LOG.info("truncating stock");
      truncateTable(statement, "stock");
      status.setTruncatedStock();
      LOG.info("truncating district");
      truncateTable(statement, "district");
      status.setTruncatedDistrict();
      LOG.info("truncating warehouse");
      truncateTable(statement, "warehouse");
      status.setTruncatedWarehouse();
      LOG.info("truncating item");
      truncateTable(statement, "item");
      status.setTruncatedItem();
      if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
        statement.execute("set autocommit=false");
      }
    }
  }

  private Connection createConnection() throws SQLException {
    Connection connection = DriverManager.getConnection(connectionUrl);
    // Allow copy operations to be non-atomic.
    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      connection.createStatement().execute("set autocommit_dml_mode='partitioned_non_atomic'");
    } else {
      // Use upsert instead of insert for COPY to prevent data loading errors if the
      // tables are already half-filled.
      connection.createStatement().execute("set spanner.copy_upsert=true");
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
    }
    return connection;
  }
}
