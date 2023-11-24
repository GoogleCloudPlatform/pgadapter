package com.google.cloud.pgadapter.tpcc.dataloader;

import com.google.cloud.pgadapter.tpcc.BenchmarkApplication;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
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

  private final String connectionUrl;

  private final TpccConfiguration tpccConfiguration;

  private final ListeningExecutorService loadDataExecutor;

  private final ListeningExecutorService rowProducerExecutor;

  public DataLoader(String connectionUrl, TpccConfiguration tpccConfiguration) {
    this.connectionUrl = connectionUrl;
    this.tpccConfiguration = tpccConfiguration;
    this.loadDataExecutor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(tpccConfiguration.getLoadDataThreads()));
    this.rowProducerExecutor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(tpccConfiguration.getLoadDataThreads()));
  }

  @Override
  public void close() {
    this.loadDataExecutor.shutdown();
    this.rowProducerExecutor.shutdown();
  }

  public void loadData() throws Exception {
    if (tpccConfiguration.isTruncateBeforeLoad()) {
      truncate();
    }

    int warehouseCount = tpccConfiguration.getWarehouses();
    ListenableFuture<Long> warehouseFuture =
        loadDataExecutor.submit(
            () -> {
              long rowCount = loadTable(new WarehouseRowProducer(warehouseCount));
              LOG.info("Loaded {} warehouse records", rowCount);
              return rowCount;
            });

    int itemCount = tpccConfiguration.getItemCount();
    ListenableFuture<Long> itemFuture =
        loadDataExecutor.submit(
            () -> {
              long rowCount = loadTable(new ItemRowProducer(itemCount));
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
                            warehouseId, tpccConfiguration.getDistrictsPerWarehouse()));
                LOG.info("Loaded {} district records for warehouse {}", rowCount, warehouseId);
                return rowCount;
              }));
    }
    // Wait for all districts to have been loaded before continuing with customers.
    Futures.allAsList(districtFutures).get();

    List<ListenableFuture<Long>> customerFutures =
        new ArrayList<>(warehouseCount * tpccConfiguration.getDistrictsPerWarehouse());
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      long warehouseId = Long.reverse(warehouse);
      for (int district = 0; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
        long districtId = Long.reverse(district);
        customerFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new CustomerRowProducer(
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
        orderFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new OrderRowProducer(
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
      }
    }

    // Start loading stock when all items have been loaded.
    itemFuture.get();
    List<ListenableFuture<Long>> stockFutures = new ArrayList<>(warehouseCount);
    for (int warehouse = 0; warehouse < warehouseCount; warehouse++) {
      long warehouseId = Long.reverse(warehouse);
      stockFutures.add(
          loadDataExecutor.submit(
              () -> {
                long rowCount =
                    loadTable(new StockRowProducer(warehouseId, tpccConfiguration.getItemCount()));
                LOG.info("Loaded {} stock records for warehouse {}", rowCount, warehouseId);
                return rowCount;
              }));
    }

    // Wait for all orders to have loaded before continuing with new_orders and order_lines.
    Futures.allAsList(orderFutures).get();

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
        orderLineFutures.add(
            loadDataExecutor.submit(
                () -> {
                  long rowCount =
                      loadTable(
                          new OrderLineRowProducer(
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
      }
    }

    // Wait for all remaining data loaders to finish.
    Futures.allAsList(historyFutures).get();
    Futures.allAsList(stockFutures).get();
    Futures.allAsList(newOrderFutures).get();
    Futures.allAsList(orderLineFutures).get();

    loadDataExecutor.shutdown();
    if (!loadDataExecutor.awaitTermination(60L, TimeUnit.SECONDS)) {
      throw new TimeoutException("Loading data timed out while waiting for executor to shut down.");
    }
  }

  long loadTable(AbstractRowProducer rowProducer) throws SQLException, IOException {
    PipedWriter writer = new PipedWriter();
    try (PipedReader reader = new PipedReader(writer);
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

  void truncate() throws SQLException {
    try (Connection connection = createConnection();
        Statement statement = connection.createStatement()) {
      LOG.info("truncating new_orders");
      statement.execute("truncate table new_orders");
      LOG.info("truncating order_line");
      statement.execute("truncate table order_line");
      LOG.info("truncating orders");
      statement.execute("truncate table orders");
      LOG.info("truncating history");
      statement.execute("truncate table history");
      LOG.info("truncating customer");
      statement.execute("truncate table customer");
      LOG.info("truncating stock");
      statement.execute("truncate table stock");
      LOG.info("truncating district");
      statement.execute("truncate table district");
      LOG.info("truncating warehouse");
      statement.execute("truncate table warehouse");
      LOG.info("truncating item");
      statement.execute("truncate table item");
    }
  }

  private Connection createConnection() throws SQLException {
    Connection connection = DriverManager.getConnection(connectionUrl);
    // Use upsert instead of insert for COPY to prevent data loading errors if the tables are
    // already half-filled.
    connection.createStatement().execute("set spanner.copy_upsert=true");
    // Allow copy operations to be non-atomic.
    connection
        .createStatement()
        .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
    return connection;
  }
}
