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

import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.jdbc.JdbcSqlExceptionFactory.JdbcAbortedException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractBenchmarkRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractBenchmarkRunner.class);

  private final Random random = new Random();

  private final Statistics statistics;

  private final TpccConfiguration tpccConfiguration;

  // We have to define the following configuration in the abstract class.
  // If we define them in inherited classes, they will be null in threads.
  protected final PGAdapterConfiguration pgAdapterConfiguration;
  protected final SpannerConfiguration spannerConfiguration;
  protected final Metrics metrics;
  protected final Dialect dialect;

  private boolean failed;

  AbstractBenchmarkRunner(
      Statistics statistics,
      TpccConfiguration tpccConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      SpannerConfiguration spannerConfiguration,
      Metrics metrics,
      Dialect dialect) {
    this.statistics = statistics;
    this.tpccConfiguration = tpccConfiguration;
    this.pgAdapterConfiguration = pgAdapterConfiguration;
    this.spannerConfiguration = spannerConfiguration;
    this.metrics = metrics;
    this.dialect = dialect;
  }

  @Override
  public void run() {
    LOG.info("Starting benchmark runner: " + statistics.getRunnerName());
    try {
      setup();
      runTransactions();
      LOG.info("Stopping benchmark runner: " + statistics.getRunnerName());
    } catch (InterruptedException interruptedException) {
      LOG.info("Stopping benchmark runner due to interruption: " + statistics.getRunnerName());
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      LOG.error("Benchmark runner failed:" + statistics.getRunnerName(), throwable);
      failed = true;
    } finally {
      try {
        teardown();
      } catch (Throwable teardownThrowable) {
        teardownThrowable.printStackTrace();
        LOG.error("Failed to clean up resources:" + statistics.getRunnerName(), teardownThrowable);
      }
    }
  }

  private void runTransactions() throws SQLException, InterruptedException {
    while (!Thread.interrupted()) {
      try {
        int transaction = random.nextInt(23);
        if (transaction < 10) {
          newOrder();
          statistics.incNewOrder();
        } else if (transaction < 20) {
          payment();
          statistics.incPayment();
        } else if (transaction < 21) {
          orderStatus();
          statistics.incOrderStatus();
        } else if (transaction < 22) {
          delivery();
          statistics.incDelivery();
        } else if (transaction < 23) {
          stockLevel();
          statistics.incStockLevel();
        } else {
          LOG.info("No transaction");
        }
      } catch (Throwable exception) {
        if ((exception instanceof PSQLException psqlException)
            && psqlException.getServerErrorMessage() != null
            && Objects.equals(
                psqlException.getServerErrorMessage().getSQLState(),
                PSQLState.SERIALIZATION_FAILURE.getState())
            && Objects.requireNonNull(psqlException.getServerErrorMessage().getMessage())
                .contains("concurrent modification")) {
          LOG.debug("Transaction aborted by Cloud Spanner via PG JDBC");
          statistics.incAborted();
        } else if (exception instanceof JdbcAbortedException) {
          LOG.debug("Transaction aborted by Cloud Spanner via Spanner JDBC");
          statistics.incAborted();
        } else {
          LOG.warn("Transaction failed", exception);
          statistics.incFailed();
        }
        try {
          executeStatement("rollback");
        } catch (Throwable rollbackException) {
          // Ignore errors. For an interrupted error, the while loop will be ended.
        }
      }
    }
  }

  private long getOtherWarehouseId(long currentId) {
    if (tpccConfiguration.getWarehouses() == 1) {
      return currentId;
    }
    while (true) {
      long otherId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
      if (otherId != currentId) {
        return otherId;
      }
    }
  }

  private String getNowFunctionInQuery() {
    return dialect == Dialect.POSTGRESQL ? "Now()" : "CURRENT_TIMESTAMP()";
  }

  private void newOrder() throws SQLException {
    LOG.debug("Executing new_order");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    long customerId = Long.reverse(random.nextInt(tpccConfiguration.getCustomersPerDistrict()));

    int orderLineCount = random.nextInt(5, 16);
    long[] itemIds = new long[orderLineCount];
    long[] supplyWarehouses = new long[orderLineCount];
    int[] quantities = new int[orderLineCount];
    int rollback = random.nextInt(100);
    int allLocal = 1;

    for (int line = 0; line < orderLineCount; line++) {
      if (rollback == 1 && line == orderLineCount - 1) {
        itemIds[line] = Long.reverse(Long.MAX_VALUE);
      } else {
        itemIds[line] = Long.reverse(random.nextInt(tpccConfiguration.getItemCount()));
      }
      if (random.nextInt(100) == 50) {
        supplyWarehouses[line] = getOtherWarehouseId(warehouseId);
        allLocal = 0;
      } else {
        supplyWarehouses[line] = warehouseId;
      }
      quantities[line] = random.nextInt(1, 10);
    }

    Object[] row;
    executeStatement("begin transaction");
    row =
        paramQueryRow(
            (tpccConfiguration.isLockScannedRanges() ? "/*@ lock_scanned_ranges=exclusive */" : "")
                + "SELECT c_discount, c_last, c_credit, w_tax "
                + "FROM customer c, warehouse w "
                + "WHERE w.w_id = ? AND c.w_id = w.w_id AND c.d_id = ? AND c.c_id = ?",
            new Object[] {warehouseId, districtId, customerId});
    BigDecimal discount = (BigDecimal) row[0];
    String last = (String) row[1];
    String credit = (String) row[2];
    BigDecimal warehouseTax = (BigDecimal) row[3];

    row =
        paramQueryRow(
            (tpccConfiguration.isLockScannedRanges() ? "/*@ lock_scanned_ranges=exclusive */" : "")
                + "SELECT d_next_o_id, d_tax "
                + "FROM district "
                + "WHERE w_id = ? AND d_id = ?",
            new Object[] {warehouseId, districtId});
    long districtNextOrderId = (long) row[0];
    BigDecimal districtTax = (BigDecimal) row[1];

    executeParamStatement(
        "UPDATE district " + "SET d_next_o_id = ? " + "WHERE d_id = ? AND w_id= ?",
        new Object[] {districtNextOrderId + 1L, districtId, warehouseId});
    executeParamStatement(
        "INSERT INTO orders (o_id, d_id, w_id, c_id, o_entry_d, o_ol_cnt, o_all_local) "
            + "VALUES (?,?,?,?,"
            + getNowFunctionInQuery()
            + ",?,?)",
        new Object[] {
          districtNextOrderId, districtId, warehouseId, customerId, orderLineCount, allLocal
        });
    executeParamStatement(
        "INSERT INTO new_orders (o_id, c_id, d_id, w_id) " + "VALUES (?,?,?,?)",
        new Object[] {districtNextOrderId, customerId, districtId, warehouseId});

    for (int line = 0; line < orderLineCount; line++) {
      long orderLineSupplyWarehouseId = supplyWarehouses[line];
      long orderLineItemId = itemIds[line];
      int orderLineQuantity = quantities[line];

      try {
        row =
            paramQueryRow(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                new Object[] {orderLineItemId});
      } catch (RowNotFoundException ignore) {
        // TODO: Record deliberate rollback
        LOG.info("Rolling back new_order transaction");
        executeStatement("rollback transaction");
        return;
      }
      BigDecimal itemPrice = (BigDecimal) row[0];
      String itemName = (String) row[1];
      String itemData = (String) row[2];

      row =
          paramQueryRow(
              (tpccConfiguration.isLockScannedRanges()
                      ? "/*@ lock_scanned_ranges=exclusive */"
                      : "")
                  + "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 "
                  + "FROM stock "
                  + "WHERE s_i_id = ? AND w_id= ?",
              new Object[] {orderLineItemId, orderLineSupplyWarehouseId});
      long stockQuantity = (long) row[0];
      String stockData = (String) row[1];
      String[] stockDistrict = new String[10];
      for (int i = 2; i < stockDistrict.length + 2; i++) {
        stockDistrict[i - 2] = (String) row[i];
      }
      String orderLineDistrictInfo =
          stockDistrict[(int) (Long.reverse(districtId) % stockDistrict.length)];
      if (stockQuantity > orderLineQuantity) {
        stockQuantity = stockQuantity - orderLineQuantity;
      } else {
        stockQuantity = stockQuantity - orderLineQuantity + 91;
      }

      executeParamStatement(
          "UPDATE stock " + "SET s_quantity = ? " + "WHERE s_i_id = ? AND w_id= ?",
          new Object[] {stockQuantity, orderLineItemId, orderLineSupplyWarehouseId});

      BigDecimal totalTax = BigDecimal.ONE.add(warehouseTax).add(districtTax);
      BigDecimal discountFactor = BigDecimal.ONE.subtract(discount);
      BigDecimal orderLineAmount =
          BigDecimal.valueOf(orderLineQuantity)
              .multiply(itemPrice)
              .multiply(totalTax)
              .multiply(discountFactor);
      executeParamStatement(
          "INSERT INTO order_line (o_id, c_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) "
              + "VALUES (?,?,?,?,?,?,?,?,?,?)",
          new Object[] {
            districtNextOrderId,
            customerId,
            districtId,
            warehouseId,
            line,
            orderLineItemId,
            orderLineSupplyWarehouseId,
            orderLineQuantity,
            orderLineAmount,
            orderLineDistrictInfo
          });
    }

    LOG.debug("Committing new_order transaction");
    executeStatement("commit");
  }

  private void payment() throws SQLException {
    LOG.debug("Executing payment");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    long customerId = Long.reverse(random.nextInt(tpccConfiguration.getCustomersPerDistrict()));
    BigDecimal amount = BigDecimal.valueOf(random.nextInt(1, 5000));

    long customerWarehouseId;
    long customerDistrictId;
    String lastName = LastNameGenerator.generateLastName(this.random, Long.MAX_VALUE);
    boolean byName;
    Object[] row;

    if (random.nextInt(100) < 60) {
      byName = true;
    } else {
      byName = false;
    }
    if (random.nextInt(100) < 85) {
      customerWarehouseId = warehouseId;
      customerDistrictId = districtId;
    } else {
      customerWarehouseId = getOtherWarehouseId(warehouseId);
      customerDistrictId =
          Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    }

    executeStatement("begin transaction");
    executeParamStatement(
        "UPDATE warehouse " + "SET w_ytd = w_ytd + ? " + "WHERE w_id = ?",
        new Object[] {amount, warehouseId});

    row =
        paramQueryRow(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "
                + "FROM warehouse "
                + "WHERE w_id = ?",
            new Object[] {warehouseId});
    String warehouseStreet1 = (String) row[0];
    String warehouseStreet2 = (String) row[1];
    String warehouseCity = (String) row[2];
    String warehouseState = (String) row[3];
    String warehouseZip = (String) row[4];
    String warehouseName = (String) row[5];

    executeParamStatement(
        "UPDATE district " + "SET d_ytd = d_ytd + ? " + "WHERE w_id = ? AND d_id= ?",
        new Object[] {amount, warehouseId, districtId});

    row =
        paramQueryRow(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "
                + "FROM district "
                + "WHERE w_id = ? AND d_id = ?",
            new Object[] {warehouseId, districtId});
    String districtStreet1 = (String) row[0];
    String districtStreet2 = (String) row[1];
    String districtCity = (String) row[2];
    String districtState = (String) row[3];
    String districtZip = (String) row[4];
    String districtName = (String) row[5];

    if (byName) {
      row =
          paramQueryRow(
              "SELECT count(c_id) namecnt "
                  + "FROM customer "
                  + "WHERE w_id = ? AND d_id= ? AND c_last=?",
              new Object[] {customerWarehouseId, customerDistrictId, lastName});
      int nameCount = (int) (long) row[0];
      if (nameCount % 2 == 0) {
        nameCount++;
      }
      List<Object[]> resultSet =
          executeParamQuery(
              "SELECT c_id "
                  + "FROM customer "
                  + "WHERE w_id = ? AND d_id= ? AND c_last=? "
                  + "ORDER BY c_first",
              new Object[] {customerWarehouseId, customerDistrictId, lastName});
      for (int counter = 0; counter < Math.min(nameCount, resultSet.size()); counter++) {
        customerId = (long) resultSet.get(counter)[0];
      }
    }
    row =
        paramQueryRow(
            (tpccConfiguration.isLockScannedRanges() ? "/*@ lock_scanned_ranges=exclusive */" : "")
                + "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since "
                + "FROM customer "
                + "WHERE w_id = ? AND d_id= ? AND c_id=?",
            new Object[] {customerWarehouseId, customerDistrictId, customerId});
    String firstName = (String) row[0];
    String middleName = (String) row[1];
    lastName = (String) row[2];
    String street1 = (String) row[3];
    String street2 = (String) row[4];
    String city = (String) row[5];
    String state = (String) row[6];
    String zip = (String) row[7];
    String phone = (String) row[8];
    String credit = (String) row[9];
    long creditLimit = (long) row[10];
    BigDecimal discount = (BigDecimal) row[11];
    BigDecimal balance = (BigDecimal) row[12];
    BigDecimal ytdPayment = (BigDecimal) row[13];
    Timestamp since = (Timestamp) row[14];

    balance = balance.subtract(amount);
    ytdPayment = ytdPayment.add(amount);
    if ("BC".equals(credit)) {
      row =
          paramQueryRow(
              "SELECT c_data " + "FROM customer " + "WHERE w_id = ? AND d_id=? AND c_id= ?",
              new Object[] {customerWarehouseId, customerDistrictId, customerId});
      String customerData = (String) row[0];
      String newCustomerData =
          String.format(
              "| %4d %2d %4d %2d %4d $%7.2f %12s %24s",
              customerId,
              customerDistrictId,
              customerWarehouseId,
              districtId,
              warehouseId,
              amount,
              LocalDateTime.now(),
              customerData);
      if (newCustomerData.length() > 500) {
        newCustomerData = newCustomerData.substring(0, 500);
      }

      executeParamStatement(
          "UPDATE customer "
              + "SET c_balance=?, c_ytd_payment=?, c_data=? "
              + "WHERE w_id = ? AND d_id=? AND c_id=?",
          new Object[] {
            balance,
            ytdPayment,
            newCustomerData,
            customerWarehouseId,
            customerDistrictId,
            customerId
          });
    } else {
      executeParamStatement(
          "UPDATE customer "
              + "SET c_balance=?, c_ytd_payment=? "
              + "WHERE w_id = ? AND d_id=? AND c_id=?",
          new Object[] {balance, ytdPayment, customerWarehouseId, customerDistrictId, customerId});
    }
    executeParamStatement(
        "INSERT INTO history (d_id, w_id, c_id, h_d_id,  h_w_id, h_date, h_amount, h_data) "
            + "VALUES (?,?,?,?,?,"
            + getNowFunctionInQuery()
            + ",?,?)",
        new Object[] {
          customerDistrictId,
          customerWarehouseId,
          customerId,
          districtId,
          warehouseId,
          amount,
          String.format("%10s %10s", warehouseName, districtName)
        });

    LOG.debug("Committing payment");
    executeStatement("commit");
  }

  private void orderStatus() throws SQLException {
    LOG.debug("Executing order_status");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    long customerId = Long.reverse(random.nextInt(tpccConfiguration.getCustomersPerDistrict()));

    String lastName = LastNameGenerator.generateLastName(this.random, Long.MAX_VALUE);
    boolean byName;
    Object[] row;
    if (random.nextInt(100) < 60) {
      byName = true;
    } else {
      byName = false;
    }

    BigDecimal balance;
    String first, middle, last;

    executeStatement("begin transaction");
    if (tpccConfiguration.isUseReadOnlyTransactions()) {
      executeStatement("set transaction read only");
    }
    if (byName) {
      row =
          paramQueryRow(
              "SELECT count(c_id) namecnt "
                  + "FROM customer "
                  + "WHERE w_id = ? AND d_id= ? AND c_last=?",
              new Object[] {warehouseId, districtId, lastName});
      int nameCount = (int) (long) row[0];
      if (nameCount % 2 == 0) {
        nameCount++;
      }
      List<Object[]> resultSet =
          executeParamQuery(
              "SELECT c_balance, c_first, c_middle, c_id "
                  + "FROM customer WHERE w_id = ? AND d_id= ? AND c_last=? "
                  + "ORDER BY c_first",
              new Object[] {warehouseId, districtId, lastName});
      for (int counter = 0; counter < Math.min(nameCount, resultSet.size()); counter++) {
        balance = (BigDecimal) resultSet.get(counter)[0];
        first = (String) resultSet.get(counter)[1];
        middle = (String) resultSet.get(counter)[2];
        customerId = (long) resultSet.get(counter)[3];
      }
    } else {
      row =
          paramQueryRow(
              "SELECT c_balance, c_first, c_middle, c_last "
                  + "FROM customer "
                  + "WHERE w_id = ? AND d_id=? AND c_id=?",
              new Object[] {warehouseId, districtId, customerId});
      balance = (BigDecimal) row[0];
      first = (String) row[1];
      middle = (String) row[2];
      last = (String) row[3];
    }

    row =
        paramQueryRow(
            "SELECT o_id, o_carrier_id, o_entry_d "
                + "FROM orders "
                + "WHERE w_id = ? AND d_id = ? AND c_id = ? "
                + "ORDER BY o_id DESC",
            new Object[] {warehouseId, districtId, customerId});
    long orderId = (long) row[0];

    long item_id, supply_warehouse_id, quantity;
    BigDecimal amount;
    Timestamp delivery_date;
    List<Object[]> resultSet =
        executeParamQuery(
            "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
                + "FROM order_line "
                + "WHERE w_id = ? AND d_id = ?  AND o_id = ?",
            new Object[] {warehouseId, districtId, orderId});
    for (int counter = 0; counter < resultSet.size(); counter++) {
      item_id = (long) resultSet.get(counter)[0]; // item_id
      supply_warehouse_id = (long) resultSet.get(counter)[1]; // supply_warehouse_id
      quantity = (long) resultSet.get(counter)[2]; // quantity
      amount = (BigDecimal) resultSet.get(counter)[3]; // amount
      delivery_date = (Timestamp) resultSet.get(counter)[4]; // delivery_date
    }

    LOG.debug("Committing order_status");
    executeStatement("commit");
  }

  private void delivery() throws SQLException {
    LOG.debug("Executing delivery");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long carrierId = Long.reverse(random.nextInt(10));
    Object[] row;

    executeStatement("begin transaction");
    for (long district = 0L; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
      long districtId = Long.reverse(district);
      row =
          paramQueryRow(
              (tpccConfiguration.isLockScannedRanges()
                      ? "/*@ lock_scanned_ranges=exclusive */"
                      : "")
                  + "SELECT o_id, c_id "
                  + "FROM new_orders "
                  + "WHERE d_id = ? AND w_id = ? "
                  + "ORDER BY o_id ASC "
                  + "LIMIT 1",
              new Object[] {districtId, warehouseId});
      if (row != null) {
        long newOrderId = (long) row[0];
        long customerId = (long) row[1];
        executeParamStatement(
            "DELETE "
                + "FROM new_orders "
                + "WHERE o_id = ? AND c_id = ? AND d_id = ? AND w_id = ?",
            new Object[] {newOrderId, customerId, districtId, warehouseId});
        row =
            paramQueryRow(
                "SELECT c_id FROM orders WHERE o_id = ? AND d_id = ? AND w_id = ?",
                new Object[] {newOrderId, districtId, warehouseId});
        executeParamStatement(
            "UPDATE orders "
                + "SET o_carrier_id = ? "
                + "WHERE o_id = ? AND c_id = ? AND d_id = ? AND w_id = ?",
            new Object[] {carrierId, newOrderId, customerId, districtId, warehouseId});
        executeParamStatement(
            "UPDATE order_line "
                + "SET ol_delivery_d = "
                + getNowFunctionInQuery()
                + " "
                + "WHERE o_id = ? AND c_id = ? AND d_id = ? AND w_id = ?",
            new Object[] {newOrderId, customerId, districtId, warehouseId});
        row =
            paramQueryRow(
                "SELECT SUM(ol_amount) sm "
                    + "FROM order_line "
                    + "WHERE o_id = ? AND c_id = ? AND d_id = ? AND w_id = ?",
                new Object[] {newOrderId, customerId, districtId, warehouseId});
        BigDecimal sumOrderLineAmount = (BigDecimal) row[0];
        executeParamStatement(
            "UPDATE customer "
                + "SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 "
                + "WHERE c_id = ? AND d_id = ? AND w_id = ?",
            new Object[] {sumOrderLineAmount, customerId, districtId, warehouseId});
      }
    }

    LOG.debug("Committing delivery");
    executeStatement("commit");
  }

  private void stockLevel() throws SQLException {
    LOG.debug("Executing stock_level");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    int level = random.nextInt(10, 21);

    executeStatement("begin transaction");
    if (tpccConfiguration.isUseReadOnlyTransactions()) {
      executeStatement("set transaction read only");
    }
    String stockLevelQueries = "case1";
    Object[] row;

    row =
        paramQueryRow(
            "SELECT d_next_o_id " + "FROM district " + "WHERE d_id = ? AND w_id= ?",
            new Object[] {districtId, warehouseId});
    long nextOrderId = (long) row[0];
    List<Object[]> resultSet =
        executeParamQuery(
            "SELECT COUNT(DISTINCT (s_i_id)) "
                + "FROM order_line ol, stock s "
                + "WHERE ol.w_id = ? "
                + "AND ol.d_id = ? "
                + "AND ol.o_id < ? "
                + "AND ol.o_id >= ? "
                + "AND s.w_id= ? "
                + "AND s_i_id=ol_i_id "
                + "AND s_quantity < ?",
            new Object[] {
              warehouseId, districtId, nextOrderId, nextOrderId - 20, warehouseId, level
            });
    for (int counter = 0; counter < resultSet.size(); counter++) {
      long orderLineItemId = (long) resultSet.get(counter)[0];
      // Note: We need a separate statement, because we already have an open result
      // set on the
      // original statement.
      row =
          paramQueryRow(
              "SELECT count(*) FROM stock "
                  + "WHERE w_id = ? AND s_i_id = ? "
                  + "AND s_quantity < ?",
              new Object[] {warehouseId, orderLineItemId, level});
      long stockCount = (long) row[0];
    }

    LOG.debug("Committing stock_level");
    executeStatement("commit");
  }

  public boolean isFailed() {
    return failed;
  }

  static class RowNotFoundException extends SQLException {
    RowNotFoundException(String message) {
      super(message);
    }
  }

  abstract void setup() throws SQLException, IOException;

  abstract void teardown() throws SQLException;

  abstract void executeStatement(String sql) throws SQLException;

  abstract Object[] paramQueryRow(String sql, Object[] params) throws SQLException;

  abstract void executeParamStatement(String sql, Object[] params) throws SQLException;

  abstract List<Object[]> executeParamQuery(String sql, Object[] params) throws SQLException;
}
