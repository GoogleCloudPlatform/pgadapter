package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Random;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BenchmarkRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

  private final Random random = new Random();

  private final Statistics statistics;

  private final String connectionUrl;

  private final TpccConfiguration tpccConfiguration;

  private boolean failed;

  BenchmarkRunner(
      Statistics statistics, String connectionUrl, TpccConfiguration tpccConfiguration) {
    this.statistics = statistics;
    this.connectionUrl = connectionUrl;
    this.tpccConfiguration = tpccConfiguration;
  }

  @Override
  public void run() {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      prepareStatements(connection);
      runTransactions(connection);
      LOG.info("Stopping benchmark runner");
    } catch (InterruptedException interruptedException) {
      LOG.info("Stopping benchmark runner due to interruption");
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      LOG.error("Benchmark runner failed", throwable);
      failed = true;
    }
  }

  private void runTransactions(Connection connection) throws SQLException, InterruptedException {
    try (Statement statement = connection.createStatement()) {
      while (true) {
        try {
          int transaction = random.nextInt(23);
          if (transaction < 10) {
            newOrder(statement);
            statistics.incNewOrder();
          } else if (transaction < 20) {
            payment(statement);
            statistics.incPayment();
          } else if (transaction < 21) {
            orderStatus(statement);
            statistics.incOrderStatus();
          } else if (transaction < 22) {
            delivery(statement);
            statistics.incDelivery();
          } else if (transaction < 23) {
            stockLevel(statement);
            statistics.incStockLevel();
          } else {
            LOG.info("No transaction");
          }
          if (Thread.interrupted()) {
            break;
          }
        } catch (Throwable exception) {
          if ((exception instanceof PSQLException psqlException)
              && psqlException.getServerErrorMessage() != null
              && Objects.equals(
                  psqlException.getServerErrorMessage().getSQLState(),
                  PSQLState.SERIALIZATION_FAILURE.getState())
              && Objects.requireNonNull(psqlException.getServerErrorMessage().getMessage())
                  .contains("concurrent modification")) {
            LOG.debug("Transaction aborted by Cloud Spanner");
            statistics.incAborted();
          } else {
            LOG.warn("Transaction failed", exception);
            statistics.incFailed();
          }
          statement.execute("rollback");
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

  private void newOrder(Statement statement) throws SQLException {
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
    statement.execute("begin transaction");
    // TODO: This should use FOR UPDATE.
    row =
        queryRow(
            statement,
            "execute select_customer_info (%d, %d, %d)",
            warehouseId,
            districtId,
            customerId);
    BigDecimal discount = (BigDecimal) row[0];
    String last = (String) row[1];
    String credit = (String) row[2];
    BigDecimal warehouseTax = (BigDecimal) row[3];

    // TODO: This should use FOR UPDATE.
    row =
        queryRow(statement, "execute get_next_order_id_and_tax (%d, %d)", warehouseId, districtId);
    long districtNextOrderId = (long) row[0];
    BigDecimal districtTax = (BigDecimal) row[1];

    statement.execute(
        String.format(
            "execute update_next_order_id (%d, %d, %d)",
            districtNextOrderId + 1L, districtId, warehouseId));
    statement.execute(
        String.format(
            "execute insert_order (%d,%d,%d,%d,%d,%d)",
            districtNextOrderId, districtId, warehouseId, customerId, orderLineCount, allLocal));
    statement.execute(
        String.format(
            "execute insert_new_order (%d,%d,%d)", districtNextOrderId, districtId, warehouseId));

    for (int line = 0; line < orderLineCount; line++) {
      long orderLineSupplyWarehouseId = supplyWarehouses[line];
      long orderLineItemId = itemIds[line];
      int orderLineQuantity = quantities[line];

      try {
        row = queryRow(statement, "execute select_item (%d)", orderLineItemId);
      } catch (RowNotFoundException ignore) {
        // TODO: Record deliberate rollback
        LOG.info("Rolling back new_order transaction");
        statement.execute("rollback transaction");
        return;
      }
      BigDecimal itemPrice = (BigDecimal) row[0];
      String itemName = (String) row[1];
      String itemData = (String) row[2];

      row =
          queryRow(
              statement,
              "execute select_stock (%d, %d)",
              orderLineItemId,
              orderLineSupplyWarehouseId);
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

      statement.execute(
          String.format(
              "execute update_stock (%d, %d, %d)",
              stockQuantity, orderLineItemId, orderLineSupplyWarehouseId));

      BigDecimal totalTax = BigDecimal.ONE.add(warehouseTax).add(districtTax);
      BigDecimal discountFactor = BigDecimal.ONE.subtract(discount);
      BigDecimal orderLineAmount =
          BigDecimal.valueOf(orderLineQuantity)
              .multiply(itemPrice)
              .multiply(totalTax)
              .multiply(discountFactor);
      statement.execute(
          String.format(
              "execute insert_order_line (%d,%d,%d,%d,%d,%d,%d,%s,'%s')",
              districtNextOrderId,
              districtId,
              warehouseId,
              line,
              orderLineItemId,
              orderLineSupplyWarehouseId,
              orderLineQuantity,
              orderLineAmount,
              orderLineDistrictInfo));
    }

    LOG.debug("Committing new_order transaction");
    statement.execute("commit");
  }

  private void payment(Statement statement) throws SQLException {
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

    statement.execute("begin transaction");
    statement.execute(String.format("execute update_warehouse (%s, %d)", amount, warehouseId));

    row = queryRow(statement, "execute select_warehouse (%d)", warehouseId);
    String warehouseStreet1 = (String) row[0];
    String warehouseStreet2 = (String) row[1];
    String warehouseCity = (String) row[2];
    String warehouseState = (String) row[3];
    String warehouseZip = (String) row[4];
    String warehouseName = (String) row[5];

    statement.execute(
        String.format("execute update_district (%s, %d, %d)", amount, warehouseId, districtId));

    row = queryRow(statement, "execute select_district (%d, %d)", warehouseId, districtId);
    String districtStreet1 = (String) row[0];
    String districtStreet2 = (String) row[1];
    String districtCity = (String) row[2];
    String districtState = (String) row[3];
    String districtZip = (String) row[4];
    String districtName = (String) row[5];

    if (byName) {
      row =
          queryRow(
              statement,
              "execute count_customer (%d, %d, '%s')",
              customerWarehouseId,
              customerDistrictId,
              lastName);
      int nameCount = (int) (long) row[0];
      if (nameCount % 2 == 0) {
        nameCount++;
      }
      try (ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "execute select_customer (%d, %d, '%s')",
                  customerWarehouseId, customerDistrictId, lastName))) {
        for (int counter = 0; counter < nameCount; counter++) {
          if (resultSet.next()) {
            customerId = resultSet.getLong(1);
          }
        }
      }
    }
    row =
        queryRow(
            statement,
            "execute select_customer_details (%d, %d, %d)",
            customerWarehouseId,
            customerDistrictId,
            customerId);
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
          queryRow(
              statement,
              "execute select_customer_c_data (%d, %d, %d)",
              customerWarehouseId,
              customerDistrictId,
              customerId);
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

      statement.execute(
          String.format(
              "execute update_customer_1_ (%s, %s, '%s', %d, %d, %d)",
              balance,
              ytdPayment,
              newCustomerData,
              customerWarehouseId,
              customerDistrictId,
              customerId));
    } else {
      statement.execute(
          String.format(
              "execute update_customer_2_ (%s, %s, %d, %d, %d)",
              balance, ytdPayment, customerWarehouseId, customerDistrictId, customerId));
    }
    statement.execute(
        String.format(
            "execute insert_history (%d,%d,%d,%d,%d,%s,'%s')",
            customerDistrictId,
            customerWarehouseId,
            customerId,
            districtId,
            warehouseId,
            amount,
            String.format("%10s %10s", warehouseName, districtName)));

    LOG.debug("Committing payment");
    statement.execute("commit");
  }

  private void orderStatus(Statement statement) throws SQLException {
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

    statement.execute("begin transaction");
    if (byName) {
      row =
          queryRow(
              statement,
              "execute count_customer (%d, %d, '%s')",
              warehouseId,
              districtId,
              lastName);
      int nameCount = (int) (long) row[0];
      if (nameCount % 2 == 0) {
        nameCount++;
      }
      try (ResultSet resultSet =
          statement.executeQuery(
              String.format(
                  "execute select_customer_balance_1_ (%d, %d, '%s')",
                  warehouseId, districtId, lastName))) {
        for (int counter = 0; counter < nameCount; counter++) {
          if (resultSet.next()) {
            balance = resultSet.getBigDecimal(1);
            first = resultSet.getString(2);
            middle = resultSet.getString(3);
            customerId = resultSet.getLong(4);
          }
        }
      }
    } else {
      row =
          queryRow(
              statement,
              "execute select_customer_balance_2_ (%d, %d, %d)",
              warehouseId,
              districtId,
              customerId);
      balance = (BigDecimal) row[0];
      first = (String) row[1];
      middle = (String) row[2];
      last = (String) row[3];
    }

    row =
        queryRow(
            QueryRowMode.ALLOW_MORE_THAN_ONE,
            statement,
            "execute select_order (%d, %d, %d)",
            warehouseId,
            districtId,
            customerId);
    long orderId = (long) row[0];
    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "execute select_order_line (%d, %d, %d)", warehouseId, districtId, orderId))) {
      while (resultSet.next()) {
        resultSet.getLong(1); // item_id
        resultSet.getLong(2); // supply_warehouse_id
        resultSet.getLong(3); // quantity
        resultSet.getBigDecimal(4); // amount
        resultSet.getTimestamp(5); // delivery_date
      }
    }

    LOG.debug("Committing order_status");
    statement.execute("commit");
  }

  private void delivery(Statement statement) throws SQLException {
    LOG.debug("Executing delivery");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long carrierId = Long.reverse(random.nextInt(10));
    Object[] row;

    statement.execute("begin transaction");
    for (long district = 0L; district < tpccConfiguration.getDistrictsPerWarehouse(); district++) {
      long districtId = Long.reverse(district);
      row =
          queryRow(
              QueryRowMode.ALLOW_LESS_THAN_ONE,
              statement,
              "execute select_new_order (%d, %d)",
              districtId,
              warehouseId);
      if (row != null) {
        long newOrderId = (long) row[0];
        statement.execute(
            String.format(
                "execute delete_new_order (%d, %d, %d)", newOrderId, districtId, warehouseId));
        row =
            queryRow(
                statement,
                "execute select_order_customer (%d, %d, %d)",
                newOrderId,
                districtId,
                warehouseId);
        long customerId = (long) row[0];
        statement.execute(
            String.format(
                "execute update_order (%d, %d, %d, %d)",
                carrierId, newOrderId, districtId, warehouseId));
        statement.execute(
            String.format(
                "execute update_order_line (%d, %d, %d)", newOrderId, districtId, warehouseId));
        row =
            queryRow(
                statement,
                "execute sum_order_line (%d, %d, %d)",
                newOrderId,
                districtId,
                warehouseId);
        BigDecimal sumOrderLineAmount = (BigDecimal) row[0];
        statement.execute(
            String.format(
                "execute update_customer_bal (%s, %d, %d, %d)",
                sumOrderLineAmount, customerId, districtId, warehouseId));
      }
    }

    LOG.debug("Committing delivery");
    statement.execute("commit");
  }

  private void stockLevel(Statement statement) throws SQLException {
    LOG.debug("Executing stock_level");

    long warehouseId = Long.reverse(random.nextInt(tpccConfiguration.getWarehouses()));
    long districtId = Long.reverse(random.nextInt(tpccConfiguration.getDistrictsPerWarehouse()));
    int level = random.nextInt(10, 21);

    statement.execute("begin transaction");
    String stockLevelQueries = "case1";
    Object[] row;

    row = queryRow(statement, "execute get_next_order_id (%d, %d)", districtId, warehouseId);
    long nextOrderId = (long) row[0];
    // Only 'case1' is implemented.
    try (ResultSet resultSet =
        statement.executeQuery(
            String.format(
                "execute count_order_line (%d,%d,%d,%d,%d,%d)",
                warehouseId, districtId, nextOrderId, nextOrderId - 20, warehouseId, level))) {
      while (resultSet.next()) {
        long orderLineItemId = resultSet.getLong(1);
        // Note: We need a separate statement, because we already have an open result set on the
        // original statement.
        row =
            queryRow(
                statement.getConnection().createStatement(),
                "execute select_stock_count (%d, %d, %d)",
                warehouseId,
                orderLineItemId,
                level);
        long stockCount = (long) row[0];
      }
    }

    LOG.debug("Committing stock_level");
    statement.execute("commit");
  }

  public boolean isFailed() {
    return failed;
  }

  static class RowNotFoundException extends SQLException {
    RowNotFoundException(String message) {
      super(message);
    }
  }

  enum QueryRowMode {
    REQUIRE_ONE,
    ALLOW_MORE_THAN_ONE,
    ALLOW_LESS_THAN_ONE,
  }

  private Object[] queryRow(Statement statement, String query, Object... parameters)
      throws SQLException {
    return queryRow(QueryRowMode.REQUIRE_ONE, statement, query, parameters);
  }

  private Object[] queryRow(
      QueryRowMode queryRowMode, Statement statement, String query, Object... parameters)
      throws SQLException {
    String sql = String.format(query, parameters);
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      if (!resultSet.next()) {
        if (queryRowMode == QueryRowMode.ALLOW_LESS_THAN_ONE) {
          return null;
        } else {
          throw new RowNotFoundException(String.format("No results found for: %s", sql));
        }
      }
      Object[] result = new Object[resultSet.getMetaData().getColumnCount()];
      for (int i = 0; i < result.length; i++) {
        result[i] = resultSet.getObject(i + 1);
      }
      if (queryRowMode != QueryRowMode.ALLOW_MORE_THAN_ONE && resultSet.next()) {
        throw new SQLException(String.format("More than one result found for: %s", sql));
      }
      return result;
    }
  }

  private void prepareStatements(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "PREPARE select_customer_info as "
              + "SELECT c_discount, c_last, c_credit, w_tax "
              + "FROM customer , warehouse "
              + "WHERE w_id = $1 AND c_w_id = w_id AND c_d_id = $2 AND c_id = $3");
      statement.execute(
          "PREPARE get_next_order_id_and_tax as "
              + "SELECT d_next_o_id, d_tax "
              + "FROM district "
              + "WHERE d_w_id = $1 AND d_id = $2");
      statement.execute(
          "PREPARE get_next_order_id as "
              + "SELECT d_next_o_id "
              + "FROM district "
              + "WHERE d_id = $1 AND d_w_id= $2");
      statement.execute(
          "PREPARE update_next_order_id as "
              + "UPDATE district "
              + "SET d_next_o_id = $1 "
              + "WHERE d_id = $2 AND d_w_id= $3");
      statement.execute(
          "PREPARE insert_order (bigint,bigint,bigint,bigint,bigint,bigint) as "
              + "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) "
              + "VALUES ($1,$2,$3,$4,NOW(),$5,$6)");
      statement.execute(
          "PREPARE insert_new_order as "
              + "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) "
              + "VALUES ($1,$2,$3)");
      statement.execute(
          "PREPARE select_item as SELECT i_price, i_name, i_data FROM item WHERE i_id = $1");
      statement.execute(
          "PREPARE select_stock as "
              + "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 "
              + "FROM stock "
              + "WHERE s_i_id = $1 AND s_w_id= $2");
      statement.execute(
          "PREPARE update_stock as "
              + "UPDATE stock "
              + "SET s_quantity = $1 "
              + "WHERE s_i_id = $2 AND s_w_id= $3");
      statement.execute(
          "PREPARE insert_order_line as "
              + "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) "
              + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)");
      statement.execute(
          "PREPARE update_warehouse as "
              + "UPDATE warehouse "
              + "SET w_ytd = w_ytd + $1 "
              + "WHERE w_id = $2");
      statement.execute(
          "PREPARE select_warehouse as "
              + "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "
              + "FROM warehouse "
              + "WHERE w_id = $1");
      statement.execute(
          "PREPARE update_district as "
              + "UPDATE district "
              + "SET d_ytd = d_ytd + $1 "
              + "WHERE d_w_id = $2 AND d_id= $3");
      statement.execute(
          "PREPARE select_district as "
              + "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "
              + "FROM district "
              + "WHERE d_w_id = $1 AND d_id = $2");
      statement.execute(
          "PREPARE count_customer as "
              + "SELECT count(c_id) namecnt "
              + "FROM customer "
              + "WHERE c_w_id = $1 AND c_d_id= $2 AND c_last=$3");
      statement.execute(
          "PREPARE select_customer as "
              + "SELECT c_id "
              + "FROM customer "
              + "WHERE c_w_id = $1 AND c_d_id= $2 AND c_last=$3 "
              + "ORDER BY c_first");
      statement.execute(
          "PREPARE select_customer_details as "
              + "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since "
              + "FROM customer "
              + "WHERE c_w_id = $1 AND c_d_id= $2 AND c_id=$3");
      statement.execute(
          "PREPARE select_customer_c_data as "
              + "SELECT c_data "
              + "FROM customer "
              + "WHERE c_w_id = $1 AND c_d_id=$2 AND c_id= $3");
      statement.execute(
          "PREPARE update_customer_1_ as "
              + "UPDATE customer "
              + "SET c_balance=$1, c_ytd_payment=$2, c_data=$3 "
              + "WHERE c_w_id = $4 AND c_d_id=$5 AND c_id=$6");
      statement.execute(
          "PREPARE update_customer_2_ as "
              + "UPDATE customer "
              + "SET c_balance=$1, c_ytd_payment=$2 "
              + "WHERE c_w_id = $3 AND c_d_id=$4 AND c_id=$5");
      statement.execute(
          "PREPARE insert_history (bigint,bigint,bigint,bigint,bigint,bigint,varchar) as "
              + "INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data) "
              + "VALUES ($1,$2,$3,$4,$5,NOW(),$6,$7)");
      statement.execute(
          "PREPARE select_customer_balance_1_ as "
              + "SELECT c_balance, c_first, c_middle, c_id "
              + "FROM customer WHERE c_w_id = $1 AND c_d_id= $2 AND c_last=$3 "
              + "ORDER BY c_first");
      statement.execute(
          "PREPARE select_customer_balance_2_ as "
              + "SELECT c_balance, c_first, c_middle, c_last "
              + "FROM customer "
              + "WHERE c_w_id = $1 AND c_d_id=$2 AND c_id=$3");
      statement.execute(
          "PREPARE select_order as "
              + "SELECT o_id, o_carrier_id, o_entry_d "
              + "FROM orders "
              + "WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3 "
              + "ORDER BY o_id DESC");
      statement.execute(
          "PREPARE select_order_line as "
              + "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
              + "FROM order_line "
              + "WHERE ol_w_id = $1 AND ol_d_id = $2  AND ol_o_id = $3");
      statement.execute(
          "PREPARE select_new_order as "
              + "SELECT no_o_id "
              + "FROM new_orders "
              + "WHERE no_d_id = $1 AND no_w_id = $2 "
              + "ORDER BY no_o_id ASC "
              + "LIMIT 1");
      statement.execute(
          "PREPARE delete_new_order as "
              + "DELETE "
              + "FROM new_orders "
              + "WHERE no_o_id = $1 AND no_d_id = $2 AND no_w_id = $3");
      statement.execute(
          "PREPARE select_order_customer as "
              + "SELECT o_c_id "
              + "FROM orders "
              + "WHERE o_id = $1 AND o_d_id = $2 AND o_w_id = $3");
      statement.execute(
          "PREPARE update_order as "
              + "UPDATE orders "
              + "SET o_carrier_id = $1 "
              + "WHERE o_id = $2 AND o_d_id = $3 AND o_w_id = $4");
      statement.execute(
          "PREPARE update_order_line as "
              + "UPDATE order_line "
              + "SET ol_delivery_d = NOW() "
              + "WHERE ol_o_id = $1 AND ol_d_id = $2 AND ol_w_id = $3");
      statement.execute(
          "PREPARE sum_order_line as "
              + "SELECT SUM(ol_amount) sm "
              + "FROM order_line "
              + "WHERE ol_o_id = $1 AND ol_d_id = $2 AND ol_w_id = $3");
      statement.execute(
          "PREPARE update_customer_bal as "
              + "UPDATE customer "
              + "SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + 1 "
              + "WHERE c_id = $2 AND c_d_id = $3 AND c_w_id = $4");
      statement.execute(
          "PREPARE count_order_line as "
              + "SELECT COUNT(DISTINCT (s_i_id)) "
              + "FROM order_line, stock "
              + "WHERE ol_w_id = $1 "
              + "AND ol_d_id = $2 "
              + "AND ol_o_id < $3 "
              + "AND ol_o_id >= $4 "
              + "AND s_w_id= $5 "
              + "AND s_i_id=ol_i_id "
              + "AND s_quantity < $6");
      statement.execute(
          "PREPARE select_stock_count as "
              + "SELECT count(*) FROM stock "
              + "WHERE s_w_id = $1 AND s_i_id = $2 "
              + "AND s_quantity < $3");
    }
  }
}
