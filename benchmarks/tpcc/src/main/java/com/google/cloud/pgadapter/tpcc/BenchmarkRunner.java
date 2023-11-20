package com.google.cloud.pgadapter.tpcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BenchmarkRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

  private final String connectionUrl;

  private boolean failed;

  BenchmarkRunner(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }

  @Override
  public void run() {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      prepareStatements(connection);
      runTransactions(connection);
    } catch (InterruptedException interruptedException) {
      LOG.info("Stopping benchmark runner");
    } catch (Throwable throwable) {
      LOG.error("Benchmark runner failed", throwable);
      failed = true;
    }
  }

  private void runTransactions(Connection connection) throws SQLException, InterruptedException {
    while (true) {
      selectOne(connection);
      //noinspection BusyWait
      Thread.sleep(100L);
    }
  }

  private void selectOne(Connection connection) throws SQLException {
    LOG.info("Running select 1");
    try (ResultSet resultSet = connection.createStatement().executeQuery("select 1")) {
      if (!resultSet.next()) {
        throw new RuntimeException("No rows returned for 'select 1'");
      }
      if (resultSet.getLong(1) != 1L) {
        throw new RuntimeException("Other number than 1 returned for 'select 1");
      }
      if (resultSet.next()) {
        throw new RuntimeException("More than one row returned for 'select 1'");
      }
    }
  }

  public boolean isFailed() {
    return failed;
  }

  private void prepareStatements(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "PREPARE insert_item as INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) values($1,$2,$3,$4,$5)");
      statement.execute(
          "PREPARE insert_customer as INSERT INTO customer "
              + "(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) "
              + "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)");
      statement.execute(
          "PREPARE insert_history(bigint,bigint,bigint,bigint,bigint,varchar) as "
              + "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) values($1, $2, $3, $4, $5, Now(), 10.0, $6)");
      statement.execute(
          "PREPARE insert_order as INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) values($1, $2, $3, $4, $5, $6, $7, $8)");
      statement.execute(
          "PREPARE insert_stock as INSERT INTO stock "
              + "(s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) "
              + "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)");
      statement.execute(
          "PREPARE insert_order_line as INSERT INTO order_line "
              + "(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
              + "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)");
    }
  }
}
