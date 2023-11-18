package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

class BenchmarkRunner {
  private static final Logger log = Logger.getLogger(BenchmarkRunner.class.getName());
  
  private final String connectionUrl;
  
  BenchmarkRunner(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }
  
  void prepareStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      prepareStatements(connection);
    }
  }
  
  void prepareStatements(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("PREPARE insert_item as INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) values($1,$2,$3,$4,$5)");
      statement.execute("PREPARE insert_customer as INSERT INTO customer " 
              + "(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " 
              + "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)");
      statement.execute("PREPARE insert_history(bigint,bigint,bigint,bigint,bigint,varchar) as " 
          + "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) values($1, $2, $3, $4, $5, Now(), 10.0, $6)");
      statement.execute("PREPARE insert_order as INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) values($1, $2, $3, $4, $5, $6, $7, $8)");
      statement.execute("PREPARE insert_stock as INSERT INTO stock " 
          + "(s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) " 
          + "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)");
      statement.execute("PREPARE insert_order_line as INSERT INTO order_line " 
          + "(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) " 
          + "values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)");
    }
  }
  
  void loadWarehouse() throws SQLException, IOException {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
      PipedWriter writer = new PipedWriter();
      try (PipedReader reader = new PipedReader(writer)) {
        Thread thread = new Thread("copy-warehouse") {
          public void run() {
            try {
              writer.write("1,'name','street1','street2','city','zz','9999zz',0.2,0.2\n");
            } catch (IOException e) {
              e.printStackTrace();
            } finally {
              try {
                writer.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        };
        thread.start();
        // (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
        copyManager.copyIn("copy warehouse from stdin (format csv, delimiter ',', quote '''')", reader);
      }
    }
  }
  
  void loadItemTables() {
    
    
    // function load_item_tables(drv, con, tid, threads, table_num)
    //   log('Start - loading the table `item' .. table_num .. '` by thread #' .. tid .. '\n')
    //   local i = table_num
    //
    //   -- Break up inserting 100k items to multiple queries.
    //   assert(MAXITEMS % ITEMS_PER_QUERY == 0)
    //   num_queries = MAXITEMS / ITEMS_PER_QUERY
    //
    //   for k = tid % threads, num_queries - 1, threads do
    //      local buf = sysbench.opt.enable_pg_compat_mode == 1 and {} or {"START BATCH DML;"}
    //      for j = 1 + (k * ITEMS_PER_QUERY), (k + 1) * ITEMS_PER_QUERY do
    //         local i_im_id = sysbench.rand.uniform(1,10000)
    //         local i_price = sysbench.rand.uniform_double()*100+1
    //         -- i_name is not generated as prescribed by standard, but we want to provide a better compression
    //         local i_name  = string.format("item-%d-%f-%s", i_im_id, i_price, sysbench.rand.string("@@@@@"))
    //         local i_data  = string.format("data-%s-%s", i_name, sysbench.rand.string("@@@@@"))
    //
    //         query = string.format(
    //            "EXECUTE insert_item"..i.." (%d,%d,'%s',%f,'%s');",
    //            j, i_im_id, i_name:sub(1,24), i_price, i_data:sub(1,50))
    //         buf[#buf+1] = query
    //      end
    //      if sysbench.opt.enable_pg_compat_mode ~= 1 then
    //         buf[#buf+1] = "RUN BATCH;"
    //      end
    //      con:query(table.concat( buf ))
    //   end
    //   log('End - loading the table `item' .. table_num .. '` by thread #' .. tid .. '\n')
    //end
  }
  
  void createSchema() throws IOException, SQLException {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Check if the tables already exist.
      try (ResultSet resultSet = connection.createStatement().executeQuery("select count(1) from information_schema.tables where table_schema='public' and table_name='warehouse'")) {
        if (resultSet.next() && resultSet.getInt(1) == 1) {
          log.info("Skipping schema creation as tables already exist");
          return;
        }
      }
      
      URL url = BenchmarkRunner.class.getResource("/schema.sql");
      Path path = Paths.get(Objects.requireNonNull(url).getPath());
      String ddl = Files.readString(path);
      log.info("Executing schema statements");
      String[] statements = ddl.split(";");
      for (String statement : statements) {
        log.info(statement);
        connection.createStatement().execute(statement);
      }
    }
  }

}
