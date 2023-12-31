package com.google.cloud.pgadapter.benchmark.dataloader;

import com.google.cloud.pgadapter.benchmark.BenchmarkApplication;
import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkApplication.class);

  private final String connectionUrl;

  private final BenchmarkConfiguration benchmarkConfiguration;

  private final ListeningExecutorService loadDataExecutor;

  private final ListeningExecutorService rowProducerExecutor;

  private final DataLoadStatus status;

  public DataLoader(
      DataLoadStatus status, String connectionUrl, BenchmarkConfiguration benchmarkConfiguration) {
    this.connectionUrl = connectionUrl;
    this.benchmarkConfiguration = benchmarkConfiguration;
    this.loadDataExecutor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(benchmarkConfiguration.getLoadDataThreads()));
    this.rowProducerExecutor =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(benchmarkConfiguration.getLoadDataThreads()));
    this.status = status;
  }

  @Override
  public void close() {
    this.loadDataExecutor.shutdown();
    this.rowProducerExecutor.shutdown();
  }

  public long loadData() throws Exception {
    if (benchmarkConfiguration.isTruncateBeforeLoad()) {
      truncate();
    }

    long totalRowCount = 0L;
    int recordCount = benchmarkConfiguration.getRecordCount();
    totalRowCount += recordCount;
    Future<Long> allTypesFuture =
        loadDataExecutor.submit(
            () -> {
              long rowCount = loadTable(new AllTypesRowProducer(status, recordCount));
              LOG.info("Loaded {} records", rowCount);
              return rowCount;
            });

    allTypesFuture.get();
    loadDataExecutor.shutdown();
    if (!loadDataExecutor.awaitTermination(60L, TimeUnit.SECONDS)) {
      throw new TimeoutException("Loading data timed out while waiting for executor to shut down.");
    }
    return totalRowCount;
  }

  long loadTable(AbstractRowProducer rowProducer) throws SQLException, IOException {
    PipedWriter writer = new PipedWriter();
    try (PipedReader reader = new PipedReader(writer, 30_000);
        Connection connection = createConnection()) {
      CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
      rowProducer.asyncWriteRows(this.rowProducerExecutor, writer);
      return copyManager.copyIn(
          String.format(
              "copy \"%s\" (%s) from stdin (format csv, delimiter ',', quote '''', null 'null')",
              rowProducer.getTable(), rowProducer.getColumns()),
          reader);
    }
  }

  void truncate() throws SQLException {
    try (Connection connection = createConnection();
        Statement statement = connection.createStatement()) {
      LOG.info("truncating benchmark_all_types");
      statement.execute("truncate table benchmark_all_types");
      status.setTruncatedAllTypes();
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
