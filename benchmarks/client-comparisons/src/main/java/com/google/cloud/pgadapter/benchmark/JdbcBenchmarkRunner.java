package com.google.cloud.pgadapter.benchmark;

import static org.junit.Assert.assertEquals;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.common.base.Stopwatch;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcBenchmarkRunner extends AbstractBenchmarkRunner {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcBenchmarkRunner.class);

  private final String connectionUrl;

  JdbcBenchmarkRunner(
      String name,
      Statistics statistics,
      String connectionUrl,
      BenchmarkConfiguration benchmarkConfiguration) {
    super(name, statistics, benchmarkConfiguration);
    this.connectionUrl = connectionUrl;
  }

  List<String> loadIdentifiers() {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      List<String> result = new ArrayList<>(benchmarkConfiguration.getRecordCount());
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select id from benchmark_all_types")) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      return result;
    } catch (SQLException sqlException) {
      throw new RuntimeException(sqlException);
    }
  }

  @Override
  String getParameterName(int index) {
    return "?";
  }

  @Override
  void runQuery(
      String sql,
      boolean autoCommit,
      int iterations,
      int numRows,
      ConcurrentLinkedQueue<Duration> durations) {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      connection.setAutoCommit(autoCommit);
      for (int n = 0; n < iterations; n++) {
        if (!benchmarkConfiguration.getMaxRandomWait().isZero()) {
          long sleepDuration =
              ThreadLocalRandom.current()
                  .nextLong(benchmarkConfiguration.getMaxRandomWait().toMillis());
          Thread.sleep(sleepDuration);
        }
        Stopwatch watch = Stopwatch.createStarted();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          if (numRows == 1) {
            statement.setString(1, getRandomId());
          } else {
            statement.setArray(1, connection.createArrayOf("text", getRandomIds(numRows)));
          }
          try (ResultSet resultSet = statement.executeQuery()) {
            int rowCount = 0;
            while (resultSet.next()) {
              for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
                if (connection.isWrapperFor(CloudSpannerJdbcConnection.class)
                    && !Objects.equals(
                        String.class.getName(), resultSet.getMetaData().getColumnClassName(col))) {
                  Object value1 = resultSet.getObject(col);
                  Object value2 = resultSet.getObject(resultSet.getMetaData().getColumnLabel(col));
                  if (value1 != null
                      && value2 != null
                      && value1.getClass().isArray()
                      && value2.getClass().isArray()) {
                    assertEquals(Array.getLength(value1), Array.getLength(value2));
                    for (int i = 0; i < Array.getLength(value1); i++) {
                      assertEquals(Array.get(value1, i), Array.get(value2, i));
                    }
                  } else {
                    assertEquals(value1, value2);
                  }
                } else {
                  assertEquals(
                      resultSet.getString(col),
                      resultSet.getString(resultSet.getMetaData().getColumnLabel(col)));
                }
              }
              rowCount++;
            }
            assertEquals(numRows, rowCount);
          }
        }
        if (!autoCommit) {
          connection.commit();
        }
        statistics.incOperations();
        durations.add(watch.elapsed());
      }
    } catch (SQLException exception) {
      throw new RuntimeException(exception);
    } catch (InterruptedException interruptedException) {
      throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
    }
  }
}
