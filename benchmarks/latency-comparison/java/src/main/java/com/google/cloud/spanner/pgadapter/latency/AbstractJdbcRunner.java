package com.google.cloud.spanner.pgadapter.latency;

import com.google.common.base.Stopwatch;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Random;

public abstract class AbstractJdbcRunner implements BenchmarkRunner {
  private final Random random = new Random();
  private long numNullValues;
  private long numNonNullValues;

  protected Duration executeQuery(Connection connection, String sql) throws SQLException {
    Stopwatch watch = Stopwatch.createStarted();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, random.nextInt(100000));
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
            if (resultSet.getObject(i+1) == null) {
              numNullValues++;
            } else {
              numNonNullValues++;
            }
          }
        }
      }
    }
    return watch.elapsed();
  }

}
