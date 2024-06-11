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

import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.common.base.Stopwatch;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcBenchmarkRunner extends AbstractBenchmarkRunner {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcBenchmarkRunner.class);

  private final String connectionUrl;

  private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<Statement> statementThreadLocal = new ThreadLocal<>();

  JdbcBenchmarkRunner(
      Statistics statistics,
      String connectionUrl,
      TpccConfiguration tpccConfiguration,
      Metrics metrics) {
    super(statistics, tpccConfiguration, metrics);
    this.connectionUrl = connectionUrl;
  }

  void setup() throws SQLException {
    connectionThreadLocal.set(DriverManager.getConnection(connectionUrl));
    statementThreadLocal.set(connectionThreadLocal.get().createStatement());
  }

  void teardown() throws SQLException {
    statementThreadLocal.get().close();
    connectionThreadLocal.get().close();
  }

  public enum QueryRowMode {
    REQUIRE_ONE,
    ALLOW_MORE_THAN_ONE,
    ALLOW_LESS_THAN_ONE,
  }

  Object[] paramQueryRow(String sql, Object[] params) throws SQLException {
    Object[] row;
    try (PreparedStatement statement = connectionThreadLocal.get().prepareStatement(sql)) {
      int index = 0;
      setParams(statement, params);
      Stopwatch stopwatch = Stopwatch.createStarted();
      row = paramQueryRow(QueryRowMode.ALLOW_MORE_THAN_ONE, statement);
      Duration executionDuration = stopwatch.elapsed();
      metrics.recordLatency(executionDuration.toMillis());
    }
    return row;
  }

  Object[] paramQueryRow(QueryRowMode queryRowMode, PreparedStatement statement)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery()) {
      if (!resultSet.next()) {
        if (queryRowMode == QueryRowMode.ALLOW_LESS_THAN_ONE) {
          return null;
        } else {
          throw new RowNotFoundException(String.format("No results found for: %s", statement));
        }
      }
      Object[] result = new Object[resultSet.getMetaData().getColumnCount()];
      for (int i = 0; i < result.length; i++) {
        result[i] = resultSet.getObject(i + 1);
      }
      if (queryRowMode != QueryRowMode.ALLOW_MORE_THAN_ONE && resultSet.next()) {
        throw new SQLException(String.format("More than one result found for: %s", statement));
      }
      return result;
    }
  }

  void setParams(PreparedStatement statement, Object[] params) throws SQLException {
    int index = 0;
    for (Object param : params) {
      if (param instanceof BigDecimal) {
        statement.setBigDecimal(++index, (BigDecimal) param);
      } else if (param instanceof String) {
        statement.setString(++index, (String) param);
      } else if (param instanceof Integer) {
        statement.setInt(++index, (int) param);
      } else if (param instanceof Long) {
        statement.setLong(++index, (long) param);
      } else {
        throw new SQLException(String.format("Unknown type for the parameter: %s", param));
      }
    }
  }

  void executeStatement(String dml) throws SQLException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    statementThreadLocal.get().execute(dml);
    Duration executionDuration = stopwatch.elapsed();
    metrics.recordLatency(executionDuration.toMillis());
  }

  void executeParamStatement(String sql, Object[] params) throws SQLException {
    try (PreparedStatement statement = connectionThreadLocal.get().prepareStatement(sql)) {
      setParams(statement, params);
      Stopwatch stopwatch = Stopwatch.createStarted();
      statement.execute();
      Duration executionDuration = stopwatch.elapsed();
      metrics.recordLatency(executionDuration.toMillis());
    }
  }

  List<Object[]> executeParamQuery(String sql, Object[] params) throws SQLException {
    List<Object[]> results = new ArrayList<>();
    try (PreparedStatement statement = connectionThreadLocal.get().prepareStatement(sql)) {
      setParams(statement, params);
      Stopwatch stopwatch = Stopwatch.createStarted();
      ResultSet resultSet = statement.executeQuery();
      Duration executionDuration = stopwatch.elapsed();
      metrics.recordLatency(executionDuration.toMillis());
      while (resultSet.next()) {
        Object[] result = new Object[resultSet.getMetaData().getColumnCount()];
        for (int i = 0; i < result.length; i++) {
          result[i] = resultSet.getObject(i + 1);
        }
        results.add(result);
      }
    }
    return results;
  }
}
