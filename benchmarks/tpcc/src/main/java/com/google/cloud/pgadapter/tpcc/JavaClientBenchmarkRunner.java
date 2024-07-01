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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JavaClientBenchmarkRunner extends AbstractBenchmarkRunner {

  public enum Dialect {
    POSTGRESQL,
    GOOGLE_STANDARD_SQL,
  }

  private static final Logger LOG = LoggerFactory.getLogger(JavaClientBenchmarkRunner.class);
  private Spanner spanner;
  private final ThreadLocal<TransactionManager> transactionManagerThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<TransactionContext> transactionThreadLocal = new ThreadLocal<>();

  JavaClientBenchmarkRunner(
      Statistics statistics,
      TpccConfiguration tpccConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      SpannerConfiguration spannerConfiguration,
      Metrics metrics) {
    super(statistics, tpccConfiguration, pgAdapterConfiguration, spannerConfiguration, metrics);
  }

  void setup() throws SQLException, IOException {
    this.spanner = createSpanner();
  }

  void teardown() throws SQLException {
    spanner.close();
  }

  void executeStatement(String sql) throws SQLException {
    if (sql.equals("begin transaction")) {
      DatabaseClient databaseClient =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spannerConfiguration.getProject(),
                  spannerConfiguration.getInstance(),
                  spannerConfiguration.getDatabase()));
      transactionManagerThreadLocal.set(databaseClient.transactionManager());
      transactionThreadLocal.set(transactionManagerThreadLocal.get().begin());
    } else if (sql.equals("commit")) {
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        transactionManagerThreadLocal.get().commit();
        Duration executionDuration = stopwatch.elapsed();
        metrics.recordLatency(executionDuration.toMillis());
      } catch (AbortedException e) {
        // Ignore the aborted exception. Roll back the transaction.
        transactionManagerThreadLocal.get().rollback();
      } finally {
        transactionManagerThreadLocal.get().close();
      }
    } else if (sql.startsWith("rollback")) {
      transactionManagerThreadLocal.get().rollback();
      transactionManagerThreadLocal.get().close();
    } else if (sql.equals("set transaction read only")) {
      // No-op because TransactionManager only supports read/write transactions.
    }
  }

  void bindParams(Statement.Builder builder, Object[] params) throws SQLException {
    for (int i = 0; i < params.length; i++) {
      Object param = params[i];
      if (param instanceof BigDecimal) {
        builder.bind("p" + (i + 1)).to(Value.pgNumeric(((BigDecimal) param).toPlainString()));
      } else if (param instanceof String) {
        builder.bind("p" + (i + 1)).to((String) param);
      } else if (param instanceof Integer) {
        builder.bind("p" + (i + 1)).to((int) param);
      } else if (param instanceof Long) {
        builder.bind("p" + (i + 1)).to((long) param);
      } else {
        throw new SQLException(String.format("Unknown type for the parameter: %s", param));
      }
    }
  }

  Object[] paramQueryRow(String sql, Object[] params) throws SQLException {
    ParametersInfo parametersInfo = convertPositionalParametersToNamedParameters(sql, '?', "$");
    Statement.Builder builder = Statement.newBuilder(parametersInfo.sqlWithNamedParameters);
    bindParams(builder, params);
    Statement stmt = builder.build();

    Stopwatch stopwatch = Stopwatch.createStarted();
    ResultSet resultSet = transactionThreadLocal.get().executeQuery(stmt);
    // The above executeQuery() does not actually execute the query until we call
    // next() for the first time.
    boolean is_next_successful = resultSet.next();
    Duration executionDuration = stopwatch.elapsed();
    metrics.recordLatency(executionDuration.toMillis());

    if (!is_next_successful) {
      throw new RowNotFoundException(String.format("No results found for: %s", sql));
    }

    Object[] result = new Object[resultSet.getColumnCount()];
    for (int i = 0; i < result.length; i++) {
      result[i] = getObject(resultSet.getValue(i));
    }
    return result;
  }

  void executeParamStatement(String sql, Object[] params) throws SQLException {
    ParametersInfo parametersInfo = convertPositionalParametersToNamedParameters(sql, '?', "$");
    Statement.Builder builder = Statement.newBuilder(parametersInfo.sqlWithNamedParameters);
    bindParams(builder, params);
    Statement stmt = builder.build();

    Stopwatch stopwatch = Stopwatch.createStarted();
    transactionThreadLocal.get().executeUpdate(stmt);
    Duration executionDuration = stopwatch.elapsed();
    metrics.recordLatency(executionDuration.toMillis());
  }

  public static class ParametersInfo {
    public final int numberOfParameters;
    public final String sqlWithNamedParameters;

    ParametersInfo(int numberOfParameters, String sqlWithNamedParameters) {
      this.numberOfParameters = numberOfParameters;
      this.sqlWithNamedParameters = sqlWithNamedParameters;
    }
  }

  public ParametersInfo convertPositionalParametersToNamedParameters(
      String sql, char paramChar, String queryParameterPrefix) {
    final String namedParamPrefix = queryParameterPrefix;
    StringBuilder named = new StringBuilder(sql.length() + countOccurrencesOf(paramChar, sql));
    int index = 0;
    int paramIndex = 1;
    while (index < sql.length()) {
      char c = sql.charAt(index);
      // We have an assumption in any SQL strings: 1) comments do not contain question
      // marks; 2) string literals do not contain question marks. This is because we
      // believe all benchmark queries are hard-coded and none of them can cause the
      // above cases. If any of above cases happen, it should fail explicitly.
      if (c == paramChar) {
        named.append(namedParamPrefix).append(paramIndex);
        paramIndex++;
        index++;
      } else {
        named.append(c);
        index++;
      }
    }
    return new ParametersInfo(paramIndex - 1, named.toString());
  }

  /** Convenience method that is used to estimate the number of parameters in a SQL statement. */
  static int countOccurrencesOf(char c, String string) {
    int res = 0;
    for (int i = 0; i < string.length(); i++) {
      if (string.charAt(i) == c) {
        res++;
      }
    }
    return res;
  }

  // If getAsObject() in Struct can be a public method, then we should use it
  // instead of using the following approach.
  Object getObject(Value value) {
    if (value.getType().equals(Type.numeric())) {
      return value.getNumeric();
    } else if (value.getType().equals(Type.string())) {
      return value.getString();
    } else if (value.getType().equals(Type.int64())) {
      return value.getInt64();
    } else if (value.getType().equals(Type.float64())) {
      return value.getFloat64();
    } else if (value.getType().equals(Type.bool())) {
      return value.getBool();
    } else if (value.getType().equals(Type.pgNumeric())) {
      return new BigDecimal(value.getString());
    }
    return null;
  }

  List<Object[]> executeParamQuery(String sql, Object[] params) throws SQLException {
    ParametersInfo parametersInfo = convertPositionalParametersToNamedParameters(sql, '?', "$");
    List<Object[]> results = new ArrayList<>();

    Statement.Builder builder = Statement.newBuilder(parametersInfo.sqlWithNamedParameters);
    bindParams(builder, params);
    Statement stmt = builder.build();

    Stopwatch stopwatch = Stopwatch.createStarted();
    ResultSet resultSet = transactionThreadLocal.get().executeQuery(stmt);
    // The above executeQuery() does not actually execute the query until we call
    // next() for the first time.
    boolean is_next_successful = resultSet.next();
    Duration executionDuration = stopwatch.elapsed();
    metrics.recordLatency(executionDuration.toMillis());

    while (is_next_successful) {
      Object[] result = new Object[resultSet.getColumnCount()];
      for (int i = 0; i < result.length; i++) {
        result[i] = getObject(resultSet.getValue(i));
      }
      results.add(result);
      is_next_successful = resultSet.next();
    }

    return results;
  }

  private Spanner createSpanner() throws IOException {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setProjectId(spannerConfiguration.getProject())
            .setNumChannels(pgAdapterConfiguration.getNumChannels())
            .setSessionPoolOption(
                SessionPoolOptions.newBuilder()
                    .setMinSessions(pgAdapterConfiguration.getMinSessions())
                    .setMaxSessions(pgAdapterConfiguration.getMaxSessions())
                    .build());
    if (!Strings.isNullOrEmpty(pgAdapterConfiguration.getCredentials())) {
      builder.setCredentials(
          GoogleCredentials.fromStream(
              new FileInputStream(pgAdapterConfiguration.getCredentials())));
    }
    return builder.build().getService();
  }
}
