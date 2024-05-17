// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.latency;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.common.base.Stopwatch;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractJdbcRunner extends AbstractRunner {
  protected final DatabaseId databaseId;
  private long numNullValues;
  private long numNonNullValues;

  public AbstractJdbcRunner(DatabaseId databaseId) {
    this.databaseId = databaseId;
  }

  @Override
  public List<Duration> execute(
      TransactionType transactionType, int numClients, int numOperations, int waitMillis) {
    try {
      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        results.add(
            service.submit(
                () -> runBenchmark(createUrl(), transactionType, numOperations, waitMillis)));
      }
      return collectResults(service, results, numClients, numOperations);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }

  protected abstract String createUrl();

  private List<Duration> runBenchmark(
      String url, TransactionType transactionType, int numOperations, int waitMillis) {
    List<Duration> results = new ArrayList<>(numOperations);
    try (Connection connection = DriverManager.getConnection(url)) {
      // Execute one query to make sure everything has been warmed up.
      executeTransaction(connection, transactionType);

      for (int i = 0; i < numOperations; i++) {
        randomWait(waitMillis);
        results.add(executeTransaction(connection, transactionType));
        incOperations();
      }
    } catch (SQLException exception) {
      throw SpannerExceptionFactory.newSpannerException(exception);
    } catch (InterruptedException interruptedException) {
      throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
    }
    return results;
  }

  protected Duration executeTransaction(Connection connection, TransactionType transactionType)
      throws SQLException {
    Stopwatch watch = Stopwatch.createStarted();
    switch (transactionType) {
      case READ_ONLY:
        executeReadOnlyTransaction(connection, transactionType.getJdbcSql());
        break;
      case READ_WRITE:
        executeReadWriteTransaction(connection, transactionType.getJdbcSql());
        break;
    }
    return watch.elapsed();
  }

  protected void executeReadOnlyTransaction(Connection connection, String sql) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, ThreadLocalRandom.current().nextInt(100000));
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
            if (resultSet.getObject(i + 1) == null) {
              numNullValues++;
            } else {
              numNonNullValues++;
            }
          }
        }
      }
    }
  }

  protected void executeReadWriteTransaction(Connection connection, String sql)
      throws SQLException {
    connection.createStatement().execute("begin transaction");
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, generateRandomString());
      statement.setInt(2, ThreadLocalRandom.current().nextInt(100000));
      statement.executeUpdate();
    }
    connection.createStatement().execute("commit");
  }
}
