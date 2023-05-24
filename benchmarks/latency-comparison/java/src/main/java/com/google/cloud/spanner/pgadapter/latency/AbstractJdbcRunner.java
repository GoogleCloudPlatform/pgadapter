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
import com.google.cloud.spanner.jdbc.JdbcSqlExceptionFactory.JdbcAbortedDueToConcurrentModificationException;
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
  protected final boolean delayBeginTransaction;
  private long numNullValues;
  private long numNonNullValues;

  public AbstractJdbcRunner(DatabaseId databaseId, boolean delayBeginTransaction) {
    this.databaseId = databaseId;
    this.delayBeginTransaction = delayBeginTransaction;
  }

  @Override
  public List<Duration> execute(String sql, int numClients, int numOperations) {
    try {
      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        results.add(service.submit(() -> runBenchmark(createUrl(), sql, numOperations)));
      }
      return collectResults(service, results, numClients, numOperations);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }

  protected abstract String createUrl();

  private List<Duration> runBenchmark(String url, String sql, int numOperations) {
    List<Duration> results = new ArrayList<>(numOperations);
    try (Connection connection = DriverManager.getConnection(url)) {
      // Execute one query to make sure everything has been warmed up.
      executeQuery(connection, sql);

      for (int i = 0; i < numOperations; i++) {
        results.add(executeQuery(connection, sql));
      }
    } catch (SQLException exception) {
      throw SpannerExceptionFactory.newSpannerException(exception);
    }
    return results;
  }

  protected Duration executeQuery(Connection connection, String sql) throws SQLException {
    Stopwatch watch = Stopwatch.createStarted();
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
    return watch.elapsed();
  }
  
  protected Duration executeUpdate(Connection connection, String sql) throws SQLException {
    byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(50) + 50];
    ThreadLocalRandom.current().nextBytes(bytes);
    Stopwatch watch = Stopwatch.createStarted();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, new String(bytes));
      statement.setInt(2, ThreadLocalRandom.current().nextInt(100000));
      statement.executeUpdate();
    }
    return watch.elapsed();
  }

  @Override
  public List<Duration> executeTransaction(String query, String update, int numClients, int numTransactions, int numQueriesInTransaction, int numUpdatesInTransaction) {
    try {
      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        results.add(service.submit(() -> runTransactionBenchmark(createUrl(), query, update, numTransactions, numQueriesInTransaction, numUpdatesInTransaction)));
      }
      return collectResults(service, results, numClients, numTransactions);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }

  private List<Duration> runTransactionBenchmark(String url, String query, String update, int numTransactions, int numQueriesInTransaction, int numUpdatesInTransaction) throws InterruptedException {
    List<Duration> results = new ArrayList<>(numTransactions);
    try (Connection connection = DriverManager.getConnection(url)) {
      // Execute one query to make sure everything has been warmed up.
      executeQuery(connection, query);

      connection.setAutoCommit(false);
      for (int i = 0; i < numTransactions; i++) {
        Stopwatch watch = Stopwatch.createStarted();
        while (true) {
          try {
            for (int q = 0; q < numQueriesInTransaction; q++) {
              executeQuery(connection, query);
            }
            for (int u = 0; u < numUpdatesInTransaction; u++) {
              executeUpdate(connection, update);
            }
            connection.commit();
            break;
          } catch (JdbcAbortedDueToConcurrentModificationException aborted) {
            // retry transaction after a delay.
            if (aborted.getCause().getRetryDelayInMillis() > 0L) {
              //noinspection BusyWait
              Thread.sleep(aborted.getCause().getRetryDelayInMillis());
            }
          }
        }
        System.out.printf("Finished transaction %d/%d\n", i, numTransactions);
        results.add(watch.elapsed());
      }
    } catch (Throwable exception) {
      exception.printStackTrace();
      throw SpannerExceptionFactory.newSpannerException(exception);
    }
    System.out.println("Finished running benchmark");
    return results;
  }

}
