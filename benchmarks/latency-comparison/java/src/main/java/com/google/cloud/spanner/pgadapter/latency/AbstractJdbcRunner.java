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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.jdbc.JdbcSqlExceptionFactory.JdbcAbortedDueToConcurrentModificationException;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public abstract class AbstractJdbcRunner extends AbstractRunner {
  protected final DatabaseId databaseId;
  protected final boolean delayBeginTransaction;
  private long numNullValues;
  private long numNonNullValues;

  private Map<String, List<String>> ids;

  public AbstractJdbcRunner(DatabaseId databaseId, boolean delayBeginTransaction) {
    this.databaseId = databaseId;
    this.delayBeginTransaction = delayBeginTransaction;
  }

  @Override
  public List<Duration> execute(String sql, int numClients, int numOperations) {
    System.out.println();
    System.out.println("Running query benchmark");
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Execute a couple of queries to make sure everything has been warmed up.
      for (int i = 0; i < 4; i++) {
        executeQuery(connection, sql);
      }
    } catch (SQLException exception) {
      throw SpannerExceptionFactory.asSpannerException(exception);
    }

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
        int percentage = (i + 1) * 100 / numOperations;
        String dashes =
            "["
                + Strings.repeat("#", percentage / 5)
                + Strings.repeat(" ", 20 - percentage / 5)
                + "]";
        //        System.out.printf("\r%s %d%%", dashes, percentage);
        results.add(executeQuery(connection, sql));
      }
    } catch (SQLException exception) {
      throw SpannerExceptionFactory.newSpannerException(exception);
    }
    //    System.out.println();
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
  public List<Duration> executeTransaction(
      String query,
      String update,
      int numClients,
      int numTransactions,
      int numQueriesInTransaction,
      int numUpdatesInTransaction) {
    System.out.println();
    System.out.println("Running transactions benchmark");
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      ids = new HashMap<>();
      ids.put("singers", initIdCollection(connection, "singers"));
      ids.put("albums", initIdCollection(connection, "albums"));
      ids.put("venues", initIdCollection(connection, "venues"));
      ids.put("concerts", initIdCollection(connection, "concerts"));
    } catch (SQLException exception) {
      throw SpannerExceptionFactory.asSpannerException(exception);
    }
    try {
      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        results.add(
            service.submit(
                () ->
                    runTransactionBenchmark(
                        createUrl(),
                        query,
                        update,
                        numTransactions,
                        numQueriesInTransaction,
                        numUpdatesInTransaction)));
      }
      return collectResults(service, results, numClients, numTransactions);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }

  private List<Duration> runTransactionBenchmark(
      String url,
      String query,
      String update,
      int numTransactions,
      int numQueriesInTransaction,
      int numUpdatesInTransaction) {
    List<Duration> results = new ArrayList<>(numTransactions);
    try (Connection connection = DriverManager.getConnection(url)) {
      // Execute one query to make sure everything has been warmed up.
      executeQuery(connection, query);

      connection.setAutoCommit(false);
      for (int i = 0; i < numTransactions; i++) {
        int percentage = (i + 1) * 100 / numTransactions;
        String dashes =
            "["
                + Strings.repeat("#", percentage / 5)
                + Strings.repeat(" ", 20 - percentage / 5)
                + "]";
        //        System.out.printf("\r%s %d%%", dashes, percentage);
        Stopwatch watch = Stopwatch.createStarted();
        while (true) {
          try {
            Map<String, Object> singer1 = selectRandomRecord(connection, "singers");
            Map<String, Object> singer2 = selectRandomRecord(connection, "singers");
            Map<String, Object> album1 = selectRandomRecord(connection, "albums");
            Map<String, Object> album2 = selectRandomRecord(connection, "albums");
            Map<String, Object> album3 = selectRandomRecord(connection, "albums");
            Map<String, Object> album4 = selectRandomRecord(connection, "albums");
            Map<String, Object> album5 = selectRandomRecord(connection, "albums");
            Map<String, Object> venue1 = selectRandomRecord(connection, "venues");
            Map<String, Object> concert1 = selectRandomRecord(connection, "concerts");
            Map<String, Object> concert2 = selectRandomRecord(connection, "concerts");
            Map<String, Object> concert3 = selectRandomRecord(connection, "concerts");
            Map<String, Object> concert4 = selectRandomRecord(connection, "concerts");

            album1.put("singer_id", singer1.get("id"));
            album2.put("singer_id", singer2.get("id"));
            concert1.put("venue_id", venue1.get("id"));
            concert1.put("singer_id", singer1.get("id"));
            assertEquals(1, updateRecord(connection, "albums", album1));
            assertEquals(1, updateRecord(connection, "albums", album2));
            assertEquals(1, updateRecord(connection, "concerts", concert1));
            int numTracks = ThreadLocalRandom.current().nextInt(3) + 3;
            int[] expected = new int[numTracks];
            Arrays.fill(expected, 1);
            assertArrayEquals(
                expected, insertTracks(connection, album3.get("id").toString(), numTracks));

            connection.commit();
            break;
          } catch (JdbcAbortedDueToConcurrentModificationException aborted) {
            connection.rollback();
            // retry transaction after a delay.
            if (aborted.getCause().getRetryDelayInMillis() > 0L) {
              //noinspection BusyWait
              Thread.sleep(aborted.getCause().getRetryDelayInMillis());
            }
          }
        }
        results.add(watch.elapsed());
      }
    } catch (Throwable exception) {
      exception.printStackTrace();
      throw SpannerExceptionFactory.newSpannerException(exception);
    }
    //    System.out.println();
    return results;
  }

  private Map<String, Object> selectRandomRecord(Connection connection, String table)
      throws SQLException {
    List<String> idList = ids.get(table);
    String id = idList.get(ThreadLocalRandom.current().nextInt(idList.size()));
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(String.format("select * from %s where id=?", table))) {
      preparedStatement.setString(1, id);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          Map<String, Object> result = new HashMap<>();
          for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
            result.put(resultSet.getMetaData().getColumnName(col), resultSet.getObject(col));
          }
          return result;
        }
      }
    }
    throw new SQLException(String.format("record in table %s not found: %s", table, id));
  }

  private int updateRecord(Connection connection, String table, Map<String, Object> record)
      throws SQLException {
    List<Entry<String, Object>> entries =
        record.entrySet().stream().sorted(Entry.comparingByKey()).collect(Collectors.toList());
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(
            String.format(
                "update %s set %s where id=?",
                table,
                entries.stream()
                    .filter(o -> !o.getKey().equals("id"))
                    .map(o -> o.getKey() + "=?")
                    .collect(Collectors.joining(", "))))) {
      int index = 0;
      for (Entry<String, Object> entry : entries) {
        if (!entry.getKey().equals("id")) {
          preparedStatement.setObject(++index, entry.getValue());
        }
      }
      preparedStatement.setObject(++index, record.get("id"));
      return preparedStatement.executeUpdate();
    }
  }

  private int[] insertTracks(Connection connection, String albumId, int numTracks)
      throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "insert into tracks (id, track_number, title, sample_rate, created_at, updated_at) values (?, ?, ?, ?, ?, ?)")) {
      for (int row = 0; row < numTracks; row++) {
        statement.setString(1, albumId);
        statement.setLong(2, ThreadLocalRandom.current().nextLong());
        statement.setString(3, UUID.randomUUID().toString());
        statement.setDouble(4, ThreadLocalRandom.current().nextDouble());
        statement.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
        statement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
        statement.addBatch();
      }
      return statement.executeBatch();
    }
  }

  private List<String> initIdCollection(Connection connection, String tableName)
      throws SQLException {
    List<String> result = new ArrayList<>();
    try (ResultSet resultSet =
        connection.createStatement().executeQuery(String.format("select id from %s", tableName))) {
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
    }
    return result;
  }
}
