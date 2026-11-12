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

package com.google.cloud.spanner.pgadapter;

import static com.google.cloud.spanner.connection.AbstractMockServerTest.RANDOM_RESULT_SET_ROW_COUNT;
import static com.google.cloud.spanner.connection.AbstractMockServerTest.SELECT_RANDOM_STATEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.common.base.Strings;
import com.google.protobuf.AbstractMessage;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.RollbackRequest;
import io.grpc.Status;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.jdbc.PSQLSavepoint;

@RunWith(JUnit4.class)
public class SavepointMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @Before
  public void setupRandomResults() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_RANDOM_STATEMENT,
            new RandomResultSetGenerator(RANDOM_RESULT_SET_ROW_COUNT, Dialect.POSTGRESQL)
                .generate()));
  }

  @After
  public void clearRequests() {
    mockSpanner.clearRequests();
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  private Connection createConnection() throws SQLException {
    return DriverManager.getConnection(createUrl());
  }

  @Test
  public void testCreateSavepoint() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      connection.setSavepoint("s1");

      // PostgreSQL allows multiple savepoints with the same name.
      connection.setSavepoint("s1");

      // The PostgreSQL JDBC driver also allows named savepoints without a name...
      PSQLSavepoint savepoint = (PSQLSavepoint) connection.setSavepoint(null);
      assertTrue(savepoint.getPGName(), savepoint.getPGName().startsWith("JDBC_SAVEPOINT_"));

      // Test invalid identifiers.
      assertThrows(SQLException.class, () -> connection.setSavepoint(""));
      assertThrows(SQLException.class, () -> connection.setSavepoint("1"));
      assertThrows(SQLException.class, () -> connection.setSavepoint("-foo"));
      assertThrows(SQLException.class, () -> connection.setSavepoint(Strings.repeat("t", 129)));
    }
  }

  @Test
  public void testReleaseSavepoint() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      Savepoint s1 = connection.setSavepoint("s1");
      connection.releaseSavepoint(s1);
      assertThrows(SQLException.class, () -> connection.releaseSavepoint(s1));

      Savepoint s1_2 = connection.setSavepoint("s1");
      Savepoint s2 = connection.setSavepoint("s2");
      connection.releaseSavepoint(s1_2);
      // Releasing a savepoint also removes all savepoints after it.
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.releaseSavepoint(s2));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());
      // Rollback the entire transaction to make the connection usable again.
      connection.rollback();

      // PostgreSQL allows multiple savepoints with the same name.
      Savepoint savepoint1 = connection.setSavepoint("s1");
      Savepoint savepoint2 = connection.setSavepoint("s2");
      Savepoint savepoint1_2 = connection.setSavepoint("s1");
      connection.releaseSavepoint(savepoint1_2);
      connection.releaseSavepoint(savepoint2);
      connection.releaseSavepoint(savepoint1);
      assertThrows(SQLException.class, () -> connection.releaseSavepoint(savepoint1));
    }
  }

  @Test
  public void testRollbackToSavepoint() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      Savepoint s1 = connection.setSavepoint("s1");
      connection.rollback(s1);
      // Rolling back to a savepoint does not remove it, so we can roll back multiple times to the
      // same savepoint.
      connection.rollback(s1);

      Savepoint s2 = connection.setSavepoint("s2");
      connection.rollback(s1);
      // Rolling back to a savepoint removes all savepoints after it.
      assertThrows(SQLException.class, () -> connection.rollback(s2));
      // Rollback again to make the connection usable again.
      connection.rollback();

      // PostgreSQL allows multiple savepoints with the same name.
      connection.setSavepoint("s1");
      Savepoint savepoint2 = connection.setSavepoint("s2");
      Savepoint savepoint1 = connection.setSavepoint("s1");
      connection.rollback(savepoint1);
      connection.rollback(savepoint2);
      connection.rollback(savepoint1);
      connection.rollback(savepoint1);
      connection.releaseSavepoint(savepoint1);
      assertThrows(SQLException.class, () -> connection.rollback(savepoint1));
    }
  }

  @Test
  public void testSavepointInAutoCommit() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(true);
      assertThrows(SQLException.class, () -> connection.setSavepoint("s1"));

      // Starting a 'manual' transaction in autocommit mode does not enable savepoints in the PG
      // JDBC driver.
      connection.createStatement().execute("begin transaction");
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.setSavepoint("s1"));
      assertEquals(SQLState.NoActiveSqlTransaction.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testRollbackToSavepointInReadOnlyTransaction() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setReadOnly(true);
      connection.setAutoCommit(false);

      // Read-only transactions also support savepoints, but they do not do anything. This feature
      // is here purely for compatibility.
      Savepoint s1 = connection.setSavepoint("s1");
      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }

      connection.rollback(s1);
      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }
      // Committing a read-only transaction is necessary to mark the end of the transaction.
      // It is a no-op on Cloud Spanner.
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      BeginTransactionRequest beginRequest =
          mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
      assertTrue(beginRequest.getOptions().hasReadOnly());
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointInReadWriteTransaction() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      Savepoint s1 = connection.setSavepoint("s1");
      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }

      connection.rollback(s1);
      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }
      connection.commit();

      // Read/write transactions are started with inlined Begin transaction options.
      assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

      List<AbstractMessage> requests =
          mockSpanner.getRequests().stream()
              .filter(
                  request ->
                      request instanceof ExecuteSqlRequest
                          || request instanceof RollbackRequest
                          || request instanceof CommitRequest)
              .collect(Collectors.toList());
      assertEquals(4, requests.size());
      int index = 0;
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(RollbackRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(CommitRequest.class, requests.get(index++).getClass());
    }
  }

  @Test
  public void testRollbackToSavepointWithDmlStatements() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      // First do a query that is included in the transaction.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }
      // Set a savepoint and execute a couple of DML statements.
      Savepoint s1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint s2 = connection.setSavepoint("s2");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      // Rollback the last DML statement and commit.
      connection.rollback(s2);

      connection.commit();

      // Read/write transactions are started with inlined Begin transaction options.
      assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(5, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

      List<AbstractMessage> requests =
          mockSpanner.getRequests().stream()
              .filter(
                  request ->
                      request instanceof ExecuteSqlRequest
                          || request instanceof RollbackRequest
                          || request instanceof CommitRequest)
              .collect(Collectors.toList());
      assertEquals(7, requests.size());
      int index = 0;
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(RollbackRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(CommitRequest.class, requests.get(index++).getClass());
    }
  }

  @Test
  public void testRollbackToSavepointFails() throws SQLException {
    Statement statement = Statement.of("select * from foo where bar=true");
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }
      // Set a savepoint and execute a couple of DML statements.
      Savepoint s1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      // Change the result of the initial query.
      mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
      // Rollback to before the DML statements.
      // This will succeed as long as we don't execute any further statements.
      connection.rollback(s1);

      // Trying to commit the transaction or execute any other statements on the transaction will
      // fail.
      assertThrows(SQLException.class, connection::commit);

      // Read/write transactions are started with inlined Begin transaction options.
      assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(2, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

      List<AbstractMessage> requests =
          mockSpanner.getRequests().stream()
              .filter(
                  request ->
                      request instanceof ExecuteSqlRequest
                          || request instanceof RollbackRequest
                          || request instanceof CommitRequest)
              .collect(Collectors.toList());
      assertEquals(6, requests.size());
      int index = 0;
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(RollbackRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(RollbackRequest.class, requests.get(index++).getClass());
    }
  }

  @Test
  public void testRollbackToSavepointSucceedsWithRollback() throws SQLException {
    Statement statement = Statement.of("select * from foo where bar=true");
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }
      // Change the result of the initial query and set a savepoint.
      Savepoint s1 = connection.setSavepoint("s1");
      mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
      // This will succeed as long as we don't execute any further statements.
      connection.rollback(s1);

      // Rolling back the transaction should now be a no-op, as it has already been rolled back.
      connection.rollback();

      // Read/write transactions are started with inlined Begin transaction options.
      assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    }
  }

  @Test
  public void testMultipleRollbacksWithChangedResults() throws SQLException {
    Statement statement = Statement.of("select * from foo where bar=true");
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }
      Savepoint s1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint s2 = connection.setSavepoint("s2");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());

      // Change the result of the initial query to make sure that any retry will fail.
      mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
      // This will succeed as long as we don't execute any further statements.
      connection.rollback(s2);
      // Rolling back one further should also work.
      connection.rollback(s1);

      // Rolling back the transaction should now be a no-op, as it has already been rolled back.
      connection.rollback();

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    }
  }

  @Test
  public void testMultipleRollbacks() throws SQLException {
    Statement statement = Statement.of("select * from foo where bar=true");
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }
      Savepoint s1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint s2 = connection.setSavepoint("s2");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());

      // First roll back one step and then one more.
      connection.rollback(s2);
      connection.rollback(s1);

      // This will only commit the SELECT query.
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

      List<AbstractMessage> requests =
          mockSpanner.getRequests().stream()
              .filter(
                  request ->
                      request instanceof ExecuteSqlRequest
                          || request instanceof RollbackRequest
                          || request instanceof CommitRequest)
              .collect(Collectors.toList());
      assertEquals(6, requests.size());
      int index = 0;
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(RollbackRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(CommitRequest.class, requests.get(index++).getClass());
    }
  }

  @Test
  public void testRollbackBatchDml() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint s1 = connection.setSavepoint("s1");
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.executeBatch();
      }
      Savepoint s2 = connection.setSavepoint("s2");

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint s3 = connection.setSavepoint("s3");
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.executeBatch();
      }
      connection.setSavepoint("s4");

      connection.rollback(s2);

      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
      assertEquals(3, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));

      List<AbstractMessage> requests =
          mockSpanner.getRequests().stream()
              .filter(
                  request ->
                      request instanceof ExecuteSqlRequest
                          || request instanceof RollbackRequest
                          || request instanceof CommitRequest
                          || request instanceof ExecuteBatchDmlRequest)
              .collect(Collectors.toList());
      assertEquals(8, requests.size());
      int index = 0;
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteBatchDmlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteBatchDmlRequest.class, requests.get(index++).getClass());
      assertEquals(RollbackRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteSqlRequest.class, requests.get(index++).getClass());
      assertEquals(ExecuteBatchDmlRequest.class, requests.get(index++).getClass());
      assertEquals(CommitRequest.class, requests.get(index++).getClass());
    }
  }

  @Test
  public void testRollbackToSavepointWithoutInternalRetries() throws SQLException {
    Statement statement = Statement.of("select * from foo where bar=true");
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      connection.createStatement().execute("set spanner.retry_aborts_internally=false");

      Savepoint s1 = connection.setSavepoint("s1");
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }
      // This should work.
      connection.rollback(s1);
      // Resuming after a rollback is not supported without internal retries enabled.
      assertThrows(
          SQLException.class,
          () -> connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql()));
    }
  }

  @Test
  public void testRollbackToSavepointWithoutInternalRetriesInReadOnlyTransaction()
      throws SQLException {
    Statement statement = Statement.of("select * from foo where bar=true");
    int numRows = 10;
    RandomResultSetGenerator generator = new RandomResultSetGenerator(numRows, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(statement, generator.generate()));
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      connection.createStatement().execute("set spanner.retry_aborts_internally=false");
      connection.commit();
      connection.setReadOnly(true);

      Savepoint s1 = connection.setSavepoint("s1");
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }

      // Both rolling back and resuming after a rollback are supported in a read-only transaction,
      // even if internal retries have been disabled.
      connection.rollback(s1);
      try (ResultSet resultSet = connection.createStatement().executeQuery(statement.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(numRows, count);
      }
    }
  }

  @Test
  public void testRollbackToSavepointAfterStatementError() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testMultipleSavepointsAfterStatementError() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint2 = connection.setSavepoint("s2");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rollback to savepoint2 only rolls back the failed statement, keeping the statements before
      // savepoint2.
      connection.rollback(savepoint2);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointAfterStatementErrorThenRollback() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.setSavepoint("s1");
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Aborting the transaction instead of rolling back to savepoint.
      connection.rollback();

      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointAfterStatementErrorThenCommit() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.setSavepoint("s1");
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // In PostgreSQL, committing an aborted transaction rolls it back.
      connection.commit();

      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointFailsThenRecoversWithValidSavepoint() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rolling back to a non-existent savepoint fails and keeps the transaction in ABORTED state.
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("rollback to savepoint non_existent"));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());

      // Rolling back to the valid savepoint savepoint1 recovers the transaction.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointPreservesSessionSettings() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().execute("set timezone = 'UTC'");
      Savepoint savepoint1 = connection.setSavepoint("s1");
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      connection.rollback(savepoint1);

      try (ResultSet resultSet = connection.createStatement().executeQuery("show time zone")) {
        assertTrue(resultSet.next());
        assertEquals("UTC", resultSet.getString(1));
      }
      connection.commit();
    }
  }

  @Test
  public void testSavepointsDoNotLeakAcrossTransactionsAfterCommit() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      // Create > 16 savepoints in transaction 1 to test both isolation and the resize threshold.
      Savepoint lastSavepoint = null;
      for (int index = 0; index < 25; index++) {
        lastSavepoint = connection.setSavepoint("savepoint_" + index);
      }
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
      mockSpanner.clearRequests();

      // Transaction 2 starts with no savepoints.
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Because transaction 2 has no savepoints, Spanner should be rolled back eagerly.
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));

      // Attempting to rollback to a savepoint from transaction 1 should fail.
      final Savepoint savepointFromTransaction1 = lastSavepoint;
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.rollback(savepointFromTransaction1));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testSavepointsDoNotLeakAcrossTransactionsAfterRollback() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      Savepoint savepointFromTransaction1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.rollback();

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      mockSpanner.clearRequests();

      // Transaction 2 starts with no savepoints.
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Because transaction 2 has no savepoints, Spanner should be rolled back eagerly.
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));

      // Attempting to rollback to savepoint from transaction 1 should fail.
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.rollback(savepointFromTransaction1));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testRollbackToSavepointThenImmediateCommit() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      connection.rollback(savepoint1);
      // Immediately commit without executing further statements.
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testMultipleRollbacksToSameSavepointAfterErrors() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      // First error and rollback to savepoint1.
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));
      connection.rollback(savepoint1);

      // Second error and rollback to savepoint1 again.
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testHierarchicalSavepointsAfterErrors() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint2 = connection.setSavepoint("s2");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint3 = connection.setSavepoint("s3");

      // Error after s3, rollback to s2. This invalidates s3.
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));
      connection.rollback(savepoint2);

      // s3 is no longer valid.
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.rollback(savepoint3));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());

      // Another error, rollback to s1. This invalidates s2.
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));
      connection.rollback(savepoint1);

      // s2 is no longer valid.
      exception = assertThrows(SQLException.class, () -> connection.rollback(savepoint2));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());

      // Rollback to valid savepoint s1 recovers from the aborted state.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testStatementsRejectedWhileAbortedBeforeSavepointRollback() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // In the aborted state, regular queries and savepoint definitions are rejected.
      SQLException queryException =
          assertThrows(
              SQLException.class, () -> connection.createStatement().executeQuery("select 1"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), queryException.getSQLState());

      SQLException savepointException =
          assertThrows(SQLException.class, () -> connection.setSavepoint("s2"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), savepointException.getSQLState());

      SQLException releaseException =
          assertThrows(SQLException.class, () -> connection.releaseSavepoint(savepoint1));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), releaseException.getSQLState());

      // Rollback to savepoint1 recovers the transaction.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testReleaseSavepointBeforeError() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      Savepoint savepoint2 = connection.setSavepoint("s2");
      connection.releaseSavepoint(savepoint2);

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rolling back to the released savepoint2 fails.
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.rollback(savepoint2));
      assertEquals(SQLState.SavepointException.toString(), exception.getSQLState());

      // Rolling back to savepoint1 succeeds.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testReleaseAllSavepointsBeforeErrorEagerlyRollsBack() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      connection.releaseSavepoint(savepoint1);

      // When the statement fails, because all savepoints were released, Spanner rolls back eagerly.
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testIdenticalNameSavepointsAfterError() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint outerSavepoint = connection.setSavepoint("s1");
      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint innerSavepoint = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rolling back to s1 should roll back to the innermost s1.
      connection.rollback(innerSavepoint);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRedundantBeginDoesNotClearSavepoints() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      // Redundant BEGIN should not clear active savepoints.
      connection.createStatement().execute("begin");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rolling back to savepoint1 should still succeed.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointSimpleQueryProtocol() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?preferQueryMode=simple")) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rolling back to savepoint1 in simple query mode recovers the transaction.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointAfterErrorInReadOnlyTransaction() throws SQLException {
    String invalidSql = "select * from non_existing_table";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.NOT_FOUND.withDescription("Table not found").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setReadOnly(true);
      connection.setAutoCommit(false);

      Savepoint savepoint1 = connection.setSavepoint("s1");
      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }

      assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(invalidSql));

      // Rolling back to savepoint1 recovers the read-only transaction.
      connection.rollback(savepoint1);

      try (ResultSet resultSet =
          connection.createStatement().executeQuery(SELECT_RANDOM_STATEMENT.getSql())) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(RANDOM_RESULT_SET_ROW_COUNT, count);
      }
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testDdlBatchAbortedOnErrorWithActiveSavepoint() throws SQLException {
    String invalidDdl = "create table invalid_table (id bigint primary key)";
    addDdlExceptionToSpannerAdmin();

    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.setSavepoint("s1");

      // Executing DDL commits the active transaction and starts a DDL batch.
      connection.createStatement().execute(invalidDdl);

      // The DDL error is surfaced when the batch is committed.
      SQLException exception = assertThrows(SQLException.class, connection::commit);
      assertTrue(exception.getMessage(), exception.getMessage().contains("Statement is invalid"));

      // The transaction was committed before starting the DDL batch.
      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testPreparedStatementErrorRecoveryWithSavepoint() throws SQLException {
    Statement validInsert =
        Statement.newBuilder("INSERT INTO FOO VALUES ($1)").bind("p1").to(1L).build();
    Statement invalidInsert =
        Statement.newBuilder("INSERT INTO FOO VALUES ($1)").bind("p1").to(2L).build();
    mockSpanner.putStatementResult(StatementResult.update(validInsert, 1L));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            invalidInsert,
            Status.INVALID_ARGUMENT.withDescription("Value 2 is invalid").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      Savepoint savepoint1 = connection.setSavepoint("s1");

      try (PreparedStatement statement =
          connection.prepareStatement("INSERT INTO FOO VALUES (?)")) {
        statement.setLong(1, 2L);
        assertThrows(SQLException.class, statement::executeUpdate);

        connection.rollback(savepoint1);

        statement.setLong(1, 1L);
        assertEquals(1, statement.executeUpdate());
      }
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testRollbackToSavepointThenFullRollback() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rollback to savepoint1 recovers the transaction from aborted state.
      connection.rollback(savepoint1);

      // Now rollback the entire transaction.
      connection.rollback();

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testDmlBatchErrorRecoveryWithSavepoint() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      // Execute a DML batch where one of the statements fails.
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(invalidSql);
        assertThrows(SQLException.class, statement::executeBatch);
      }

      // Aborting the failed DML batch allows rollback to savepoint1 to succeed.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testCompoundQueryWithRollbackToSavepointSimpleQueryProtocol() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?preferQueryMode=simple")) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // In simple query mode, execute a compound query containing both ROLLBACK TO SAVEPOINT and
      // the next statement in a single roundtrip while in aborted state.
      connection
          .createStatement()
          .execute(String.format("rollback to savepoint s1; %s;", INSERT_STATEMENT.getSql()));

      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testQueryErrorRecoveryInReadWriteTransaction() throws SQLException {
    String invalidSql = "select * from non_existing_table";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.NOT_FOUND.withDescription("Table not found").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(invalidSql));

      // Rollback to savepoint1 recovers from query failure in read-write transaction.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testLocalSyntaxErrorRecoveryWithSavepoint() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().execute("select * from;"));

      // Rollback to savepoint1 recovers from local syntax error.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testDefineNewSavepointAfterRecovery() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      connection.rollback(savepoint1);

      // Define a new savepoint s2 after recovering.
      Savepoint savepoint2 = connection.setSavepoint("s2");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      connection.rollback(savepoint2);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testCascadingSavepointReleaseInvalidatesInnerSavepoints() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");
      connection.setSavepoint("s2");

      // Releasing s1 releases both s1 and s2 (cascading).
      connection.releaseSavepoint(savepoint1);

      // Now both s1 and s2 are gone. An error here should eagerly roll back the transaction on
      // Spanner.
      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testReleaseDuplicateNamedSavepointPreservesOuterSavepoint() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint outerSavepoint = connection.setSavepoint("s1");
      Savepoint innerSavepoint = connection.setSavepoint("s1");

      // Releasing s1 should release only the inner s1.
      connection.releaseSavepoint(innerSavepoint);

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Rolling back to outer s1 should succeed because outer s1 is still active.
      connection.rollback(outerSavepoint);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    }
  }

  @Test
  public void testDdlStatementInAbortedTransactionWithSavepointDoesNotCommit() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Sending DDL while the transaction is aborted must fail with 25P02 and must NOT commit the
      // pending statements to Spanner.
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement().execute("create table bar (id bigint primary key)"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), exception.getSQLState());

      // Rolling back to savepoint1 should still be possible and recover the transaction.
      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testDdlStatementInAbortedTransactionWithSavepointFullRollback() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Sending DDL while the transaction is aborted must fail with 25P02 and must NOT commit the
      // pending statements to Spanner.
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement().execute("create table bar (id bigint primary key)"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), exception.getSQLState());

      // Rolling back the entire transaction should send a RollbackRequest to Spanner without
      // committing.
      connection.rollback();

      assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testMultiStatementDdlInAbortedTransactionWithSavepoint() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Sending multi-statement DDL while aborted must fail with 25P02 and must NOT commit to
      // Spanner.
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  connection
                      .createStatement()
                      .execute(
                          "create table bar (id bigint primary key); create table baz (id bigint"
                              + " primary key)"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), exception.getSQLState());

      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }

  @Test
  public void testDdlStatementInAbortedTransactionDefaultMode() throws SQLException {
    String invalidSql = "insert into foo values ('invalid')";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Invalid value").asRuntimeException()));

    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      Savepoint savepoint1 = connection.setSavepoint("s1");

      assertThrows(
          SQLException.class, () -> connection.createStatement().executeUpdate(invalidSql));

      // Under default ddl_transaction_mode=Batch, DDL in aborted transaction must fail with 25P02
      // (InFailedSqlTransaction), not 25000 (InvalidTransactionState).
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement().execute("create table bar (id bigint primary key)"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), exception.getSQLState());

      connection.rollback(savepoint1);

      connection.createStatement().executeUpdate(INSERT_STATEMENT.getSql());
      connection.commit();

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    }
  }
}
