// Copyright 2022 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.TRANSACTION_ABORTED_ERROR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.RollbackRequest;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.jdbc.TimestampUtils;

/**
 * Tests the native PG JDBC driver in simple query mode. This is similar to the protocol that is
 * used by psql, and for example allows batches to be given as semicolon-separated strings.
 */
@RunWith(JUnit4.class)
public class JdbcSimpleModeMockServerTest extends AbstractMockServerTest {
  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(null, Collections.emptyList());
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
   */
  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/my-db?preferQueryMode=simple", pgServer.getLocalPort());
  }

  @Test
  public void testQuery() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    // The statement is sent only once to the mock server in simple query mode.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testEmptyStatement() throws SQLException {
    String sql = "";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      assertFalse(connection.createStatement().execute(sql));
    }

    // An empty statement is not sent to Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testEmptyStatementFollowedByNonEmptyStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Execute should return false for the first statement.
        assertFalse(statement.execute(""));

        assertTrue(statement.execute("SELECT 1"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertFalse(statement.getMoreResults());
      }
    }
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testWrongDialect() {
    // Let the mock server respond with the Google SQL dialect instead of PostgreSQL. The
    // connection should be gracefully rejected. Close all open pooled Spanner objects so we know
    // that we will get a fresh one for our connection. This ensures that it will execute a query to
    // determine the dialect of the database.
    try {
      closeSpannerPool();
    } catch (SpannerException ignore) {
      // ignore
    }
    try {
      mockSpanner.putStatementResult(
          StatementResult.detectDialectResult(Dialect.GOOGLE_STANDARD_SQL));

      String url =
          String.format(
              "jdbc:postgresql://localhost:%d/wrong-dialect-db?preferQueryMode=simple",
              pgServer.getLocalPort());
      SQLException exception =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(url));

      assertTrue(exception.getMessage().contains("The database uses dialect GOOGLE_STANDARD_SQL"));
    } finally {
      mockSpanner.putStatementResult(StatementResult.detectDialectResult(Dialect.POSTGRESQL));
      try {
        closeSpannerPool();
      } catch (SpannerException ignore) {
        // ignore
      }
    }
  }

  @Test
  public void testQueryWithParameters() throws SQLException {
    // Query parameters are not supported by the PG wire protocol in the simple query mode. The JDBC
    // driver will therefore convert parameters to literals before sending them to PostgreSQL.
    // The bytea data type is not supported for that (by the PG JDBC driver).
    // Also, the JDBC driver always uses the default timezone of the JVM when setting a timestamp.
    // This is a requirement in the JDBC API (and one that causes about a trillion confusions per
    // year). So we need to extract that from the env in order to determine what the timestamp
    // string will be.
    OffsetDateTime zonedDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    String timestampString =
        new TimestampUtils(false, TimeZone::getDefault)
            .timeToString(java.sql.Timestamp.from(Instant.from(zonedDateTime)), true);

    String pgSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_numeric, col_timestamptz, col_varchar "
            + "from all_types "
            + "where col_bigint=1 "
            + "and col_bool='TRUE' "
            + "and col_float8=3.14 "
            + "and col_numeric=6.626 "
            + String.format("and col_timestamptz='%s' ", timestampString)
            + "and col_varchar='test'";
    String jdbcSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_numeric, col_timestamptz, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_float8=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_varchar=?";
    mockSpanner.putStatementResult(
        StatementResult.query(com.google.cloud.spanner.Statement.of(pgSql), ALL_TYPES_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
        int index = 0;
        preparedStatement.setLong(++index, 1L);
        preparedStatement.setBoolean(++index, true);
        preparedStatement.setDouble(++index, 3.14d);
        preparedStatement.setBigDecimal(++index, new BigDecimal("6.626"));
        preparedStatement.setTimestamp(
            ++index, java.sql.Timestamp.from(Instant.from(zonedDateTime)));
        preparedStatement.setString(++index, "test");
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }

    // The statement is sent only once to the mock server in simple query mode.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
    assertEquals(pgSql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result is an update count or no result.
        assertFalse(
            statement.execute(String.format("%s; %s;", INSERT_STATEMENT, UPDATE_STATEMENT)));

        // Note that we have sent two DML statements to the database in one string. These should be
        // treated as separate statements, and there should therefore be two results coming back
        // from the server. That is; The first update count should be 1 (the INSERT), and the second
        // should be 2 (the UPDATE).
        assertEquals(1, statement.getUpdateCount());

        // The following is a prime example of how not to design an API, but this is how JDBC works.
        // getMoreResults() returns true if the next result is a ResultSet. However, if the next
        // result is an update count, it returns false, and we have to check getUpdateCount() to
        // verify whether there were any more results.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // Verify that the DML statements were batched together by PgAdapter.
    List<ExecuteBatchDmlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testBatchInActiveTransaction() throws SQLException {
    String sql = String.format("%s; %s; %s;", INSERT_STATEMENT, "COMMIT", SELECT2);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Start an explicit transaction before executing batch
        assertFalse(statement.execute("BEGIN; SELECT 1"));
        assertEquals(0, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Execute Batch
        assertFalse(statement.execute(sql));
        assertEquals(1, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(1).getSql());
    assertFalse(requests.get(1).getTransaction().hasBegin());
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(SELECT2.getSql(), requests.get(2).getSql());
    assertTrue(requests.get(2).getTransaction().hasSingleUse());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testErrorHandlingInTwoDmlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(
                SQLException.class,
                () -> statement.execute(String.format("%s; %s;", INSERT_STATEMENT, INVALID_DML)));
        assertTrue(exception.getMessage().contains("INVALID_ARGUMENT: Statement is invalid."));
      }
    }

    // Verify that the DML statements were batched together by PgAdapter.
    List<ExecuteBatchDmlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(INVALID_DML.getSql(), request.getStatements(1).getSql());

    // Verify that the implicit transaction is rolled back due to the exception
    List<RollbackRequest> rollbackRequests = mockSpanner.getRequestsOfType(RollbackRequest.class);
    assertEquals(1, rollbackRequests.size());
  }

  @Test
  public void testErrorHandlingInExplicitTransactionWithCommit() throws SQLException {
    String sql =
        String.format(
            "%s; %s; %s; %s; %s;",
            INSERT_STATEMENT, "BEGIN", UPDATE_STATEMENT, INVALID_DML, "COMMIT");
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));

        // Execute a client side statement to verify that the transaction is in the aborted state.
        exception =
            assertThrows(
                SQLException.class, () -> statement.execute("show transaction isolation level"));
        assertTrue(
            exception.getMessage(), exception.getMessage().contains(TRANSACTION_ABORTED_ERROR));
        // Rollback the transaction and verify that we can get out of the aborted state.
        statement.execute("rollback work");
        assertTrue(statement.execute("show transaction isolation level"));
      }
    }

    // The first DML statement is executed separately, as it is followed by a non-DML statement.
    // The BEGIN statement will switch the batch to use an explicit transaction. The first DML
    // statement will be included as part of that explicit transaction.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());

    // Verify that the DML statements were batched together by PgAdapter.
    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = batchDmlRequests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(INVALID_DML.getSql(), request.getStatements(1).getSql());

    // The explicit transaction is rolled back by PGAdapter. The
    List<RollbackRequest> rollbackRequests = mockSpanner.getRequestsOfType(RollbackRequest.class);
    assertEquals(1, rollbackRequests.size());

    // The transaction should not be committed as it fails on the invalid DML statement.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(0, commitRequests.size());
  }

  @Test
  public void testErrorHandlingInExplicitTransactionWithoutCommit() throws SQLException {
    String sql =
        String.format("%s; %s; %s; %s;", INSERT_STATEMENT, "BEGIN", UPDATE_STATEMENT, INVALID_DML);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));
      }
    }

    // Verify that the DML statements were batched together by PgAdapter.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());

    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = batchDmlRequests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(INVALID_DML.getSql(), request.getStatements(1).getSql());

    // The aborted transaction should be rolled back by PGAdapter.
    List<RollbackRequest> rollbackRequests = mockSpanner.getRequestsOfType(RollbackRequest.class);
    assertEquals(1, rollbackRequests.size());

    // BEGIN statement converts the implicit transaction to an explicit transaction, but is
    // otherwise a no-op. We should therefore not receive any commits.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(0, commitRequests.size());
  }

  @Test
  public void testErrorHandlingOfDmlBatchAfterCommit() throws SQLException {
    String sql =
        String.format(
            "%s; %s; %s; %s; %s;",
            INSERT_STATEMENT, "COMMIT", UPDATE_STATEMENT, INVALID_DML, SELECT1);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());

    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchDmlRequests.size());
    ExecuteBatchDmlRequest request = batchDmlRequests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(INVALID_DML.getSql(), request.getStatements(1).getSql());
  }

  @Test
  public void testErrorHandlingInImplicitTransaction() throws SQLException {
    String sql =
        String.format(
            "%s; %s; %s; %s; %s;", INSERT_STATEMENT, "COMMIT", SELECT1, INVALID_SELECT, SELECT2);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));

        // Verify that the transaction was rolled back and that the connection is usable.
        assertTrue(statement.execute("show transaction isolation level"));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // The rest of the statements in the batch are all selects, so PGAdapter tries to use a
    // read-only transaction.
    assertEquals(1, mockSpanner.getRequestsOfType(BeginTransactionRequest.class).size());
    assertTrue(
        mockSpanner
            .getRequestsOfType(BeginTransactionRequest.class)
            .get(0)
            .getOptions()
            .hasReadOnly());
    assertEquals(SELECT1.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(INVALID_SELECT.getSql(), requests.get(2).getSql());
    assertTrue(requests.get(2).getTransaction().hasId());

    // We get one commit for the read/write transaction. The read-only transaction is not committed
    // or rolled back, as that is not necessary for read-only transactions.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());

    List<RollbackRequest> rollbackRequests = mockSpanner.getRequestsOfType(RollbackRequest.class);
    assertEquals(0, rollbackRequests.size());
  }

  @Test
  public void testTwoQueries() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns true if the result is a result set.
        assertTrue(statement.execute("SELECT 1; SELECT 2;"));

        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() returns true if the next result is a ResultSet.
        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasId());
    assertEquals(SELECT2.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasId());

    // PGAdapter will use a read-only transaction if it sees that the implicit transaction will only
    // read.
    List<BeginTransactionRequest> beginRequests =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class);
    assertEquals(1, beginRequests.size());
    assertTrue(beginRequests.get(0).getOptions().hasReadOnly());
  }

  @Test
  public void testBeginAndDml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // batch a BEGIN [TRANSACTION] statement together with an update statement.
        assertFalse(statement.execute(String.format("BEGIN; %s;", UPDATE_STATEMENT)));
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Execute an insert statement as part of the transaction.
        assertFalse(statement.execute(INSERT_STATEMENT.getSql()));
        assertEquals(1, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        statement.execute("COMMIT");
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    // The first statement in the transaction will include the BeginTransaction option.
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasId());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testTransactionStatementsInBatch() throws SQLException {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2; COMMIT;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Send a complete transaction as a single batch.
        assertFalse(statement.execute(sql));
        // The BEGIN statement should not return anything.
        assertEquals(0, statement.getUpdateCount());
        // INSERT
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());
        // UPDATE
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());
        // COMMIT
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // As the two DML statements are adjacent in the batch, they can be combined into a BatchDML
    // request. This is the first statement in the transaction and will include the BeginTransaction
    // option.
    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchDmlRequests.size());
    ExecuteBatchDmlRequest batchDmlRequest = batchDmlRequests.get(0);
    assertTrue(batchDmlRequest.getTransaction().hasBegin());
    assertEquals(INSERT_STATEMENT.getSql(), batchDmlRequest.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), batchDmlRequest.getStatements(1).getSql());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testBeginInExplicitTransaction() throws SQLException {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2; COMMIT;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Start an explicit transaction before executing batch
        assertFalse(statement.execute("BEGIN; SELECT 1"));
        assertEquals(0, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // The BEGIN statement should be no-op and not cause exception.
        assertFalse(statement.execute(sql));
        // The BEGIN statement should not return anything.
        assertEquals(0, statement.getUpdateCount());
        // INSERT
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());
        // UPDATE
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());
        // COMMIT
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchDmlRequests.size());
    ExecuteBatchDmlRequest batchDmlRequest = batchDmlRequests.get(0);
    // Verify that the BEGIN statement is no-op
    assertFalse(batchDmlRequest.getTransaction().hasBegin());
    assertTrue(batchDmlRequest.getTransaction().hasId());
    assertEquals(INSERT_STATEMENT.getSql(), batchDmlRequest.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), batchDmlRequest.getStatements(1).getSql());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testSelectAtStartOfBatch() throws SQLException {
    String sql = "SELECT 1; INSERT INTO FOO VALUES (1); SELECT 2;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(sql));

        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() returns false as the next result is an update count.
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }

      List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
      assertEquals(3, requests.size());
      assertEquals(SELECT1.getSql(), requests.get(0).getSql());
      assertTrue(requests.get(0).getTransaction().hasBegin());
      assertEquals(INSERT_STATEMENT.getSql(), requests.get(1).getSql());
      assertTrue(requests.get(1).getTransaction().hasId());
      assertEquals(SELECT2.getSql(), requests.get(2).getSql());
      assertTrue(requests.get(2).getTransaction().hasId());

      List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
      assertEquals(1, commitRequests.size());
    }
  }

  @Test
  public void testTwoDdlStatements() throws SQLException {
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // execute returns false if the result is an update count or no result.
        assertFalse(
            statement.execute(
                String.format(
                    "%s; %s;",
                    "CREATE TABLE FOO (id bigint primary key)",
                    "CREATE TABLE BAR (id bigint primary key)")));
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // Verify that the two DDL statements are sent as one batch.
    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(r -> r instanceof UpdateDatabaseDdlRequest)
            .map(r -> (UpdateDatabaseDdlRequest) r)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals("CREATE TABLE FOO (id bigint primary key)", request.getStatements(0));
    assertEquals("CREATE TABLE BAR (id bigint primary key)", request.getStatements(1));
  }

  @Test
  public void testBeginTransactionWithOptions() throws SQLException {
    String sql = "BEGIN TRANSACTION; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Execute returns false for statements that return no results.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());

        // getMoreResults also returns false for statements that return no results.
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        // getMoreResults also returns false if there are no more results. If getUpdateCount returns
        // -1, it is an indication that there are no more results.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Execute a garbled statement. This should abort the explicit transaction.
        assertThrows(SQLException.class, () -> statement.execute("bork bork bork"));
        SQLException exception =
            assertThrows(
                SQLException.class, () -> statement.execute("show transaction isolation level"));
        assertTrue(
            exception.getMessage(), exception.getMessage().contains(TRANSACTION_ABORTED_ERROR));

        // Verify that we can get out of the aborted transaction by committing it. This will
        // effectively rollback the transaction.
        assertFalse(statement.execute("commit work and no chain"));
        assertTrue(statement.execute("show transaction isolation level"));
      }
    }
    // There should be no commit or rollback requests on the server, as PGAdapter never actually
    // starts a transaction, as all statements are handled in the connection itself.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testJdbcBatch() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(UPDATE_STATEMENT.getSql());
        int[] updateCounts = statement.executeBatch();

        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(2, updateCounts[1]);
      }
    }

    // The PostgreSQL JDBC driver will send the DML statements as separated statements to PG when
    // executing a batch using simple mode. This means that Spanner will receive two separate DML
    // requests.
    assertTrue(mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).isEmpty());
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);

    assertEquals(2, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(1).getSql());
  }

  @Test
  public void testAbortedTransactionRollbackInAutocommit() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute("BEGIN"));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        assertFalse(statement.execute(UPDATE_STATEMENT.getSql()));
        assertEquals(2, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute(INVALID_DML.getSql()));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));

        assertFalse(statement.execute("COMMIT"));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    assertEquals(INVALID_DML.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasId());

    List<RollbackRequest> rollbackRequests = mockSpanner.getRequestsOfType(RollbackRequest.class);
    assertEquals(1, rollbackRequests.size());
  }

  @Test
  public void testStatementTagError() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute("begin"));
        assertFalse(statement.execute(INSERT_STATEMENT.getSql()));
        assertFalse(statement.execute("set spanner.statement_tag='foo'"));
        // Execute an invalid statement.
        assertThrows(SQLException.class, () -> statement.execute("set statement_timeout=2s"));
        // Make sure that we actually received a Rollback statement. The rollback was initiated by
        // PGAdapter when the transaction was aborted.
        assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
      }
    }
  }
}
