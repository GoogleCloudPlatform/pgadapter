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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.spanner.v1.CommitRequest;
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.jdbc.TimestampUtils;

/**
 * Tests the native PG JDBC driver in simple query mode. This is similar to the protocol that is
 * used by psql, and for example allows batches to be given as semicolon-separated strings.
 */
@RunWith(Parameterized.class)
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

  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
   */
  private String createUrl() {
    if (useDomainSocket) {
      return String.format(
          "jdbc:postgresql://localhost/?"
              + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=/tmp/.s.PGSQL.%d"
              + "&preferQueryMode=simple",
          pgServer.getLocalPort());
    }
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
  public void testQueryHint() throws SQLException {
    String sql = "/* @OPTIMIZER_VERSION=1 */ SELECT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(com.google.cloud.spanner.Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(sql, executeRequest.getSql());
  }

  @Test
  public void testQueryHintBatch() throws SQLException {
    String sql =
        "/* @OPTIMIZER_VERSION=1 */ SELECT 1; /* @OPTIMIZER_VERSION=2 */ SELECT 2 /* This is just a ; comment */";
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.of("/* @OPTIMIZER_VERSION=1 */ SELECT 1"),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.of(
                "/* @OPTIMIZER_VERSION=2 */ SELECT 2 /* This is just a ; comment */"),
            SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(sql));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
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

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(
        "/* @OPTIMIZER_VERSION=1 */ SELECT 1",
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
    assertEquals(
        "/* @OPTIMIZER_VERSION=2 */ SELECT 2 /* This is just a ; comment */",
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1).getSql());
  }

  @Test
  public void testDml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result is an update count or no result.
        assertFalse(statement.execute(INSERT_STATEMENT.getSql()));
        assertEquals(1, statement.getUpdateCount());
        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
    assertEquals(INSERT_STATEMENT.getSql(), request.getSql());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testBegin() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute("BEGIN"));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }

  @Test
  public void testCommit() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute("COMMIT"));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
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
  public void testInvalidDml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute(INVALID_DML.getSql()));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));

        // Verify that the transaction was rolled back and that the connection is usable.
        assertTrue(statement.execute("show transaction isolation level"));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INVALID_DML.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // There is no Rollback request, because the first statement failed and did not return a
    // transaction.
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInvalidQuery() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute(INVALID_SELECT.getSql()));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));

        // Verify that the transaction was rolled back and that the connection is usable.
        assertTrue(statement.execute("show transaction isolation level"));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INVALID_SELECT.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasSingleUse());
    assertTrue(requests.get(0).getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInvalidDdl() throws SQLException {
    addDdlExceptionToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute(INVALID_DDL.getSql()));
        assertThat(
            exception.getMessage(), containsString("INVALID_ARGUMENT: Statement is invalid."));

        // Verify that the transaction was rolled back and that the connection is usable.
        assertTrue(statement.execute("show transaction isolation level"));
      }
    }
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
