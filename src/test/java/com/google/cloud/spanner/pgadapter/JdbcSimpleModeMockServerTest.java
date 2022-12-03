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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
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
        assertEquals("ERROR: Statement is invalid.", exception.getMessage());

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
        assertEquals(
            "ERROR: Statement is invalid. - Statement: 'SELECT foo'", exception.getMessage());

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
        assertEquals("ERROR: Statement is invalid.", exception.getMessage());

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

  @Test
  public void testPrepareStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute("prepare my_statement as SELECT 1"));
        assertTrue(statement.execute("execute my_statement"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertFalse(statement.execute("deallocate my_statement"));
      }
    }
  }

  @Test
  public void testInvalidPrepareStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertThrows(SQLException.class, () -> statement.execute("prepare my_statement foo"));
      }
    }
  }

  @Test
  public void testPrepareStatementWithError() throws SQLException {
    String sql = "select * from non_existing_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            com.google.cloud.spanner.Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(
                SQLException.class, () -> statement.execute("prepare my_statement as " + sql));
        assertEquals(
            "ERROR: Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
      }
    }
  }

  @Test
  public void testStatementIsConnectionSpecific() throws SQLException {
    try (Connection connection1 = DriverManager.getConnection(createUrl());
        Connection connection2 = DriverManager.getConnection(createUrl())) {
      connection1.createStatement().execute("prepare my_statement as SELECT 1");
      assertTrue(connection1.createStatement().execute("execute my_statement"));
      assertThrows(
          SQLException.class, () -> connection2.createStatement().execute("execute my_statement"));
    }
  }

  @Test
  public void testDeallocateAll() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute("prepare my_statement1 as SELECT 1"));
        assertFalse(statement.execute("prepare my_statement2 as SELECT 2"));
        assertTrue(statement.execute("execute my_statement1"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertTrue(statement.execute("execute my_statement2"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertFalse(statement.execute("deallocate all"));

        assertThrows(SQLException.class, () -> statement.execute("execute my_statement1"));
        assertThrows(SQLException.class, () -> statement.execute("execute my_statement2"));
      }
    }
  }

  @Test
  public void testDeallocateUnknownStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertThrows(SQLException.class, () -> statement.execute("deallocate my_statement"));
      }
    }
  }

  @Test
  public void testExecuteUnknownStatement() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertThrows(SQLException.class, () -> statement.execute("execute my_statement"));
      }
    }
  }

  @Test
  public void testGetTimezoneStringUtc() throws SQLException {
    String sql = "select '2022-01-01 10:00:00+01'::timestamptz";
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.TIMESTAMP)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("2022-01-01T09:00:00Z").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set time zone utc");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("2022-01-01 09:00:00+00", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testGetTimezoneStringEuropeAmsterdam() throws SQLException {
    String sql = "select '2022-01-01 10:00:00Z'::timestamptz";
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.TIMESTAMP)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("2022-01-01T10:00:00Z").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set time zone 'Europe/Amsterdam'");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("2022-01-01 11:00:00+01", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testGetTimezoneStringAmericaLosAngeles() throws SQLException {
    String sql = "select '1883-11-18 00:00:00Z'::timestamptz";
    mockSpanner.putStatementResult(
        StatementResult.query(
            com.google.cloud.spanner.Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.TIMESTAMP)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder().setStringValue("1883-11-18T00:00:00Z").build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set time zone 'America/Los_Angeles'");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        if (OptionsMetadata.isJava8()) {
          // Java8 does not support timezone offsets with second precision.
          assertEquals("1883-11-17 16:07:02-07:52", resultSet.getString(1));
        } else {
          assertEquals("1883-11-17 16:07:02-07:52:58", resultSet.getString(1));
        }
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSetInvalidTimezone() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("set time zone 'foo'"));
      assertEquals(
          "ERROR: invalid value for parameter \"TimeZone\": \"foo\"", exception.getMessage());
    }
  }

  @Test
  public void testImplicitDdlBatch() throws SQLException {
    String sql =
        "create table test1 (id bigint primary key); "
            + "create table test2 (id bigint primary key); ";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute(sql);
    }

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(message -> message instanceof UpdateDatabaseDdlRequest)
            .map(message -> (UpdateDatabaseDdlRequest) message)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    assertEquals(2, requests.get(0).getStatementsCount());
    assertEquals("create table test1 (id bigint primary key)", requests.get(0).getStatements(0));
    assertEquals("create table test2 (id bigint primary key)", requests.get(0).getStatements(1));
  }

  @Test
  public void testDdlBatchWithStartAndRun() throws SQLException {
    String sql =
        "start batch ddl; "
            + "create table test1 (id bigint primary key); "
            + "create table test2 (id bigint primary key); "
            + "run batch";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute(sql);
    }

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(message -> message instanceof UpdateDatabaseDdlRequest)
            .map(message -> (UpdateDatabaseDdlRequest) message)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    assertEquals(2, requests.get(0).getStatementsCount());
    assertEquals("create table test1 (id bigint primary key)", requests.get(0).getStatements(0));
    assertEquals("create table test2 (id bigint primary key)", requests.get(0).getStatements(1));
  }

  @Test
  public void testMixedImplicitDdlBatch() throws SQLException {
    String sql =
        "create table test1 (id bigint primary key); "
            + "show statement_timeout; "
            + "create table test2 (id bigint primary key); ";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.createStatement().execute(sql));
      assertEquals(
          "ERROR: DDL statements are not allowed in mixed batches or transactions.",
          exception.getMessage());
    }

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(message -> message instanceof UpdateDatabaseDdlRequest)
            .map(message -> (UpdateDatabaseDdlRequest) message)
            .collect(Collectors.toList());
    assertEquals(0, requests.size());
  }

  @Test
  public void testMixedDdlBatchWithStartAndRun() throws SQLException {
    String sql =
        "start batch ddl; "
            + "create table test1 (id bigint primary key); "
            + "set statement_timeout = '10s'; "
            + "create table test2 (id bigint primary key); "
            + "run batch";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(SQLException.class, () -> connection.createStatement().execute(sql));
      assertEquals(
          "ERROR: DDL statements are not allowed in mixed batches or transactions.",
          exception.getMessage());
    }

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(message -> message instanceof UpdateDatabaseDdlRequest)
            .map(message -> (UpdateDatabaseDdlRequest) message)
            .collect(Collectors.toList());
    assertEquals(0, requests.size());
  }

  @Test
  public void testImplicitBatchOfClientSideStatements() throws SQLException {
    String sql = "set statement_timeout = '10s'; " + "show statement_timeout; ";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals("10s", resultSet.getString(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }
}
