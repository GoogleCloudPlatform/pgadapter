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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.rpc.ResourceInfo;
import com.google.spanner.admin.database.v1.Database;
import com.google.spanner.admin.database.v1.DatabaseDialect;
import com.google.spanner.admin.database.v1.ListDatabasesResponse;
import com.google.spanner.admin.instance.v1.Instance;
import com.google.spanner.admin.instance.v1.ListInstanceConfigsResponse;
import com.google.spanner.admin.instance.v1.ListInstancesResponse;
import com.google.spanner.v1.DatabaseName;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.SessionName;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.PSQLException;

@RunWith(JUnit4.class)
public class EmulatedPsqlMockServerTest extends AbstractMockServerTest {

  private static final String INSERT1 = "insert into foo values (1)";
  private static final String INSERT2 = "insert into foo values (2)";

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void setDetectClient() {
    ClientAutoDetector.FORCE_DETECT_CLIENT.set(WellKnownClient.PSQL);
  }

  @AfterClass
  public static void clearDetectClient() {
    ClientAutoDetector.FORCE_DETECT_CLIENT.set(null);
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter without a default database.
    doStartMockSpannerAndPgAdapterServers(null, builder -> {});

    mockSpanner.putStatementResults(
        StatementResult.update(Statement.of(INSERT1), 1L),
        StatementResult.update(Statement.of(INSERT2), 1L));
  }

  @After
  public void removeExecutionTimes() {
    mockSpanner.removeAllExecutionTimes();
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
   */
  private String createUrl(String database) {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s?preferQueryMode=simple",
        pgServer.getLocalPort(), database);
  }

  @Test
  public void testConnectToDifferentDatabases() throws SQLException {
    final ImmutableList<String> databases = ImmutableList.of("db1", "db2");
    for (String database : databases) {
      try (Connection connection = DriverManager.getConnection(createUrl(database))) {
        connection.createStatement().execute(INSERT1);
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(databases.size(), requests.size());
    for (int i = 0; i < requests.size(); i++) {
      assertEquals(databases.get(i), SessionName.parse(requests.get(i).getSession()).getDatabase());
    }
  }

  @Test
  public void testConnectToFullDatabasePath() throws Exception {
    String databaseName =
        "projects/full-path-test-project/instances/full-path-test-instance/databases/full-path-test-database";
    // Note that we need to URL encode the database name as it contains multiple forward slashes.
    try (Connection connection =
        DriverManager.getConnection(
            createUrl(URLEncoder.encode(databaseName, StandardCharsets.UTF_8.name())))) {
      connection.createStatement().execute(INSERT1);
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    SessionName sessionName = SessionName.parse(requests.get(0).getSession());
    DatabaseName gotDatabaseName =
        DatabaseName.of(
            sessionName.getProject(), sessionName.getInstance(), sessionName.getDatabase());
    assertEquals(DatabaseName.parse(databaseName), gotDatabaseName);
  }

  @Ignore(
      "Skipped because of a bug in the gRPC server implementation that causes random NullPointerExceptions")
  @Test
  public void testConnectToNonExistingDatabase() {
    try {
      mockSpanner.setBatchCreateSessionsExecutionTime(
          SimulatedExecutionTime.stickyDatabaseNotFoundException("non-existing-db"));
      // The Connection API calls listInstanceConfigs(..) once first when the connection is a
      // localhost connection. It does so to verify that the connection is valid and to quickly
      // return an error if someone is for example trying to connect to the emulator while the
      // emulator is not running. This does not happen when you connect to a remote host. We
      // therefore need to add a response for the listInstanceConfigs as well.
      mockInstanceAdmin.addResponse(ListInstanceConfigsResponse.getDefaultInstance());
      mockInstanceAdmin.addResponse(
          Instance.newBuilder()
              .setName("projects/p/instances/i")
              .setConfig("projects/p/instanceConfigs/ic")
              .build());
      mockDatabaseAdmin.addResponse(
          ListDatabasesResponse.newBuilder()
              .addDatabases(
                  Database.newBuilder()
                      .setName("projects/p/instances/i/databases/d")
                      .setDatabaseDialect(DatabaseDialect.POSTGRESQL)
                      .build())
              .addDatabases(
                  Database.newBuilder()
                      .setName("projects/p/instances/i/databases/google-sql-db")
                      .setDatabaseDialect(DatabaseDialect.GOOGLE_STANDARD_SQL)
                      .build())
              .build());

      SQLException exception =
          assertThrows(
              SQLException.class, () -> DriverManager.getConnection(createUrl("non-existing-db")));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));
      assertTrue(
          exception.getMessage(),
          exception
              .getMessage()
              .contains(
                  "These PostgreSQL databases are available on instance projects/p/instances/i:"));
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("\tprojects/p/instances/i/databases/d\n"));
      assertFalse(
          exception.getMessage(),
          exception.getMessage().contains("\tprojects/p/instances/i/databases/google-sql-db\n"));
    } finally {
      closeSpannerPool(true);
    }
  }

  @Test
  public void testConnectToNonExistingInstance() {
    for (boolean isPsql : new boolean[] {true, false}) {
      try {
        if (isPsql) {
          setDetectClient();
        } else {
          clearDetectClient();
        }
        mockSpanner.setExecuteStreamingSqlExecutionTime(
            SimulatedExecutionTime.ofStickyException(
                newStatusResourceNotFoundException(
                    "i",
                    "type.googleapis.com/google.spanner.admin.instance.v1.Instance",
                    "projects/p/instances/i")));
        // The Connection API calls listInstanceConfigs(..) once first when the connection is a
        // localhost connection. It does so to verify that the connection is valid and to quickly
        // return an error if someone is for example trying to connect to the emulator while the
        // emulator is not running. This does not happen when you connect to a remote host. We
        // therefore need to add a response for the listInstanceConfigs as well.
        mockInstanceAdmin.addResponse(ListInstanceConfigsResponse.getDefaultInstance());
        mockInstanceAdmin.addResponse(
            ListInstancesResponse.newBuilder()
                .addInstances(
                    Instance.newBuilder()
                        .setConfig("projects/p/instanceConfigs/ic")
                        .setName("projects/p/instances/i")
                        .build())
                .build());

        SQLException exception =
            assertThrows(
                SQLException.class,
                () -> DriverManager.getConnection(createUrl("non-existing-db")));
        assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));

        assertEquals(
            exception.getMessage(),
            isPsql,
            exception.getMessage().contains("These instances are available in project p:"));
        assertEquals(
            exception.getMessage(),
            isPsql,
            exception.getMessage().contains("\tprojects/p/instances/i\n"));
      } finally {
        closeSpannerPool(true);
        setDetectClient();
      }
    }
  }

  @Test
  public void testConnectFailed() {
    try {
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofStickyException(
              Status.INVALID_ARGUMENT.withDescription("test error").asRuntimeException()));
      SQLException exception =
          assertThrows(
              SQLException.class, () -> DriverManager.getConnection(createUrl("non-existing-db")));
      assertTrue(exception.getMessage(), exception.getMessage().contains("test error"));

    } finally {
      closeSpannerPool(true);
    }
  }

  @Test
  public void testTwoInserts() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute(String.format("%s; %s", INSERT1, INSERT2));
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT1, request.getStatements(0).getSql());
    assertEquals(INSERT2, request.getStatements(1).getSql());
  }

  @Test
  public void testNestedBlockComment() throws SQLException {
    String sql1 =
        "/* This block comment surrounds a query which itself has a block comment...\n"
            + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
            + "*/\n"
            + "SELECT 1";
    String sql2 = "-- This is a line comment\n SELECT 2";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql1), SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(String.format("%s;%s;", sql1, sql2)));
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
      }
    }
  }

  @Test
  public void testPrepareExecuteDeallocate() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute("prepare my_prepared_statement as SELECT 1");

      assertEquals(1, pgServer.getNumberOfConnections());
      ConnectionHandler connectionHandler = pgServer.getConnectionHandlers().get(0);
      IntermediateStatement preparedStatement =
          connectionHandler.getStatement("my_prepared_statement");
      assertNotNull(preparedStatement);
      assertEquals("SELECT 1", preparedStatement.getStatement());

      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute("execute my_prepared_statement"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }

      connection.createStatement().execute("deallocate my_prepared_statement");
      PGException exception =
          assertThrows(
              PGException.class, () -> connectionHandler.getStatement("my_prepared_statement"));
      assertEquals(
          "prepared statement my_prepared_statement does not exist", exception.getMessage());

      SQLException sqlException =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("execute my_prepared_statement"));
      assertEquals(
          "ERROR: prepared statement my_prepared_statement does not exist",
          sqlException.getMessage());
    }
  }

  @Test
  public void testPrepareInvalidStatement() throws SQLException {
    // Register an error for an invalid statement.
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of("SELECT"),
            Status.INVALID_ARGUMENT
                .withDescription("Statement must produce at least one output column")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      // Try to create a prepared statement using the invalid SELECT statement.
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection.createStatement().execute("prepare my_prepared_statement as SELECT"));
      assertTrue(
          exception.getMessage().contains("Statement must produce at least one output column"));

      // Verify that we can create a prepared statement with the same name without having to drop it
      // first.
      connection.createStatement().execute("prepare my_prepared_statement as SELECT 1");
      // Verify that we can now use the prepared statement.
      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute("execute my_prepared_statement"));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testPrepareStatementWithErrorDoesNotReserveName() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of("select bad_statement"),
            Status.INVALID_ARGUMENT
                .withDescription("column \"bad_statement\" does not exist")
                .asRuntimeException()));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        PSQLException exception1 =
            assertThrows(
                PSQLException.class,
                () ->
                    statement.execute(
                        "prepare test_q(timestamptz, timestamptz) as select bad_statement"));
        assertTrue(exception1.getMessage().contains("column \"bad_statement\" does not exist"));

        // Running the same failed prepare statement again should return the same error,
        // and NOT complain that the statement name is already reserved or that the connection is
        // closed.
        PSQLException exception2 =
            assertThrows(
                PSQLException.class,
                () ->
                    statement.execute(
                        "prepare test_q(timestamptz, timestamptz) as select bad_statement"));
        assertTrue(exception2.getMessage().contains("column \"bad_statement\" does not exist"));

        // Now prepare a valid statement using the same name.
        statement.execute("prepare test_q as SELECT 1");
        try (ResultSet resultSet = statement.executeQuery("execute test_q")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        statement.execute("deallocate test_q");
      }
    }
  }

  @Test
  public void testPrepareDuplicateStatementName() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.execute("prepare test_dup as SELECT 1");

        PSQLException exception =
            assertThrows(
                PSQLException.class, () -> statement.execute("prepare test_dup as SELECT 2"));
        assertEquals("42P05", exception.getSQLState());
        assertTrue(
            exception.getMessage().contains("prepared statement \"test_dup\" already exists"));

        // Verify that the original statement is still intact and runnable.
        try (ResultSet resultSet = statement.executeQuery("execute test_dup")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        statement.execute("deallocate test_dup");
      }
    }
  }

  @Test
  public void testPrepareCaseInsensitiveAndQuotedIdentifier() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Unquoted mixed-case identifier folds to lowercase.
        statement.execute("prepare MyStmt as SELECT 1");
        try (ResultSet resultSet = statement.executeQuery("execute mystmt")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        try (ResultSet resultSet = statement.executeQuery("execute MyStmt")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        statement.execute("deallocate MyStmt");

        // Quoted identifier preserves case.
        statement.execute("prepare \"MyQuotedStmt\" as SELECT 2");
        try (ResultSet resultSet = statement.executeQuery("execute \"MyQuotedStmt\"")) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        statement.execute("deallocate \"MyQuotedStmt\"");
      }
    }
  }

  @Test
  public void testPrepareZeroLengthDelimitedIdentifier() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute("prepare \"\" as SELECT 1"));
        assertEquals(SQLState.InvalidSqlStatementName.toString(), exception.getSQLState());
        assertTrue(exception.getMessage().contains("zero-length delimited identifier"));
      }
    }
  }

  @Test
  public void testPrepareSelectCurrentSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // 1. Prepare and execute SELECT current_setting(...)
        statement.execute("prepare test_setting as select current_setting('application_name')");
        try (ResultSet resultSet = statement.executeQuery("execute test_setting")) {
          assertTrue(resultSet.next());
          assertEquals("PostgreSQL JDBC Driver", resultSet.getString(1));
          assertFalse(resultSet.next());
        }
        statement.execute("deallocate test_setting");

        // 2. Prepare and execute SELECT set_config(...)
        statement.execute(
            "prepare test_set_config as select set_config('application_name', 'my-custom-app', false)");
        try (ResultSet resultSet = statement.executeQuery("execute test_set_config")) {
          assertTrue(resultSet.next());
          assertEquals("my-custom-app", resultSet.getString(1));
          assertFalse(resultSet.next());
        }
        statement.execute("deallocate test_set_config");

        // 3. Prepare an invalid client-side statement (syntax error for current_setting).
        SQLException exception =
            assertThrows(
                SQLException.class,
                () ->
                    statement.execute("prepare test_invalid_setting as select current_setting()"));
        assertTrue(exception.getMessage().contains("Invalid quote character"));

        // Verify that the failed client-side statement did not reserve the name.
        statement.execute("prepare test_invalid_setting as SELECT 1");
        try (ResultSet resultSet = statement.executeQuery("execute test_invalid_setting")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        statement.execute("deallocate test_invalid_setting");
      }
    }
  }

  @Test
  public void testPrepareFailureInExplicitTransactionAbortsTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.setAutoCommit(false);
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException prepareException =
            assertThrows(
                SQLException.class,
                () ->
                    statement.execute("prepare test_invalid as select * from non_existing_table"));
        assertTrue(prepareException.getMessage().contains("non_existing_table"));

        // Subsequent statements in the transaction must fail with 25P02 (transaction aborted).
        SQLException abortedException =
            assertThrows(SQLException.class, () -> statement.execute("SELECT 1"));
        assertEquals(SQLState.InFailedSqlTransaction.toString(), abortedException.getSQLState());

        connection.rollback();

        // After rollback, the connection should be healthy again.
        try (ResultSet resultSet = statement.executeQuery("SELECT 1")) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testRoundParamValueForPreparedStatement() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from my_table where id=$1"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("").toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder("select * from my_table where id=$1").bind("p1").to(2L).build(),
            createAllTypesResultSet("")));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection
          .createStatement()
          .execute("prepare my_prepared_statement as select * from my_table where id=$1");
      // 1.5 is automatically rounded to 2 because the parameter type has been inferred as bigint.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("execute my_prepared_statement (1.5)")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertEquals(
        Type.newBuilder().setCode(TypeCode.INT64).build(),
        executeRequest.getParamTypesMap().get("p1"));
    assertEquals("2", executeRequest.getParams().getFieldsMap().get("p1").getStringValue());
  }

  @Test
  public void testTimezone() throws SQLException {
    String sql = "select ts from foo";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.TIMESTAMP)))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2023-01-06T11:49:15.123456789Z")
                                .build())
                        .build())
                .build()));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute("set time zone cet");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-06 12:49:15.123456+01", resultSet.getString(1));
        }
      }
      connection.createStatement().execute("set time zone ist");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-06 17:19:15.123456+05:30", resultSet.getString(1));
        }
      }
      connection.createStatement().execute("set time zone 'America/Los_Angeles'");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-06 03:49:15.123456-08", resultSet.getString(1));
        }
      }
      connection.createStatement().execute("set time zone -12");
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals("2023-01-05 23:49:15.123456-12", resultSet.getString(1));
        }
      }
    }
  }

  @Test
  public void testDateForTimestamptzParameter() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from my_table where ts=$1"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("").toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder("select * from my_table where ts=$1")
                .bind("p1")
                .to(Timestamp.parseTimestamp("2022-12-27T23:00:00Z"))
                .build(),
            createAllTypesResultSet("")));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute("set time zone 'Europe/Amsterdam'");
      connection
          .createStatement()
          .execute("prepare my_prepared_statement as select * from my_table where ts=$1");
      // '2022-12-28' is interpreted in timezone 'Europe/Amsterdam', which means
      // '2022-12-28T23:00:00Z'.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("execute my_prepared_statement ('2022-12-28')")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertEquals(
        Type.newBuilder().setCode(TypeCode.TIMESTAMP).build(),
        executeRequest.getParamTypesMap().get("p1"));
    assertEquals(
        "2022-12-27T23:00:00Z",
        executeRequest.getParams().getFieldsMap().get("p1").getStringValue());
  }

  @Test
  public void testInvalidParseDoesNotReExecutePreviousUnnamedStatementSimpleMode()
      throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      assertEquals(1, connection.createStatement().executeUpdate(INSERT1));
      assertThrows(
          SQLException.class, () -> connection.createStatement().execute("copy bad_syntax"));
      assertThrows(
          SQLException.class,
          () ->
              connection.createStatement().execute("select * from non_existing_table; " + INSERT1));
    }
    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(INSERT1))
            .collect(Collectors.toList());
    assertEquals(1, insertRequests.size());
  }

  @Test
  public void testInvalidParseDoesNotReExecutePreviousUnnamedStatementExtendedMode()
      throws Exception {
    String invalidSql = "select * from non_existing_table";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSql),
            Status.INVALID_ARGUMENT.withDescription("Table not found").asRuntimeException()));

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      DataInputStream in = new DataInputStream(socket.getInputStream());

      // Send StartupMessage (protocol 3.0)
      ByteArrayOutputStream startupPayload = new ByteArrayOutputStream();
      DataOutputStream startupData = new DataOutputStream(startupPayload);
      startupData.writeInt(196608);
      startupData.writeBytes("user\0postgres\0database\0my-db\0\0");
      byte[] startupBytes = startupPayload.toByteArray();
      out.writeInt(startupBytes.length + 4);
      out.write(startupBytes);
      out.flush();
      readUntilReadyForQuery(in);

      // 1. Parse("", INSERT1) + Bind("", "") + Execute("") + Sync
      writeParse(out, "", INSERT1);
      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(0, readUntilReadyForQuery(in));
      assertEquals(
          1,
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
              .filter(request -> request.getSql().equals(INSERT1))
              .count());

      // 2. Refused Parse("", "copy bad_syntax") + Bind("", "") + Execute("") + Sync in one pipeline
      writeParse(out, "", "copy bad_syntax");
      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      // 3. Subsequent Bind("", "") + Execute("") + Sync after failed Parse("", "copy bad_syntax")
      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      // 4. Re-register unnamed statement INSERT1 and execute once more (total = 2)
      writeParse(out, "", INSERT1);
      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(0, readUntilReadyForQuery(in));
      assertEquals(
          2,
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
              .filter(request -> request.getSql().equals(INSERT1))
              .count());

      // 5. Parse("", invalidSql) + Describe('S', "") + Sync (fails during DescribeStatement)
      writeParse(out, "", invalidSql);
      writeDescribeStatement(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      // 6. Subsequent Bind("", "") + Execute("") + Sync must fail with 26000 and NOT re-execute
      // INSERT1
      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      // Verify INSERT1 was still only executed 2 times total on Spanner.
      assertEquals(
          2,
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
              .filter(request -> request.getSql().equals(INSERT1))
              .count());

      // 7. Pipelined invalid Parse followed by valid Parse(INSERT1) + Bind + Execute in the SAME
      // pipeline before Sync: the subsequent INSERT1 in the same pipeline must be aborted and NOT
      // executed, and its unnamed statement registration must also be aborted.
      writeParse(out, "", "copy bad_syntax");
      writeBind(out, "", "");
      writeExecute(out, "");
      writeParse(out, "", INSERT1);
      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));
      assertEquals(
          2,
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
              .filter(request -> request.getSql().equals(INSERT1))
              .count());

      // 8. Named invalid statement Parse("s1", "copy bad_syntax") must clean up "s1" on flush
      // without errors, and subsequent Bind("", "s1") must fail with 26000.
      writeParse(out, "s1", "copy bad_syntax");
      writeBind(out, "", "s1");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      writeBind(out, "", "s1");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      // 9. Parse("", invalidSql) + Flush ('H') + Describe('S', "") + Sync must close "" when
      // Describe('S', "") fails, so a subsequent Bind("", "") + Execute("") + Sync fails and does
      // not execute any statement.
      writeParse(out, "", invalidSql);
      writeFlush(out);
      writeDescribeStatement(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));

      writeBind(out, "", "");
      writeExecute(out, "");
      writeSync(out);
      out.flush();
      assertEquals(1, readUntilReadyForQuery(in));
      assertEquals(
          2,
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
              .filter(request -> request.getSql().equals(INSERT1))
              .count());

      // Send Terminate ('X')
      out.writeByte('X');
      out.writeInt(4);
      out.flush();
    }
  }

  private static void writeParse(DataOutputStream out, String name, String sql) throws IOException {
    byte[] nameBytes = (name + "\0").getBytes(StandardCharsets.UTF_8);
    byte[] sqlBytes = (sql + "\0").getBytes(StandardCharsets.UTF_8);
    out.writeByte('P');
    out.writeInt(4 + nameBytes.length + sqlBytes.length + 2);
    out.write(nameBytes);
    out.write(sqlBytes);
    out.writeShort(0);
  }

  private static void writeDescribeStatement(DataOutputStream out, String name) throws IOException {
    byte[] nameBytes = (name + "\0").getBytes(StandardCharsets.UTF_8);
    out.writeByte('D');
    out.writeInt(4 + 1 + nameBytes.length);
    out.writeByte('S');
    out.write(nameBytes);
  }

  private static void writeBind(DataOutputStream out, String portal, String statement)
      throws IOException {
    byte[] portalBytes = (portal + "\0").getBytes(StandardCharsets.UTF_8);
    byte[] statementBytes = (statement + "\0").getBytes(StandardCharsets.UTF_8);
    out.writeByte('B');
    out.writeInt(4 + portalBytes.length + statementBytes.length + 2 + 2 + 2);
    out.write(portalBytes);
    out.write(statementBytes);
    out.writeShort(0);
    out.writeShort(0);
    out.writeShort(0);
  }

  private static void writeExecute(DataOutputStream out, String portal) throws IOException {
    byte[] portalBytes = (portal + "\0").getBytes(StandardCharsets.UTF_8);
    out.writeByte('E');
    out.writeInt(4 + portalBytes.length + 4);
    out.write(portalBytes);
    out.writeInt(0);
  }

  private static void writeFlush(DataOutputStream out) throws IOException {
    out.writeByte('H');
    out.writeInt(4);
  }

  private static void writeSync(DataOutputStream out) throws IOException {
    out.writeByte('S');
    out.writeInt(4);
  }

  private static int readUntilReadyForQuery(DataInputStream in) throws IOException {
    int errorCount = 0;
    while (true) {
      byte type = in.readByte();
      int length = in.readInt();
      byte[] payload = new byte[length - 4];
      in.readFully(payload);
      if (type == 'E') {
        errorCount++;
      } else if (type == 'Z') {
        return errorCount;
      }
    }
  }

  static StatusRuntimeException newStatusResourceNotFoundException(
      String shortName, String resourceType, String resourceName) {
    ResourceInfo resourceInfo =
        ResourceInfo.newBuilder()
            .setResourceType(resourceType)
            .setResourceName(resourceName)
            .build();
    Metadata.Key<ResourceInfo> key =
        Metadata.Key.of(
            resourceInfo.getDescriptorForType().getFullName() + Metadata.BINARY_HEADER_SUFFIX,
            ProtoLiteUtils.metadataMarshaller(resourceInfo));
    Metadata trailers = new Metadata();
    trailers.put(key, resourceInfo);
    String message =
        String.format("%s not found: %s with id %s not found", shortName, shortName, resourceName);
    return Status.NOT_FOUND.withDescription(message).asRuntimeException(trailers);
  }
}
