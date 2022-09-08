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

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.TRANSACTION_ABORTED_ERROR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.PgStatement;
import org.postgresql.util.PSQLException;

@RunWith(Parameterized.class)
public class JdbcMockServerTest extends AbstractMockServerTest {
  private static final int RANDOM_RESULTS_ROW_COUNT = 10;
  private static final Statement SELECT_RANDOM = Statement.of("select * from random_table");
  private static final ImmutableList<String> JDBC_STARTUP_STATEMENTS =
      ImmutableList.of(
          "SET extra_float_digits = 3", "SET application_name = 'PostgreSQL JDBC Driver'");

  @Parameter public String pgVersion;

  @Parameters(name = "pgVersion = {0}")
  public static Object[] data() {
    return new Object[] {"1.0", "14.1"};
  }

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    addRandomResultResults();
  }

  private static void addRandomResultResults() {
    RandomResultSetGenerator generator =
        new RandomResultSetGenerator(RANDOM_RESULTS_ROW_COUNT, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(SELECT_RANDOM, generator.generate()));
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default extended
   * mode for queries and DML statements.
   */
  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?options=-c%%20server_version=%s",
        pgServer.getLocalPort(), pgVersion);
  }

  private String getExpectedInitialApplicationName() {
    return pgVersion.equals("1.0") ? null : "PostgreSQL JDBC Driver";
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

    // The statement is only sent once to the mock server. The DescribePortal message will trigger
    // the execution of the query, and the result from that execution will be used for the Execute
    // message.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      assertEquals(sql, request.getSql());
      assertTrue(request.getTransaction().hasSingleUse());
      assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    }
  }

  @Test
  public void testInvalidQuery() throws SQLException {
    String sql = "/ not a valid comment / SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PSQLException exception =
          assertThrows(PSQLException.class, () -> connection.createStatement().executeQuery(sql));
      assertEquals(
          "ERROR: Unknown statement: / not a valid comment / SELECT 1", exception.getMessage());
    }

    // The statement is not sent to the mock server.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSelectCurrentSchema() throws SQLException {
    String sql = "SELECT current_schema";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("public", resultSet.getString("current_schema"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSelectCurrentDatabase() throws SQLException {
    for (String sql :
        new String[] {
          "SELECT current_database()",
          "select current_database()",
          "select * from CURRENT_DATABASE()"
        }) {

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals("d", resultSet.getString("current_database"));
          assertFalse(resultSet.next());
        }
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSelectCurrentCatalog() throws SQLException {
    for (String sql :
        new String[] {
          "SELECT current_catalog", "select current_catalog", "select * from CURRENT_CATALOG"
        }) {

      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals("d", resultSet.getString("current_catalog"));
          assertFalse(resultSet.next());
        }
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testShowSearchPath() throws SQLException {
    String sql = "show search_path";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals("public", resultSet.getString("search_path"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testSetSearchPath() throws SQLException {
    String sql = "set search_path to public";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testShowServerVersion() throws SQLException {
    String sql = "show server_version";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(pgVersion, resultSet.getString("server_version"));
        assertFalse(resultSet.next());
      }
    }

    // The statement is handled locally and not sent to Cloud Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testQueryHint() throws SQLException {
    String sql = "/* @OPTIMIZER_VERSION=1 */ SELECT 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

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
  public void testQueryWithParameters() throws SQLException {
    String jdbcSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_int=? "
            + "and col_float8=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=?";
    String pgSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar "
            + "from all_types "
            + "where col_bigint=$1 "
            + "and col_bool=$2 "
            + "and col_bytea=$3 "
            + "and col_int=$4 "
            + "and col_float8=$5 "
            + "and col_numeric=$6 "
            + "and col_timestamptz=$7 "
            + "and col_date=$8 "
            + "and col_varchar=$9";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(pgSql), ALL_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(100)
                .bind("p5")
                .to(3.14d)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123457000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test")
                .build(),
            ALL_TYPES_RESULTSET));

    OffsetDateTime offsetDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    OffsetDateTime truncatedOffsetDateTime = offsetDateTime.truncatedTo(ChronoUnit.MICROS);

    // Threshold 5 is the default. Use a named prepared statement if it is executed 5 times or more.
    // Threshold 1 means always use a named prepared statement.
    // Threshold 0 means never use a named prepared statement.
    // Threshold -1 means use binary transfer of values and use DESCRIBE statement.
    // (10 points to you if you guessed the last one up front!).
    for (int preparedThreshold : new int[] {5, 1, 0, -1}) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
          preparedStatement.unwrap(PgStatement.class).setPrepareThreshold(preparedThreshold);
          int index = 0;
          preparedStatement.setLong(++index, 1L);
          preparedStatement.setBoolean(++index, true);
          preparedStatement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
          preparedStatement.setInt(++index, 100);
          preparedStatement.setDouble(++index, 3.14d);
          preparedStatement.setBigDecimal(++index, new BigDecimal("6.626"));
          preparedStatement.setObject(++index, offsetDateTime);
          preparedStatement.setObject(++index, LocalDate.of(2022, 3, 29));
          preparedStatement.setString(++index, "test");
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            index = 0;
            assertEquals(1L, resultSet.getLong(++index));
            assertTrue(resultSet.getBoolean(++index));
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
            assertEquals(3.14d, resultSet.getDouble(++index), 0.0d);
            assertEquals(100, resultSet.getInt(++index));
            assertEquals(new BigDecimal("6.626"), resultSet.getBigDecimal(++index));
            if (preparedThreshold < 0) {
              // The binary format will truncate the timestamp value to microseconds.
              assertEquals(
                  truncatedOffsetDateTime, resultSet.getObject(++index, OffsetDateTime.class));
            } else {
              assertEquals(offsetDateTime, resultSet.getObject(++index, OffsetDateTime.class));
            }
            assertEquals(LocalDate.of(2022, 3, 29), resultSet.getObject(++index, LocalDate.class));
            assertEquals("test", resultSet.getString(++index));
            assertFalse(resultSet.next());
          }
        }
      }

      List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
      // Prepare threshold less than 0 means use binary transfer + DESCRIBE statement.
      assertEquals(preparedThreshold < 0 ? 2 : 1, requests.size());

      ExecuteSqlRequest executeRequest = requests.get(requests.size() - 1);
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertEquals(pgSql, executeRequest.getSql());

      Map<String, Value> params = executeRequest.getParams().getFieldsMap();
      Map<String, Type> types = executeRequest.getParamTypesMap();

      assertEquals(TypeCode.INT64, types.get("p1").getCode());
      assertEquals("1", params.get("p1").getStringValue());
      assertEquals(TypeCode.BOOL, types.get("p2").getCode());
      assertTrue(params.get("p2").getBoolValue());
      assertEquals(TypeCode.BYTES, types.get("p3").getCode());
      assertEquals(
          Base64.getEncoder().encodeToString("test".getBytes(StandardCharsets.UTF_8)),
          params.get("p3").getStringValue());
      assertEquals(TypeCode.INT64, types.get("p4").getCode());
      assertEquals("100", params.get("p4").getStringValue());
      assertEquals(TypeCode.FLOAT64, types.get("p5").getCode());
      assertEquals(3.14d, params.get("p5").getNumberValue(), 0.0d);
      assertEquals(TypeCode.NUMERIC, types.get("p6").getCode());
      assertEquals(TypeAnnotationCode.PG_NUMERIC, types.get("p6").getTypeAnnotation());
      assertEquals("6.626", params.get("p6").getStringValue());
      assertEquals(TypeCode.TIMESTAMP, types.get("p7").getCode());
      assertEquals("2022-02-16T13:18:02.123457000Z", params.get("p7").getStringValue());
      assertEquals(TypeCode.DATE, types.get("p8").getCode());
      assertEquals("2022-03-29", params.get("p8").getStringValue());
      assertEquals(TypeCode.STRING, types.get("p9").getCode());
      assertEquals("test", params.get("p9").getStringValue());

      mockSpanner.clearRequests();
    }
  }

  @Test
  public void testQueryWithLegacyDateParameter() throws SQLException {
    String jdbcSql = "select col_date from all_types where col_date=?";
    String pgSql = "select col_date from all_types where col_date=$1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(pgSql), ALL_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(pgSql).bind("p1").to(Date.parseDate("2022-03-29")).build(),
            ALL_TYPES_RESULTSET));

    // Threshold 5 is the default. Use a named prepared statement if it is executed 5 times or more.
    // Threshold 1 means always use a named prepared statement.
    // Threshold 0 means never use a named prepared statement.
    // Threshold -1 means use binary transfer of values and use DESCRIBE statement.
    // (10 points to you if you guessed the last one up front!).
    for (int preparedThreshold : new int[] {5, 1, 0, -1}) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
          preparedStatement.unwrap(PgStatement.class).setPrepareThreshold(preparedThreshold);
          int index = 0;
          preparedStatement.setDate(++index, new java.sql.Date(2022 - 1900, Calendar.MARCH, 29));
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            assertEquals(
                new java.sql.Date(2022 - 1900, Calendar.MARCH, 29), resultSet.getDate("col_date"));
            assertFalse(resultSet.next());
          }
        }

        List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
        // Prepare threshold less than 0 means use binary transfer + DESCRIBE statement.
        // However, the legacy date type will never use BINARY transfer and will always be sent with
        // unspecified type by the JDBC driver the first time. This means that we need 3 round trips
        // for a query that uses a prepared statement the first time.
        assertEquals(
            "Prepare threshold: " + preparedThreshold,
            preparedThreshold == 1 || preparedThreshold < 0 ? 3 : 1,
            requests.size());

        ExecuteSqlRequest executeRequest;
        if (preparedThreshold == 1) {
          // The order of statements here is a little strange. The execution of the statement is
          // executed first, and the describe statements are then executed afterwards. The reason
          // for
          // this is that JDBC does the following when it encounters a statement parameter that is
          // 'unknown' (it considers the legacy date type as unknown, as it does not know if the
          // user
          // means date, timestamp or timestamptz):
          // 1. It sends a DescribeStatement message, but without a flush or a sync, as it is not
          //    planning on using the information for this request.
          // 2. It then sends the Execute message followed by a sync. This causes PGAdapter to sync
          //    the backend connection and execute everything in the actual execution pipeline.
          // 3. PGAdapter then executes anything left in the message queue. The DescribeMessage is
          //    still there, and is therefore executed after the Execute message.
          // All the above still works as intended, as the responses are sent in the expected order.
          executeRequest = requests.get(0);
          for (int i = 1; i < requests.size(); i++) {
            assertEquals(QueryMode.PLAN, requests.get(i).getQueryMode());
          }
        } else {
          executeRequest = requests.get(requests.size() - 1);
        }
        assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
        assertEquals(pgSql, executeRequest.getSql());

        Map<String, Value> params = executeRequest.getParams().getFieldsMap();
        Map<String, Type> types = executeRequest.getParamTypesMap();

        assertEquals(TypeCode.DATE, types.get("p1").getCode());
        assertEquals("2022-03-29", params.get("p1").getStringValue());

        mockSpanner.clearRequests();
      }
    }
  }

  @Test
  public void testMultipleQueriesInTransaction() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Use a read/write transaction to execute two queries.
      connection.setAutoCommit(false);
      // Force the use of prepared statements.
      connection.unwrap(PGConnection.class).setPrepareThreshold(-1);
      for (int i = 0; i < 2; i++) {
        // https://github.com/GoogleCloudPlatform/pgadapter/issues/278
        // This would return `ERROR: FAILED_PRECONDITION: This ResultSet is closed`
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testQueryWithNonExistingTable() throws SQLException {
    String sql = "select * from non_existing_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeQuery);
        assertEquals(
            "ERROR: Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
      }
    }

    // PGAdapter tries to execute the query directly when describing the portal, so we receive one
    // ExecuteSqlRequest in normal execute mode.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(sql, requests.get(0).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(0).getQueryMode());
  }

  @Test
  public void testDmlWithNonExistingTable() throws SQLException {
    String sql = "update non_existing_table set value=$2 where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeUpdate);
        assertEquals("ERROR: Table non_existing_table not found", exception.getMessage());
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(sql, requests.get(0).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(0).getQueryMode());
  }

  @Test
  public void testNullValues() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(
                    "insert into all_types "
                        + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
                        + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p7")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to((String) null)
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types where col_bigint is null"),
            ALL_TYPES_NULLS_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.DATE);
        statement.setNull(++index, Types.VARCHAR);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select * from all_types where col_bigint is null")) {
        assertTrue(resultSet.next());

        int index = 0;
        // Note: JDBC returns the zero-value for primitive types if the value is NULL, and you have
        // to call wasNull() to determine whether the value was NULL or zero.
        assertEquals(0L, resultSet.getLong(++index));
        assertTrue(resultSet.wasNull());
        assertFalse(resultSet.getBoolean(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBytes(++index));
        assertTrue(resultSet.wasNull());
        assertEquals(0d, resultSet.getDouble(++index), 0.0d);
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBigDecimal(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getTimestamp(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getDate(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getString(++index));
        assertTrue(resultSet.wasNull());

        assertFalse(resultSet.next());
      }
    }
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testDescribeQueryWithNonExistingTable() throws SQLException {
    String sql = "select * from non_existing_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals(
            "ERROR: Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
      }
    }

    // We only receive one ExecuteSql request, as PGAdapter tries to describe the portal first.
    // As that statement fails, it does not try to describe the parameters in the statement, but
    // just returns the error from the DescribePortal statement.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(sql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testDescribeDmlWithNonExistingTable() throws SQLException {
    String sql = "update non_existing_table set value=$2 where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    String describeSql =
        "select $1, $2 from (select value=$2 from non_existing_table where id=$1) p";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(describeSql),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals("ERROR: Table non_existing_table not found", exception.getMessage());
      }
    }

    // We receive two ExecuteSql requests:
    // 1. DescribeStatement (parameters). This statement fails as the table does not exist.
    // 2. Because the DescribeStatement step fails, PGAdapter executes the DML statement in analyze
    // mode to force a 'correct' error message.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(describeSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(sql, requests.get(1).getSql());
    assertEquals(QueryMode.PLAN, requests.get(1).getQueryMode());
  }

  @Test
  public void testDescribeDmlWithSchemaPrefix() throws SQLException {
    String sql = "update public.my_table set value=? where id=?";
    String describeSql = "select $1, $2 from (select value=$1 from public.my_table where id=$2) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .build()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(Types.VARCHAR, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(describeSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testDescribeDmlWithQuotedSchemaPrefix() throws SQLException {
    String sql = "update \"public\".\"my_table\" set value=? where id=?";
    String describeSql =
        "select $1, $2 from (select value=$1 from \"public\".\"my_table\" where id=$2) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .build()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(Types.VARCHAR, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(describeSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // The PG JDBC driver will internally split the following SQL string into two statements and
        // execute these sequentially. We still get the results back as if they were executed as one
        // batch on the same statement.
        assertFalse(statement.execute(String.format("%s;%s;", INSERT_STATEMENT, UPDATE_STATEMENT)));

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

    // The DML statements are split by the JDBC driver and sent as separate statements to PgAdapter.
    // The Sync message is however sent after the second DML statement, which means that PGAdapter
    // is able to batch these together into one ExecuteBatchDml statement.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());
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

    // The PostgreSQL JDBC driver will send the DML statements as separated statements to PG, but it
    // will only send a Sync after the second statement. This means that PGAdapter is able to batch
    // these together in one ExecuteBatchDml request.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());
  }

  @Test
  public void testTwoQueries() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns true if the result is a result set.
        assertTrue(statement.execute("SELECT 1;SELECT 2;"));

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
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testDdl() throws SQLException {
    String sql = "CREATE TABLE foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(sql, updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testDdlBatch() throws SQLException {
    ImmutableList<String> statements =
        ImmutableList.of(
            "CREATE TABLE foo (id bigint primary key)",
            "CREATE TABLE bar (id bigint primary key, value text)",
            "CREATE INDEX idx_foo ON bar (text)");
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        for (String sql : statements) {
          statement.addBatch(sql);
        }
        int[] updateCounts = statement.executeBatch();
        assertArrayEquals(new int[] {0, 0, 0}, updateCounts);
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(3, updateDatabaseDdlRequests.get(0).getStatementsCount());
    for (int i = 0; i < statements.size(); i++) {
      assertEquals(statements.get(i), updateDatabaseDdlRequests.get(0).getStatements(i));
    }
  }

  @Test
  public void testCreateTableIfNotExists_tableDoesNotExist() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "CREATE TABLE IF NOT EXISTS foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            EMPTY_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "create table foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testCreateTableIfNotExists_tableExists() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "CREATE TABLE IF NOT EXISTS foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testCreateIndexIfNotExists_indexDoesNotExist() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "CREATE INDEX IF NOT EXISTS foo on bar (value)";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            EMPTY_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "create index foo on bar (value)", updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testCreateIndexIfNotExists_indexExists() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "CREATE INDEX IF NOT EXISTS foo on bar (value)";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testDropTableIfExists_tableDoesNotExist() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "DROP TABLE IF EXISTS foo";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            EMPTY_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testDropTableIfExists_tableExists() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "DROP TABLE IF EXISTS foo";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals("drop table foo", updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testDropIndexIfExists_indexDoesNotExist() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "DROP INDEX IF EXISTS foo";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            EMPTY_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testDropIndexIfExists_indexExists() throws SQLException {
    addIfNotExistsDdlException();
    String sql = "DROP INDEX IF EXISTS foo";
    addDdlResponseToSpannerAdmin();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("foo")
                .build(),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals("drop index foo", updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testDdlBatchWithIfNotExists() throws SQLException {
    addIfNotExistsDdlException();
    ImmutableList<String> statements =
        ImmutableList.of(
            "CREATE TABLE IF NOT EXISTS \"Foo\" (id bigint primary key)",
            "CREATE TABLE IF NOT EXISTS bar (id bigint primary key, value text)",
            "CREATE INDEX IF NOT EXISTS idx_foo ON bar (text)");
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("Foo")
                .build(),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("bar")
                .build(),
            SELECT1_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(
                    "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                .bind("p1")
                .to("public")
                .bind("p2")
                .to("idx_foo")
                .build(),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        for (String sql : statements) {
          statement.addBatch(sql);
        }
        int[] updateCounts = statement.executeBatch();
        assertArrayEquals(new int[] {0, 0, 0}, updateCounts);
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testCreateTableIfNotExists_withBackendSupport() throws SQLException {
    // Add a generic error that is returned for DDL statements. This will cause PGAdapter to think
    // that the backend supports `IF [NOT] EXISTS`, as it does not receive a specific error
    // regarding an `IF NOT EXISTS` statement.
    addDdlExceptionToSpannerAdmin();
    String sql = "CREATE TABLE IF NOT EXISTS foo (id bigint primary key)";
    // Add a response for the DDL statement that is sent to Spanner.
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE IF NOT EXISTS foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testPreparedStatement() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(
                    "insert into all_types "
                        + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
                        + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p7")
                .to((Timestamp) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to((String) null)
                .build(),
            1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(
                    "insert into all_types "
                        + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
                        + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123457000"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test")
                .build(),
            1L));

    OffsetDateTime zonedDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        PGStatement pgStatement = statement.unwrap(PGStatement.class);
        pgStatement.setPrepareThreshold(1);

        int index = 0;
        statement.setLong(++index, 1L);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14d);
        statement.setInt(++index, 100);
        statement.setBigDecimal(++index, new BigDecimal("6.626"));
        statement.setObject(++index, zonedDateTime);
        statement.setObject(++index, LocalDate.of(2022, 3, 29));
        statement.setString(++index, "test");

        assertEquals(1, statement.executeUpdate());

        index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.DATE);
        statement.setNull(++index, Types.VARCHAR);

        assertEquals(1, statement.executeUpdate());
      }
    }
  }

  @Test
  public void testCursorSuccess() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement(SELECT_FIVE_ROWS.getSql())) {
        // Fetch two rows at a time from the PG server.
        statement.setFetchSize(2);
        try (ResultSet resultSet = statement.executeQuery()) {
          int index = 0;
          while (resultSet.next()) {
            assertEquals(++index, resultSet.getInt(1));
          }
          assertEquals(5, index);
        }
      }
      connection.commit();
    }
    // The ExecuteSql request should only be sent once to Cloud Spanner.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(SELECT_FIVE_ROWS.getSql(), executeRequest.getSql());

    // PGAdapter should receive 5 Execute messages:
    // 1. BEGIN
    // 2. Execute - fetch rows 1, 2
    // 3. Execute - fetch rows 3, 4
    // 4. Execute - fetch rows 5
    // 5. COMMIT
    if (pgServer != null) {
      List<DescribeMessage> describeMessages = getWireMessagesOfType(DescribeMessage.class);
      assertEquals(1, describeMessages.size());
      DescribeMessage describeMessage = describeMessages.get(0);
      assertEquals(PreparedType.Portal, describeMessage.getType());

      List<ExecuteMessage> executeMessages =
          getWireMessagesOfType(ExecuteMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(5, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(2, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages =
          getWireMessagesOfType(ParseMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(3, parseMessages.size());
      assertEquals("BEGIN", parseMessages.get(0).getStatement().getSql());
      assertEquals(SELECT_FIVE_ROWS.getSql(), parseMessages.get(1).getStatement().getSql());
      assertEquals("COMMIT", parseMessages.get(2).getStatement().getSql());
    }
  }

  @Test
  public void testCursorFailsHalfway() throws SQLException {
    mockSpanner.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStreamException(Status.DATA_LOSS.asRuntimeException(), 2));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement(SELECT_FIVE_ROWS.getSql())) {
        // Fetch one row at a time from the PG server.
        statement.setFetchSize(1);
        try (ResultSet resultSet = statement.executeQuery()) {
          // The first row should succeed.
          assertTrue(resultSet.next());
          // The second row should fail.
          assertThrows(SQLException.class, resultSet::next);
        }
      }
      connection.rollback();
    }
    // The ExecuteSql request should only be sent once to Cloud Spanner.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(SELECT_FIVE_ROWS.getSql(), executeRequest.getSql());

    // PGAdapter should receive 4 Execute messages:
    // 1. BEGIN
    // 2. Execute - fetch row 1
    // 3. Execute - fetch row 2 -- This fails with a DATA_LOSS error
    // The JDBC driver does not send a ROLLBACK
    if (pgServer != null) {
      List<DescribeMessage> describeMessages = getWireMessagesOfType(DescribeMessage.class);
      assertEquals(1, describeMessages.size());
      DescribeMessage describeMessage = describeMessages.get(0);
      assertEquals(PreparedType.Portal, describeMessage.getType());

      List<ExecuteMessage> executeMessages =
          getWireMessagesOfType(ExecuteMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(4, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(1, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages =
          getWireMessagesOfType(ParseMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(3, parseMessages.size());
      assertEquals("BEGIN", parseMessages.get(0).getStatement().getSql());
      assertEquals(SELECT_FIVE_ROWS.getSql(), parseMessages.get(1).getStatement().getSql());
      assertEquals("ROLLBACK", parseMessages.get(2).getStatement().getSql());
    }
  }

  @Test
  public void testRandomResults() throws SQLException {
    final int fetchSize = 3;
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement(SELECT_RANDOM.getSql())) {
        statement.setFetchSize(fetchSize);
        try (ResultSet resultSet = statement.executeQuery()) {
          int rowCount = 0;
          while (resultSet.next()) {
            for (int col = 0; col < resultSet.getMetaData().getColumnCount(); col++) {
              resultSet.getObject(col + 1);
            }
            rowCount++;
          }
          assertEquals(RANDOM_RESULTS_ROW_COUNT, rowCount);
        }
      }
      connection.commit();
    }
    // The ExecuteSql request should only be sent once to Cloud Spanner.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertEquals(SELECT_RANDOM.getSql(), executeRequest.getSql());

    // PGAdapter should receive 5 Execute messages:
    // 1. BEGIN
    // 2. Execute - fetch rows 1, 2
    // 3. Execute - fetch rows 3, 4
    // 4. Execute - fetch rows 5
    // 5. COMMIT
    if (pgServer != null) {
      List<DescribeMessage> describeMessages = getWireMessagesOfType(DescribeMessage.class);
      assertEquals(1, describeMessages.size());
      DescribeMessage describeMessage = describeMessages.get(0);
      assertEquals(PreparedType.Portal, describeMessage.getType());

      List<ExecuteMessage> executeMessages =
          getWireMessagesOfType(ExecuteMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      int expectedExecuteMessageCount =
          RANDOM_RESULTS_ROW_COUNT / fetchSize
              + ((RANDOM_RESULTS_ROW_COUNT % fetchSize) > 0 ? 1 : 0)
              + 2;
      assertEquals(expectedExecuteMessageCount, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(fetchSize, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages =
          getWireMessagesOfType(ParseMessage.class).stream()
              .filter(message -> !JDBC_STARTUP_STATEMENTS.contains(message.getSql()))
              .collect(Collectors.toList());
      assertEquals(3, parseMessages.size());
      assertEquals("BEGIN", parseMessages.get(0).getStatement().getSql());
      assertEquals(SELECT_RANDOM.getSql(), parseMessages.get(1).getStatement().getSql());
      assertEquals("COMMIT", parseMessages.get(2).getStatement().getSql());
    }
  }

  @Test
  public void testInformationSchemaQueryInTransaction() throws SQLException {
    String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // Execute a query to start the transaction.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the following query returns an error the first time it is executed, and
      // then succeeds the second time. This happens because the exception is 'popped' from the
      // response queue when it is returned. The next time the query is executed, it will return the
      // actual result that we set.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Make sure that the connection is still usable.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT2.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(2L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      connection.commit();
    }

    // We should receive the INFORMATION_SCHEMA statement twice on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // The first statement should start a transaction
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // The second statement (the initial attempt of the INFORMATION_SCHEMA query) should try to use
    // the transaction.
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(sql, requests.get(1).getSql());
    // The INFORMATION_SCHEMA query is then retried using a single-use read-only transaction.
    assertFalse(requests.get(2).hasTransaction());
    assertEquals(sql, requests.get(2).getSql());
    // The last statement should use the transaction.
    assertTrue(requests.get(3).getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(commitRequest.getTransactionId(), requests.get(1).getTransaction().getId());
    assertEquals(commitRequest.getTransactionId(), requests.get(3).getTransaction().getId());
  }

  @Test
  public void testInformationSchemaQueryAsFirstQueryInTransaction() throws SQLException {
    // Running an information_schema query as the first query in a transaction will cause some
    // additional retrying and transaction magic. This is because:
    // 1. The first query in a transaction will also try to begin the transaction.
    // 2. If the first query fails, it will also fail to create a transaction.
    // 3. If an additional query is executed in the transaction, the entire transaction will be
    //    retried using an explicit BeginTransaction RPC. This is done so that we can include the
    //    first query in the transaction, as an error message in itself can give away information
    //    about the state of the database, and therefore must be included in the transaction to
    //    guarantee the consistency.

    String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // This makes sure the INFORMATION_SCHEMA query will return an error the first time it is
      // executed. Then it is retried without a transaction.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the next query will once again return the same error. The reason that we
      // do this is that the following query will cause the entire transaction to be retried, and we
      // need the first statement (the INFORMATION_SCHEMA query) to return exactly the same result
      // as the first time in order to make the retry succeed.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofException(
              Status.INVALID_ARGUMENT
                  .withDescription(
                      "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                  .asRuntimeException()));

      // Verify that the connection is still usable.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT2.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(2L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
      connection.commit();
    }

    // We should receive the INFORMATION_SCHEMA statement three times on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction.
    // 3. The second query in the transaction will cause the entire transaction to retry, which will
    //    cause the INFORMATION_SCHEMA query to be executed once more.
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // The first statement should try to start a transaction (although it fails).
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // The second statement is the INFORMATION_SCHEMA query without a transaction.
    assertFalse(requests.get(1).hasTransaction());
    assertEquals(sql, requests.get(1).getSql());

    // The transaction is then retried, which means that we get the INFORMATION_SCHEMA query again.
    // This time the query tries to use a transaction that has been created using an explicit
    // BeginTransaction RPC invocation.
    assertTrue(requests.get(2).getTransaction().hasId());
    assertEquals(sql, requests.get(2).getSql());
    // The last query should also use that transaction.
    assertTrue(requests.get(3).getTransaction().hasId());
    assertEquals(SELECT2.getSql(), requests.get(3).getSql());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(commitRequest.getTransactionId(), requests.get(2).getTransaction().getId());
    assertEquals(commitRequest.getTransactionId(), requests.get(3).getTransaction().getId());
    // Verify that we also got a BeginTransaction request.
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
  }

  @Test
  public void testInformationSchemaQueryInTransactionWithErrorDuringRetry() throws SQLException {
    String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    // Register a result for the query. Note that we don't really care what the result is, just that
    // there is a result.
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Make sure that we start a transaction.
      connection.setAutoCommit(false);

      // Execute a query to start the transaction.
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // This ensures that the following query returns the specific concurrency error the first time
      // it is executed, and then a different error.
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofExceptions(
              ImmutableList.of(
                  Status.INVALID_ARGUMENT
                      .withDescription(
                          "Unsupported concurrency mode in query using INFORMATION_SCHEMA.")
                      .asRuntimeException(),
                  Status.INTERNAL.withDescription("test error").asRuntimeException())));
      SQLException sqlException =
          assertThrows(SQLException.class, () -> connection.createStatement().executeQuery(sql));
      assertEquals(
          "ERROR: test error - Statement: 'SELECT * FROM INFORMATION_SCHEMA.TABLES'",
          sqlException.getMessage());

      // Make sure that the connection is now in the aborted state.
      SQLException abortedException =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery(SELECT2.getSql()));
      assertEquals(
          "ERROR: current transaction is aborted, commands ignored until end of transaction block",
          abortedException.getMessage());
    }

    // We should receive the INFORMATION_SCHEMA statement twice on Cloud Spanner:
    // 1. The first time it returns an error because it is using the wrong concurrency mode.
    // 2. The specific error will cause the connection to retry the statement using a single-use
    //    read-only transaction. That will also fail with the second error.
    // 3. The following SELECT query is never sent to Cloud Spanner, as the transaction is in the
    //    aborted state.
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testShowGuessTypes() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.guess_types")) {
        assertTrue(resultSet.next());
        assertEquals(String.format("%d,%d", Oid.TIMESTAMPTZ, Oid.DATE), resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowGuessTypesOverwritten() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?options=-c%20spanner.guess_types=0")) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.guess_types")) {
        assertTrue(resultSet.next());
        assertEquals("0", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowValidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name")) {
        assertTrue(resultSet.next());
        assertEquals(getExpectedInitialApplicationName(), resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowSettingWithStartupValue() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // DATESTYLE is set to 'ISO' by the JDBC driver at startup.
      try (ResultSet resultSet = connection.createStatement().executeQuery("show DATESTYLE")) {
        assertTrue(resultSet.next());
        assertEquals("ISO", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowInvalidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery("show random_setting"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"random_setting\"", exception.getMessage());
    }
  }

  @Test
  public void testSetValidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-application'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name ")) {
        assertTrue(resultSet.next());
        assertEquals("my-application", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSetCaseInsensitiveSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // The setting is called 'DateStyle' in the pg_settings table.
      connection.createStatement().execute("set datestyle to 'iso'");

      try (ResultSet resultSet = connection.createStatement().executeQuery("show DATESTYLE")) {
        assertTrue(resultSet.next());
        assertEquals("iso", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSetInvalidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  connection.createStatement().executeQuery("set random_setting to 'some-value'"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"random_setting\"", exception.getMessage());
    }
  }

  @Test
  public void testResetValidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-application'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name ")) {
        assertTrue(resultSet.next());
        assertEquals("my-application", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("reset application_name");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show application_name ")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetSettingWithStartupValue() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        assertEquals("ISO", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("set datestyle to 'iso, ymd'");

      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        assertEquals("iso, ymd", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("reset datestyle");

      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        assertEquals("ISO", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetInvalidSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery("reset random_setting"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"random_setting\"", exception.getMessage());
    }
  }

  @Test
  public void testShowUndefinedExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery("show spanner.some_setting"));
      assertEquals(
          "ERROR: unrecognized configuration parameter \"spanner.some_setting\"",
          exception.getMessage());
    }
  }

  @Test
  public void testSetExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set spanner.some_setting to 'some-value'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.some_setting ")) {
        assertTrue(resultSet.next());
        assertEquals("some-value", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetValidExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set spanner.some_setting to 'some-value'");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.some_setting")) {
        assertTrue(resultSet.next());
        assertEquals("some-value", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      connection.createStatement().execute("reset spanner.some_setting");

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.some_setting")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetUndefinedExtensionSetting() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Resetting an undefined extension setting is allowed by PostgreSQL, and will effectively set
      // the extension setting to null.
      connection.createStatement().execute("reset spanner.some_setting");

      verifySettingIsNull(connection, "spanner.some_setting");
    }
  }

  @Test
  public void testCommitSet() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is 'PostgreSQL JDBC Driver'.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      connection.createStatement().execute("set application_name to \"my-application\"");
      verifySettingValue(connection, "application_name", "my-application");
      // Committing the transaction should persist the value.
      connection.commit();
      verifySettingValue(connection, "application_name", "my-application");
    }
  }

  @Test
  public void testRollbackSet() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is null.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      connection.createStatement().execute("set application_name to \"my-application\"");
      verifySettingValue(connection, "application_name", "my-application");
      // Rolling back the transaction should reset the value to what it was before the transaction.
      connection.rollback();
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
    }
  }

  @Test
  public void testCommitSetExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      connection.createStatement().execute("set spanner.random_setting to \"42\"");
      verifySettingValue(connection, "spanner.random_setting", "42");
      // Committing the transaction should persist the value.
      connection.commit();
      verifySettingValue(connection, "spanner.random_setting", "42");
    }
  }

  @Test
  public void testRollbackSetExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      connection.createStatement().execute("set spanner.random_setting to \"42\"");
      verifySettingValue(connection, "spanner.random_setting", "42");
      // Rolling back the transaction should reset the value to what it was before the transaction.
      // In this case, that means that it should be undefined.
      connection.rollback();
      verifySettingIsUnrecognized(connection, "spanner.random_setting");
    }
  }

  @Test
  public void testRollbackDefinedExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // First define the extension setting.
      connection.createStatement().execute("set spanner.random_setting to '100'");

      connection.setAutoCommit(false);

      connection.createStatement().execute("set spanner.random_setting to \"42\"");
      verifySettingValue(connection, "spanner.random_setting", "42");
      // Rolling back the transaction should reset the value to what it was before the transaction.
      // In this case, that means back to '100'.
      connection.rollback();
      verifySettingValue(connection, "spanner.random_setting", "100");
    }
  }

  @Test
  public void testCommitSetLocal() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is 'PostgreSQL JDBC Driver'.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      connection.createStatement().execute("set local application_name to \"my-application\"");
      verifySettingValue(connection, "application_name", "my-application");
      // Committing the transaction should not persist the value as it was only set for the current
      // transaction.
      connection.commit();
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
    }
  }

  @Test
  public void testCommitSetLocalAndSession() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that the initial value is 'PostgreSQL JDBC Driver'.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
      // Set both a session and a local value. The session value will be 'hidden' by the local
      // value, but the session value will be committed.
      connection
          .createStatement()
          .execute("set session application_name to \"my-session-application\"");
      verifySettingValue(connection, "application_name", "my-session-application");
      connection
          .createStatement()
          .execute("set local application_name to \"my-local-application\"");
      verifySettingValue(connection, "application_name", "my-local-application");
      // Committing the transaction should persist the session value.
      connection.commit();
      verifySettingValue(connection, "application_name", "my-session-application");
    }
  }

  @Test
  public void testCommitSetLocalAndSessionExtension() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      // Verify that the initial value is undefined.
      verifySettingIsUnrecognized(connection, "spanner.custom_setting");

      connection.setAutoCommit(false);

      // Set both a session and a local value. The session value will be 'hidden' by the local
      // value, but the session value will be committed.
      connection.createStatement().execute("set spanner.custom_setting to 'session-value'");
      verifySettingValue(connection, "spanner.custom_setting", "session-value");
      connection.createStatement().execute("set local spanner.custom_setting to 'local-value'");
      verifySettingValue(connection, "spanner.custom_setting", "local-value");
      // Committing the transaction should persist the session value.
      connection.commit();
      verifySettingValue(connection, "spanner.custom_setting", "session-value");
    }
  }

  @Test
  public void testInvalidShowAbortsTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      // Verify that executing an invalid show statement will abort the transaction.
      assertThrows(
          SQLException.class,
          () -> connection.createStatement().execute("show spanner.non_existing_param"));
      SQLException exception =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().execute("show application_name "));
      assertEquals("ERROR: " + TRANSACTION_ABORTED_ERROR, exception.getMessage());

      connection.rollback();

      // Verify that the connection is usable again.
      verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
    }
  }

  @Test
  public void testShowAll() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("show all")) {
        assertEquals(3, resultSet.getMetaData().getColumnCount());
        assertEquals("name", resultSet.getMetaData().getColumnName(1));
        assertEquals("setting", resultSet.getMetaData().getColumnName(2));
        assertEquals("description", resultSet.getMetaData().getColumnName(3));
        int count = 0;
        while (resultSet.next()) {
          if ("client_encoding".equals(resultSet.getString("name"))) {
            assertEquals("UTF8", resultSet.getString("setting"));
          }
          count++;
        }
        assertEquals(348, count);
      }
    }
  }

  @Test
  public void testResetAll() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-app'");
      connection.createStatement().execute("set search_path to 'my_schema'");
      verifySettingValue(connection, "application_name", "my-app");
      verifySettingValue(connection, "search_path", "my_schema");

      connection.createStatement().execute("reset all");

      verifySettingIsNull(connection, "application_name");
      verifySettingValue(connection, "search_path", "public");
    }
  }

  @Test
  public void testSetToDefault() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to 'my-app'");
      connection.createStatement().execute("set search_path to 'my_schema'");
      verifySettingValue(connection, "application_name", "my-app");
      verifySettingValue(connection, "search_path", "my_schema");

      connection.createStatement().execute("set application_name to default");
      connection.createStatement().execute("set search_path to default");

      verifySettingIsNull(connection, "application_name");
      verifySettingValue(connection, "search_path", "public");
    }
  }

  @Test
  public void testSetToEmpty() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("set application_name to ''");
      verifySettingValue(connection, "application_name", "");
    }
  }

  @Test
  public void testSettingsAreUniqueToConnections() throws SQLException {
    // Verify that each new connection gets a separate set of settings.
    for (int connectionNum = 0; connectionNum < 5; connectionNum++) {
      try (Connection connection = DriverManager.getConnection(createUrl())) {
        // Verify that the initial value is 'PostgreSQL JDBC Driver'.
        verifySettingValue(connection, "application_name", getExpectedInitialApplicationName());
        connection.createStatement().execute("set application_name to \"my-application\"");
        verifySettingValue(connection, "application_name", "my-application");
      }
    }
  }

  @Test
  public void testSettingInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl()
                + "?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      verifySettingValue(
          connection, "spanner.ddl_transaction_mode", "AutocommitExplicitTransaction");
    }
  }

  @Test
  public void testMultipleSettingsInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20spanner.setting1=value1%20-c%20spanner.setting2=value2")) {
      verifySettingValue(connection, "spanner.setting1", "value1");
      verifySettingValue(connection, "spanner.setting2", "value2");
    }
  }

  @Test
  public void testServerVersionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?options=-c%20server_version=4.1")) {
      verifySettingValue(connection, "server_version", "4.1");
      verifySettingValue(connection, "server_version_num", "40001");
    }
  }

  @Test
  public void testCustomServerVersionInConnectionOptions() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20server_version=5.2 custom version")) {
      verifySettingValue(connection, "server_version", "5.2 custom version");
      verifySettingValue(connection, "server_version_num", "50002");
    }
  }

  @Test
  public void testSelectPgType() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + "),\n"
                    + "pg_type as (\n"
                    + "  select 16 as oid, 'bool' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 17 as oid, 'bytea' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 20 as oid, 'int8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 21 as oid, 'int2' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 23 as oid, 'int4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 25 as oid, 'text' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 700 as oid, 'float4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 701 as oid, 'float8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1043 as oid, 'varchar' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1082 as oid, 'date' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1114 as oid, 'timestamp' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1184 as oid, 'timestamptz' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1700 as oid, 'numeric' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 3802 as oid, 'jsonb' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl\n"
                    + ")\n"
                    + "select * from pg_type"),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_catalog.pg_type")) {
        // The result is not consistent with selecting from pg_type, but that's not relevant here.
        // We just want to ensure that it includes the correct common table expressions.
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSelectPgTypeAndPgNamespace() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + "),\n"
                    + "pg_type as (\n"
                    + "  select 16 as oid, 'bool' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 17 as oid, 'bytea' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 20 as oid, 'int8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 21 as oid, 'int2' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 23 as oid, 'int4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 25 as oid, 'text' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 700 as oid, 'float4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 701 as oid, 'float8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1043 as oid, 'varchar' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1082 as oid, 'date' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1114 as oid, 'timestamp' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1184 as oid, 'timestamptz' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1700 as oid, 'numeric' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 3802 as oid, 'jsonb' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl\n"
                    + ")\n"
                    + "select * from pg_type join pg_namespace on pg_type.typnamespace=pg_namespace.oid"),
            SELECT1_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select * from pg_catalog.pg_type "
                      + "join pg_catalog.pg_namespace on pg_type.typnamespace=pg_namespace.oid")) {
        // The result is not consistent with selecting from pg_type, but that's not relevant here.
        // We just want to ensure that it includes the correct common table expressions.
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testDefaultReplacePgCatalogTables() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.replace_pg_catalog_tables")) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean(1));
        assertFalse(resultSet.next());
      }
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of(
                  "with pg_namespace as (\n"
                      + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                      + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                      + "  from information_schema.schemata\n"
                      + ")\n"
                      + "select * from pg_namespace"),
              SELECT1_RESULTSET));
      // Just verify that this works, we don't care about the result.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_catalog.pg_namespace")) {
        //noinspection StatementWithEmptyBody
        while (resultSet.next()) {}
      }
    }
  }

  @Test
  public void testReplacePgCatalogTablesOff() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(
            createUrl() + "?options=-c%20spanner.replace_pg_catalog_tables=off")) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("show spanner.replace_pg_catalog_tables")) {
        assertTrue(resultSet.next());
        assertFalse(resultSet.getBoolean(1));
        assertFalse(resultSet.next());
      }

      // The query will now not be modified by PGAdapter before it is sent to Cloud Spanner.
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of("select * from pg_catalog.pg_namespace"), SELECT1_RESULTSET));
      // Just verify that this works, we don't care about the result.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_catalog.pg_namespace")) {
        //noinspection StatementWithEmptyBody
        while (resultSet.next()) {}
      }
    }
  }

  private void verifySettingIsNull(Connection connection, String setting) throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery(String.format("show %s", setting))) {
      assertTrue(resultSet.next());
      assertNull(resultSet.getString(1));
      assertFalse(resultSet.next());
    }
  }

  private void verifySettingValue(Connection connection, String setting, String value)
      throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery(String.format("show %s", setting))) {
      assertTrue(resultSet.next());
      assertEquals(value, resultSet.getString(1));
      assertFalse(resultSet.next());
    }
  }

  private void verifySettingIsUnrecognized(Connection connection, String setting) {
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> connection.createStatement().execute(String.format("show %s", setting)));
    assertEquals(
        String.format("ERROR: unrecognized configuration parameter \"%s\"", setting),
        exception.getMessage());
  }
}
