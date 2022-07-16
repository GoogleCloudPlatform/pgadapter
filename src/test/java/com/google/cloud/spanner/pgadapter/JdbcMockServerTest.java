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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.jdbc.PgStatement;

@RunWith(JUnit4.class)
public class JdbcMockServerTest extends AbstractMockServerTest {
  private static final int RANDOM_RESULTS_ROW_COUNT = 10;
  private static final Statement SELECT_RANDOM = Statement.of("select * from random_table");

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
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
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
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains("Table non_existing_table not found"));
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
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains("Table non_existing_table not found"));
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
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains("Table non_existing_table not found"));
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
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains("Table non_existing_table not found"));
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

      List<ExecuteMessage> executeMessages = getWireMessagesOfType(ExecuteMessage.class);
      assertEquals(5, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(2, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages = getWireMessagesOfType(ParseMessage.class);
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

      List<ExecuteMessage> executeMessages = getWireMessagesOfType(ExecuteMessage.class);
      assertEquals(4, executeMessages.size());
      assertEquals("", executeMessages.get(0).getName());
      for (ExecuteMessage executeMessage : executeMessages.subList(1, executeMessages.size() - 1)) {
        assertEquals(describeMessage.getName(), executeMessage.getName());
        assertEquals(1, executeMessage.getMaxRows());
      }
      assertEquals("", executeMessages.get(executeMessages.size() - 1).getName());

      List<ParseMessage> parseMessages = getWireMessagesOfType(ParseMessage.class);
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

      List<ExecuteMessage> executeMessages = getWireMessagesOfType(ExecuteMessage.class);
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

      List<ParseMessage> parseMessages = getWireMessagesOfType(ParseMessage.class);
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
          "ERROR: INTERNAL: io.grpc.StatusRuntimeException: INTERNAL: test error - Statement: 'SELECT * FROM INFORMATION_SCHEMA.TABLES'",
          sqlException.getMessage());

      // Make sure that the connection is now in the aborted state.
      SQLException abortedException =
          assertThrows(
              SQLException.class,
              () -> connection.createStatement().executeQuery(SELECT2.getSql()));
      assertEquals(
          "ERROR: INVALID_ARGUMENT: current transaction is aborted, commands ignored until end of transaction block",
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
}
