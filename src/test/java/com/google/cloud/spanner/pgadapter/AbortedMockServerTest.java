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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
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
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.PgStatement;

@RunWith(JUnit4.class)
public class AbortedMockServerTest extends AbstractMockServerTest {
  private static final int RANDOM_RESULTS_ROW_COUNT = 100;
  private static final Statement SELECT_RANDOM = Statement.of("select * from random_table");
  private static final ImmutableList<String> JDBC_STARTUP_STATEMENTS =
      ImmutableList.of(
          "SET extra_float_digits = 3", "SET application_name = 'PostgreSQL JDBC Driver'");

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    addRandomResultResults();
    mockSpanner.setAbortProbability(0.2);
  }

  private static void addRandomResultResults() {
    RandomResultSetGenerator generator =
        new RandomResultSetGenerator(RANDOM_RESULTS_ROW_COUNT, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(SELECT_RANDOM, generator.generate()));
  }

  private String getExpectedInitialApplicationName() {
    return "PostgreSQL JDBC Driver";
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default extended
   * mode for queries and DML statements.
   */
  private String createUrl(String extraOptions) {
    return String.format("jdbc:postgresql://localhost:%d/" + extraOptions, pgServer.getLocalPort());
  }

  private Connection createConnection() throws SQLException {
    return createConnection("");
  }

  private Connection createConnection(String extraOptions) throws SQLException {
    Connection connection = DriverManager.getConnection(createUrl(extraOptions));
    connection.setAutoCommit(false);
    return connection;
  }

  @Test
  public void testQuery() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = createConnection()) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
    assertFalse(mockSpanner.getTransactionsStarted().isEmpty());
  }

  @Test
  public void testSelectCurrentSchema() throws SQLException {
    String sql = "SELECT current_schema";

    try (Connection connection = createConnection()) {
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

      try (Connection connection = createConnection()) {
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

      try (Connection connection = createConnection()) {
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

    try (Connection connection = createConnection()) {
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

    try (Connection connection = createConnection()) {
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
  public void testQueryHint() throws SQLException {
    String sql = "/* @OPTIMIZER_VERSION=1 */ SELECT 1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    try (Connection connection = createConnection()) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testQueryWithParameters() throws SQLException {
    String jdbcSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_int=? "
            + "and col_float8=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=? "
            + "and col_jsonb=?";
    String pgSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
            + "from all_types "
            + "where col_bigint=$1 "
            + "and col_bool=$2 "
            + "and col_bytea=$3 "
            + "and col_int=$4 "
            + "and col_float8=$5 "
            + "and col_numeric=$6 "
            + "and col_timestamptz=$7 "
            + "and col_date=$8 "
            + "and col_varchar=$9 "
            + "and col_jsonb=$10";
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
                .bind("p10")
                .to("{\"key\": \"value\"}")
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
      try (Connection connection = createConnection()) {
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
          preparedStatement.setString(++index, "{\"key\": \"value\"}");
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            index = 0;
            assertEquals(1L, resultSet.getLong(++index));
            assertTrue(resultSet.getBoolean(++index));
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
            assertEquals(3.14d, resultSet.getDouble(++index), 0.0d);
            assertEquals(100, resultSet.getInt(++index));
            assertEquals(new BigDecimal("6.626"), resultSet.getBigDecimal(++index));
            assertEquals(
                truncatedOffsetDateTime, resultSet.getObject(++index, OffsetDateTime.class));
            assertEquals(LocalDate.of(2022, 3, 29), resultSet.getObject(++index, LocalDate.class));
            assertEquals("test", resultSet.getString(++index));
            assertEquals("{\"key\": \"value\"}", resultSet.getString(++index));
            assertFalse(resultSet.next());
          }
        }
      }
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
      try (Connection connection = createConnection()) {
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
          connection.commit();
        }
      }
    }
  }

  @Test
  public void testMultipleQueriesInTransaction() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = createConnection()) {
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
    String sql = "select * from non_existing_table where id=?";
    String pgSql = "select * from non_existing_table where id=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(pgSql).bind("p1").to(1L).build(),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = createConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatement.setLong(1, 1L);
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeQuery);
        assertEquals(
            "ERROR: Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
      }
    }
  }

  @Test
  public void testDmlWithNonExistingTable() throws SQLException {
    String sql = "update non_existing_table set value=? where id=?";
    String pgSql = "update non_existing_table set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(pgSql).bind("p1").to("foo").bind("p2").to(1L).build(),
            Status.NOT_FOUND
                .withDescription("Table non_existing_table not found")
                .asRuntimeException()));
    try (Connection connection = createConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatement.setString(1, "foo");
        preparedStatement.setLong(2, 1L);
        SQLException exception = assertThrows(SQLException.class, preparedStatement::executeUpdate);
        assertEquals("ERROR: Table non_existing_table not found", exception.getMessage());
      }
    }
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

    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals(
            "ERROR: Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            exception.getMessage());
      }
    }
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
    try (Connection connection = createConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        SQLException exception =
            assertThrows(SQLException.class, preparedStatement::getParameterMetaData);
        assertEquals("ERROR: Table non_existing_table not found", exception.getMessage());
      }
    }
  }

  @Test
  public void testDescribeDmlWithSchemaPrefix() throws SQLException {
    String sql = "update public.my_table set value=? where id=?";
    String describeSql = "update public.my_table set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    try (Connection connection = createConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(Types.VARCHAR, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }
  }

  @Test
  public void testDescribeDmlWithQuotedSchemaPrefix() throws SQLException {
    String sql = "update \"public\".\"my_table\" set value=? where id=?";
    String describeSql = "update \"public\".\"my_table\" set value=$1 where id=$2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    try (Connection connection = createConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        ParameterMetaData metadata = preparedStatement.getParameterMetaData();
        assertEquals(Types.VARCHAR, metadata.getParameterType(1));
        assertEquals(Types.BIGINT, metadata.getParameterType(2));
      }
    }
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
    try (Connection connection = createConnection()) {
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
  }

  @Test
  public void testTwoDmlStatements_withError() throws SQLException {
    try (Connection connection = createConnection()) {
      try (java.sql.Statement statement = connection.createStatement()) {
        SQLException exception =
            assertThrows(
                SQLException.class,
                () -> statement.execute(String.format("%s;%s;", INSERT_STATEMENT, INVALID_DML)));
        assertEquals("ERROR: Statement is invalid.", exception.getMessage());
      }
    }
  }

  @Test
  public void testJdbcBatch() throws SQLException {
    try (Connection connection = createConnection()) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(UPDATE_STATEMENT.getSql());
        int[] updateCounts = statement.executeBatch();

        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(2, updateCounts[1]);
      }
    }
  }

  @Test
  public void testTwoQueries() throws SQLException {
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
  }

  @Test
  public void testCursorFailsHalfway() throws SQLException {
    mockSpanner.setExecuteStreamingSqlExecutionTime(
        SimulatedExecutionTime.ofStreamException(Status.DATA_LOSS.asRuntimeException(), 2));
    try (Connection connection = createConnection()) {
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
  }

  @Test
  public void testRandomResults() throws SQLException {
    for (boolean binary : new boolean[] {true, false}) {
      // Also get the random results using the normal Spanner client to compare the results with
      // what is returned by PGAdapter.
      Spanner spanner =
          SpannerOptions.newBuilder()
              .setProjectId("p")
              .setHost(String.format("http://localhost:%d", spannerServer.getPort()))
              .setCredentials(NoCredentials.getInstance())
              .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
              .setClientLibToken("pg-adapter")
              .build()
              .getService();
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));
      com.google.cloud.spanner.ResultSet spannerResult =
          client.singleUse().executeQuery(SELECT_RANDOM);

      String binaryTransferEnable =
          "?binaryTransferEnable="
              + ImmutableList.of(
                      Oid.BOOL,
                      Oid.BYTEA,
                      Oid.VARCHAR,
                      Oid.NUMERIC,
                      Oid.FLOAT8,
                      Oid.INT8,
                      Oid.DATE,
                      Oid.TIMESTAMPTZ)
                  .stream()
                  .map(String::valueOf)
                  .collect(Collectors.joining(","));

      final int fetchSize = 3;
      try (Connection connection = createConnection(binaryTransferEnable)) {
        connection.createStatement().execute("set time zone utc");
        connection.setAutoCommit(false);
        connection.unwrap(PGConnection.class).setPrepareThreshold(binary ? -1 : 5);
        try (PreparedStatement statement = connection.prepareStatement(SELECT_RANDOM.getSql())) {
          statement.setFetchSize(fetchSize);
          try (ResultSet resultSet = statement.executeQuery()) {
            int rowCount = 0;
            while (resultSet.next()) {
              assertTrue(spannerResult.next());
              for (int col = 0; col < resultSet.getMetaData().getColumnCount(); col++) {
                // TODO: Remove once we have a replacement for pg_type, as the JDBC driver will try
                // to read type information from the backend when it hits an 'unknown' type (jsonb
                // is not one of the types that the JDBC driver will load automatically).
                if (col == 5 || col == 14) {
                  resultSet.getString(col + 1);
                } else {
                  resultSet.getObject(col + 1);
                }
              }
              assertEqual(spannerResult, resultSet);
              rowCount++;
            }
            assertEquals(RANDOM_RESULTS_ROW_COUNT, rowCount);
          }
        }
        connection.commit();
      }

      // Close the resources used by the normal Spanner client.
      spannerResult.close();
      spanner.close();
    }
  }

  private void assertEqual(com.google.cloud.spanner.ResultSet spannerResult, ResultSet pgResult)
      throws SQLException {
    assertEquals(spannerResult.getColumnCount(), pgResult.getMetaData().getColumnCount());
    for (int col = 0; col < spannerResult.getColumnCount(); col++) {
      if (spannerResult.isNull(col)) {
        assertNull(pgResult.getObject(col + 1));
        assertTrue(pgResult.wasNull());
        continue;
      }

      switch (spannerResult.getColumnType(col).getCode()) {
        case BOOL:
          assertEquals(spannerResult.getBoolean(col), pgResult.getBoolean(col + 1));
          break;
        case INT64:
          assertEquals(spannerResult.getLong(col), pgResult.getLong(col + 1));
          break;
        case FLOAT64:
          assertEquals(spannerResult.getDouble(col), pgResult.getDouble(col + 1), 0.0d);
          break;
        case PG_NUMERIC:
        case STRING:
          assertEquals(spannerResult.getString(col), pgResult.getString(col + 1));
          break;
        case BYTES:
          assertArrayEquals(spannerResult.getBytes(col).toByteArray(), pgResult.getBytes(col + 1));
          break;
        case TIMESTAMP:
          // Compare milliseconds, as PostgreSQL does not natively support nanosecond precision, and
          // this is lost when using binary encoding.
          assertEquals(
              spannerResult.getTimestamp(col).toSqlTimestamp().getTime(),
              pgResult.getTimestamp(col + 1).getTime());
          break;
        case DATE:
          LocalDate expected =
              LocalDate.of(
                  spannerResult.getDate(col).getYear(),
                  spannerResult.getDate(col).getMonth(),
                  spannerResult.getDate(col).getDayOfMonth());
          if ((expected.getYear() == 1582 && expected.getMonth() == Month.OCTOBER)
              || (expected.getYear() <= 1582
                  && expected.getMonth() == Month.FEBRUARY
                  && expected.getDayOfMonth() > 20)) {
            // Just assert that we can get the value. Dates in the Julian/Gregorian cutover period
            // are weird, as are potential intercalaris values.
            assertNotNull(pgResult.getDate(col + 1));
          } else {
            assertEquals(expected, pgResult.getDate(col + 1).toLocalDate());
          }
          break;
        case PG_JSONB:
          assertEquals(spannerResult.getPgJsonb(col), pgResult.getString(col + 1));
          break;
        case ARRAY:
          break;
        case NUMERIC:
        case JSON:
        case STRUCT:
          fail("unsupported PG type: " + spannerResult.getColumnType(col));
      }
    }
  }

  @Test
  public void testShowValidSetting() throws SQLException {
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      // DATESTYLE is set to 'ISO' by the JDBC driver at startup ('ISO, MDY' in 42.7.0).
      try (ResultSet resultSet = connection.createStatement().executeQuery("show DATESTYLE")) {
        assertTrue(resultSet.next());
        String dateStyle = resultSet.getString(1);
        assertTrue(dateStyle, "ISO".equals(dateStyle) || "ISO, MDY".equals(dateStyle));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testShowInvalidSetting() throws SQLException {
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      String originalDateStyle;
      try (ResultSet resultSet = connection.createStatement().executeQuery("show datestyle")) {
        assertTrue(resultSet.next());
        originalDateStyle = resultSet.getString(1);
        assertTrue(
            originalDateStyle,
            "ISO".equals(originalDateStyle) || "ISO, MDY".equals(originalDateStyle));
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
        assertEquals(originalDateStyle, resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testResetInvalidSetting() throws SQLException {
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      // Resetting an undefined extension setting is allowed by PostgreSQL, and will effectively set
      // the extension setting to null.
      connection.createStatement().execute("reset spanner.some_setting");

      verifySettingIsNull(connection, "spanner.some_setting");
    }
  }

  @Test
  public void testCommitSet() throws SQLException {
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      // First define the extension setting.
      connection.createStatement().execute("set spanner.random_setting to '100'");
      connection.commit();

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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      // Verify that the initial value is undefined.
      verifySettingIsUnrecognized(connection, "spanner.custom_setting");
      connection.rollback();

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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
        assertEquals(361, count);
      }
    }
  }

  @Test
  public void testResetAll() throws SQLException {
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
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
    try (Connection connection = createConnection()) {
      connection.createStatement().execute("set application_name to ''");
      verifySettingValue(connection, "application_name", "");
    }
  }

  @Test
  public void testSetTimeZone() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.createStatement().execute("set time zone 'IST'");
      verifySettingValue(connection, "timezone", "Asia/Kolkata");
    }
  }

  @Test
  public void testSetTimeZoneToDefault() throws SQLException {
    try (Connection connection = createConnection("?options=-c%%20timezone=IST")) {
      connection.createStatement().execute("set time zone default");
      verifySettingValue(connection, "timezone", "Asia/Kolkata");
    }
  }

  @Test
  public void testSetTimeZoneToLocal() throws SQLException {
    try (Connection connection = createConnection("?options=-c%%20timezone=IST")) {
      connection.createStatement().execute("set time zone local");
      verifySettingValue(connection, "timezone", "Asia/Kolkata");
    }
  }

  @Test
  public void testSetTimeZoneWithTransactionCommit() throws SQLException {
    try (Connection connection = createConnection()) {
      connection.setAutoCommit(false);
      connection.createStatement().execute("set time zone 'UTC'");
      verifySettingValue(connection, "timezone", "UTC");
      connection.commit();
      verifySettingValue(connection, "timezone", "UTC");
    }
  }

  @Test
  public void testSetTimeZoneWithTransactionRollback() throws SQLException {
    try (Connection connection = createConnection()) {
      String originalTimeZone = null;
      try (ResultSet rs = connection.createStatement().executeQuery("SHOW TIMEZONE")) {
        assertTrue(rs.next());
        originalTimeZone = rs.getString(1);
        assertFalse(rs.next());
      }
      assertNotNull(originalTimeZone);
      connection.setAutoCommit(false);

      connection.createStatement().execute("set time zone 'UTC'");
      verifySettingValue(connection, "time zone", "UTC");
      connection.rollback();
      verifySettingValue(connection, "time zone", originalTimeZone);
    }
  }

  @Test
  public void testSettingsAreUniqueToConnections() throws SQLException {
    // Verify that each new connection gets a separate set of settings.
    for (int connectionNum = 0; connectionNum < 5; connectionNum++) {
      try (Connection connection = createConnection()) {
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
        createConnection(
            "?options=-c%%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction")) {
      verifySettingValue(
          connection, "spanner.ddl_transaction_mode", "AutocommitExplicitTransaction");
    }
  }

  @Test
  public void testMultipleSettingsInConnectionOptions() throws SQLException {
    try (Connection connection =
        createConnection(
            "?options=-c%%20spanner.setting1=value1%%20-c%%20spanner.setting2=value2")) {
      verifySettingValue(connection, "spanner.setting1", "value1");
      verifySettingValue(connection, "spanner.setting2", "value2");
    }
  }

  @Test
  public void testServerVersionInConnectionOptions() throws SQLException {
    try (Connection connection = createConnection("?options=-c%%20server_version=4.1")) {
      verifySettingValue(connection, "server_version", "4.1");
      verifySettingValue(connection, "server_version_num", "40001");
    }
  }

  @Test
  public void testCustomServerVersionInConnectionOptions() throws SQLException {
    try (Connection connection =
        createConnection("?options=-c%%20server_version=5.2 custom version")) {
      verifySettingValue(connection, "server_version", "5.2 custom version");
      verifySettingValue(connection, "server_version_num", "50002");
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
