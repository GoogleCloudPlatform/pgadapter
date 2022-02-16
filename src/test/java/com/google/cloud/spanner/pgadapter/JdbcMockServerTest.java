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
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.bouncycastle.util.encoders.Base64;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcMockServerTest extends AbstractMockServerTest {
  private static final ResultSetMetadata ALL_TYPES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("col_bigint")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_bool")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_bytea")
                          .setType(Type.newBuilder().setCode(TypeCode.BYTES).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_float8")
                          .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_numeric")
                          .setType(
                              Type.newBuilder()
                                  .setCode(TypeCode.NUMERIC)
                                  .setTypeAnnotation(TypeAnnotationCode.PG_NUMERIC)
                                  .build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_timestamptz")
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_varchar")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build()))
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet ALL_TYPES_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .setMetadata(ALL_TYPES_METADATA)
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("1").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              Base64.toBase64String("test".getBytes(StandardCharsets.UTF_8)))
                          .build())
                  .addValues(Value.newBuilder().setNumberValue(3.14d).build())
                  .addValues(Value.newBuilder().setStringValue("6.626").build())
                  .addValues(
                      Value.newBuilder().setStringValue("2022-02-16T13:18:02.123456789Z").build())
                  .addValues(Value.newBuilder().setStringValue("test").build())
                  .build())
          .build();

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
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
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testQueryWithParameters() throws SQLException {
    String jdbcSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_numeric, col_timestamptz, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            + "and col_bytea=? "
            + "and col_float8=? "
            //        + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_varchar=?";
    String pgSql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_numeric, col_timestamptz, col_varchar "
            + "from all_types "
            + "where col_bigint=$1 "
            + "and col_bool=$2 "
            + "and col_bytea=$3 "
            + "and col_float8=$4 "
            //        + "and col_numeric=$5 "
            + "and col_timestamptz=$5 "
            + "and col_varchar=$6";
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
                .to(3.14d)
                // .bind("p5").to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p5")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123457000Z"))
                .bind("p6")
                .to("test")
                .build(),
            ALL_TYPES_RESULTSET));

    OffsetDateTime zonedDateTime =
        LocalDateTime.of(2022, 2, 16, 13, 18, 2, 123456789).atOffset(ZoneOffset.UTC);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcSql)) {
        preparedStatement.setLong(1, 1L);
        preparedStatement.setBoolean(2, true);
        preparedStatement.setBytes(3, "test".getBytes(StandardCharsets.UTF_8));
        preparedStatement.setDouble(4, 3.14d);
        // preparedStatement.setBigDecimal(5, new BigDecimal("6.626"));
        preparedStatement.setTimestamp(5, java.sql.Timestamp.from(Instant.from(zonedDateTime)));
        preparedStatement.setString(6, "test");
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // The PG JDBC driver will internally split the following SQL string into two statements and
        // execute these sequentially. We still get the results back as if they were executed as one
        // batch on the same statement.
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

    // The DML statements are split by the JDBC driver and sent as separate statements to PgAdapter.
    // PgAdapter therefore also simply executes these as separate DML statements.
    assertTrue(mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).isEmpty());
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(1).getSql());
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
  }
}
