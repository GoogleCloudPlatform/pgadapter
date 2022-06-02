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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests PGAdapter using the native Go pgx driver. The Go code can be found in
 * src/test/golang/pgadapter_pgx_tests/pgx.go.
 */
@Category(GolangTest.class)
@RunWith(JUnit4.class)
public class PgxMockServerTest extends AbstractMockServerTest {
  private static PgxTest pgxTest;

  @BeforeClass
  public static void compile() throws IOException, InterruptedException {
    pgxTest = PgxTest.compile();
  }

  private GoString createConnString() {
    return new GoString(
        String.format("postgres://uid:pwd@localhost:%d/?sslmode=disable", pgServer.getLocalPort()));
  }

  @Test
  public void testHelloWorld() {
    String sql = "select 'Hello world!' as hello";

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("hello")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("Hello world!").build())
                        .build())
                .build()));

    String res = pgxTest.TestHelloWorld(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent twice
    // to the backend.
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testSelect1() {
    String sql = "SELECT 1";

    String res = pgxTest.TestSelect1(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent twice
    // to the backend.
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testQueryWithParameter() {
    String sql = "SELECT * FROM FOO WHERE BAR=$1";
    Statement statement = Statement.newBuilder(sql).bind("p1").to("baz").build();

    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("BAR")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();

    // Add a query result for the statement parameter types.
    String selectParamsSql = "select $1 from (SELECT * FROM FOO WHERE BAR=$1) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectParamsSql),
            ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));

    // Add a query result with only the metadata for the query without parameter values.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ResultSet.newBuilder().setMetadata(metadata).build()));
    // Also add a query result with both metadata and rows for the statement with parameter values.
    mockSpanner.putStatementResult(
        StatementResult.query(
            statement,
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("baz").build())
                        .build())
                .build()));

    String res = pgxTest.TestQueryWithParameter(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent three
    // times to the backend:
    // 1. DescribeStatement (results)
    // 2. DescribeStatement (parameters)
    // 3. Execute (including DescribePortal)
    assertEquals(3, requests.size());
    ExecuteSqlRequest describeStatementRequest = requests.get(0);
    assertEquals(sql, describeStatementRequest.getSql());
    assertEquals(QueryMode.PLAN, describeStatementRequest.getQueryMode());
    ExecuteSqlRequest describeParametersRequest = requests.get(1);
    assertEquals(selectParamsSql, describeParametersRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParametersRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(2);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testQueryAllDataTypes() {
    String sql = "SELECT * FROM all_types WHERE col_bigint=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String res = pgxTest.TestQueryAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. As this statement does not contain any
    // parameters, we don't need to describe the parameter types, so it is 'only' sent twice to the
    // backend.
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testInsertAllDataTypes() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
    String describeSql =
        "select $1, $2, $3, $4, $5, $6, $7, $8, $9 from (select col_bigint=$1, col_bool=$2, col_bytea=$3, col_float8=$4, col_int=$5, col_numeric=$6, col_timestamptz=$7, col_date=$8, col_varchar=$9 from all_types) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 0L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(100L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test_bytes"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(1L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .build(),
            1L));

    String res = pgxTest.TestInsertAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent three
    // times to the backend the first time it is executed:
    // 1. DescribeStatement (parameters)
    // 2. DescribeStatement (verify validity / PARSE) -- This step could be skipped.
    // 3. Execute
    assertEquals(3, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(describeSql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    ExecuteSqlRequest describeRequest = requests.get(1);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(2);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testInsertBatch() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
    String describeSql =
        "select $1, $2, $3, $4, $5, $6, $7, $8, $9 from (select col_bigint=$1, col_bool=$2, col_bytea=$3, col_float8=$4, col_int=$5, col_numeric=$6, col_timestamptz=$7, col_date=$8, col_varchar=$9 from all_types) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 0L));
    int batchSize = 10;
    for (int i = 0; i < batchSize; i++) {
      mockSpanner.putStatementResult(
          StatementResult.update(
              Statement.newBuilder(sql)
                  .bind("p1")
                  .to(100L + i)
                  .bind("p2")
                  .to(i % 2 == 0)
                  .bind("p3")
                  .to(ByteArray.copyFrom(i + "test_bytes"))
                  .bind("p4")
                  .to(3.14d + i)
                  .bind("p5")
                  .to(i)
                  .bind("p6")
                  .to(com.google.cloud.spanner.Value.pgNumeric(i + ".123"))
                  .bind("p7")
                  .to(
                      Timestamp.parseTimestamp(
                          String.format("2022-03-24T%02d:39:10.123456000Z", i)))
                  .bind("p8")
                  .to(Date.parseDate(String.format("2022-04-%02d", i + 1)))
                  .bind("p9")
                  .to("test_string" + i)
                  .build(),
              1L));
    }

    String res = pgxTest.TestInsertBatch(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent 2 times
    // to the backend to be described, and then `batchSize` times to be executed.
    // 1. DescribeStatement (parameters)
    // 2. DescribeStatement (verify validity / PARSE) -- This step could be skipped.
    // 3. Execute 10 times.
    assertEquals(batchSize + 2, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(describeSql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    assertTrue(describeParamsRequest.hasTransaction());
    // The first statement will start a (read/write) transaction.
    assertTrue(describeParamsRequest.getTransaction().hasBegin());
    ExecuteSqlRequest describeRequest = requests.get(1);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    // All following requests should use the transaction ID that was returned by the first
    // statement.
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasId());
    ByteString transactionId = describeRequest.getTransaction().getId();
    for (int i = 2; i < batchSize + 2; i++) {
      ExecuteSqlRequest executeRequest = requests.get(i);
      assertEquals(sql, executeRequest.getSql());
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertEquals(transactionId, executeRequest.getTransaction().getId());
    }
    CommitRequest commitRequest =
        mockSpanner.getRequestsOfType(CommitRequest.class).stream().findFirst().orElse(null);
    assertNotNull(commitRequest);
    assertEquals(transactionId, commitRequest.getTransactionId());
  }

  @Test
  public void testInsertNullsAllDataTypes() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
    String describeSql =
        "select $1, $2, $3, $4, $5, $6, $7, $8, $9 from (select col_bigint=$1, col_bool=$2, col_bytea=$3, col_float8=$4, col_int=$5, col_numeric=$6, col_timestamptz=$7, col_date=$8, col_varchar=$9 from all_types) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 0L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(100L)
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

    String res = pgxTest.TestInsertNullsAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent three
    // times to the backend the first time it is executed:
    // 1. DescribeStatement (parameters)
    // 2. DescribeStatement (verify validity / PARSE) -- This step could be skipped.
    // 3. Execute
    assertEquals(3, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(describeSql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    ExecuteSqlRequest describeRequest = requests.get(1);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(2);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  @Ignore("Skip until https://github.com/googleapis/java-spanner/pull/1877 has been released")
  public void testWrongDialect() {
    // Let the mock server respond with the Google SQL dialect instead of PostgreSQL. The
    // connection should be gracefully rejected. Close all open pooled Spanner objects so we know
    // that we will get a fresh one for our connection. This ensures that it will execute a query to
    // determine the dialect of the database.
    closeSpannerPool();
    try {
      mockSpanner.putStatementResult(
          StatementResult.detectDialectResult(Dialect.GOOGLE_STANDARD_SQL));

      String result = pgxTest.TestWrongDialect(createConnString());

      assertNotNull(result);
      assertTrue(result, result.contains("failed to connect to PG"));
      assertTrue(result, result.contains("The database uses dialect GOOGLE_STANDARD_SQL"));
    } finally {
      mockSpanner.putStatementResult(StatementResult.detectDialectResult(Dialect.POSTGRESQL));
      closeSpannerPool();
    }
  }
}
