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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.CopyInMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.Mutation;
import com.google.spanner.v1.Mutation.OperationCase;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.core.Oid;

/**
 * Tests PGAdapter using the native Go pgx v5 driver. The Go code can be found in
 * src/test/golang/pgadapter_pgx5_tests/pgx.go.
 */
@Category(GolangTest.class)
@RunWith(Parameterized.class)
public class Pgx5MockServerTest extends AbstractMockServerTest {
  private static PgxTest pgxTest;

  @Rule public Timeout globalTimeout = Timeout.seconds(300L);

  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void compile() throws IOException, InterruptedException {
    pgxTest = GolangTest.compile("pgadapter_pgx5_tests/pgx.go", PgxTest.class);
  }

  private GoString createConnString() {
    if (useDomainSocket) {
      return new GoString(String.format("host=/tmp port=%d", pgServer.getLocalPort()));
    }
    return new GoString(
        String.format("postgres://uid:pwd@localhost:%d/?sslmode=disable", pgServer.getLocalPort()));
  }

  @Test
  public void testHelloWorld() {
    String sql = "select 'Hello world!' as hello";

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
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
  public void testShowApplicationName() {
    String res = pgxTest.TestShowApplicationName(createConnString());

    assertNull(res);

    // This should all be handled in PGAdapter.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
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
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build()))
                .build()));
    // Add a query result with both metadata and rows for the statement with parameter values.
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
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend:
    // 1. DescribeStatement (parameters + results)
    // 2. Execute (including DescribePortal)
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeStatementRequest = requests.get(0);
    assertEquals(sql, describeStatementRequest.getSql());
    assertEquals(QueryMode.PLAN, describeStatementRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testQueryAllDataTypes() {
    String sql =
        "SELECT col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb FROM all_types WHERE col_bigint=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    // Request the data of each column once in both text and binary format to ensure that we support
    // each format for all data types, *AND* that PGAdapter actually uses the format that the client
    // requests.
    for (int oid :
        new int[] {
          Oid.INT8,
          Oid.BOOL,
          Oid.BYTEA,
          Oid.FLOAT4,
          Oid.FLOAT8,
          Oid.INT4,
          Oid.NUMERIC,
          Oid.DATE,
          Oid.TIMESTAMPTZ,
          Oid.VARCHAR,
          Oid.JSONB,
        }) {
      for (int format : new int[] {0, 1}) {
        String res = pgxTest.TestQueryAllDataTypes(createConnString(), oid, format);

        assertNull(res);
        List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
        // pgx by default always uses prepared statements. As this statement does not contain any
        // parameters, we don't need to describe the parameter types, so it is 'only' sent twice to
        // the
        // backend.
        assertEquals(2, requests.size());
        ExecuteSqlRequest describeRequest = requests.get(0);
        assertEquals(sql, describeRequest.getSql());
        assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
        ExecuteSqlRequest executeRequest = requests.get(1);
        assertEquals(sql, executeRequest.getSql());
        assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

        mockSpanner.clearRequests();
      }
    }
  }

  @Test
  public void testUpdateAllDataTypes() {
    String sql =
        "UPDATE \"all_types\" SET \"col_bigint\"=$1,\"col_bool\"=$2,\"col_bytea\"=$3,\"col_float8\"=$4,\"col_int\"=$5,\"col_numeric\"=$6,\"col_timestamptz\"=$7,\"col_date\"=$8,\"col_varchar\"=$9,\"col_jsonb\"=$10 WHERE \"col_varchar\" = $11";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON,
                            TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
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
                .to(com.google.cloud.spanner.Value.pgNumeric("6626e-3"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .bind("p11")
                .to("test")
                .build(),
            1L));

    String res = pgxTest.TestUpdateAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement (parameters)
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(sql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testInsertAllDataTypes() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
            + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON),
                        true))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
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
                .to(com.google.cloud.spanner.Value.pgNumeric("6626e-3"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .bind("p11")
                .toInt64Array(Arrays.asList(100L, null, 200L))
                .bind("p12")
                .toBoolArray(Arrays.asList(true, null, false))
                .bind("p13")
                .toBytesArray(
                    Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")))
                .bind("p14")
                .toFloat64Array(Arrays.asList(3.14d, null, 6.626d))
                .bind("p15")
                .toInt64Array(Arrays.asList(-1L, null, -2L))
                .bind("p16")
                .toPgNumericArray(Arrays.asList("-6626e-3", null, "314e-2"))
                .bind("p17")
                .toTimestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"),
                        null,
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z")))
                .bind("p18")
                .toDateArray(
                    Arrays.asList(Date.parseDate("2022-04-02"), null, Date.parseDate("1970-01-01")))
                .bind("p19")
                .toStringArray(Arrays.asList("string1", null, "string2"))
                .bind("p20")
                .toPgJsonbArray(Arrays.asList("{\"key\":\"value1\"}", null, "{\"key\":\"value2\"}"))
                .build(),
            1L));

    String res = pgxTest.TestInsertAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement (parameters)
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(sql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testInsertAllDataTypesReturning() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning *";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    ALL_TYPES_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.INT64,
                                        TypeCode.BOOL,
                                        TypeCode.BYTES,
                                        TypeCode.FLOAT64,
                                        TypeCode.INT64,
                                        TypeCode.NUMERIC,
                                        TypeCode.TIMESTAMP,
                                        TypeCode.DATE,
                                        TypeCode.STRING,
                                        TypeCode.JSON))
                                .getUndeclaredParameters()))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
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
                .to(com.google.cloud.spanner.Value.pgNumeric("6626e-3"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ALL_TYPES_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.INT64,
                                        TypeCode.BOOL,
                                        TypeCode.BYTES,
                                        TypeCode.FLOAT64,
                                        TypeCode.INT64,
                                        TypeCode.NUMERIC,
                                        TypeCode.TIMESTAMP,
                                        TypeCode.DATE,
                                        TypeCode.STRING,
                                        TypeCode.JSON))
                                .getUndeclaredParameters()))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(ALL_TYPES_RESULTSET.getRows(0))
                .build()));

    String res = pgxTest.TestInsertAllDataTypesReturning(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement (parameters)
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(sql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testInsertBatch() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
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
                  .to(com.google.cloud.spanner.Value.pgNumeric(i == 0 ? "123e-3" : i + "123e-3"))
                  .bind("p7")
                  .to(
                      Timestamp.parseTimestamp(
                          String.format("2022-03-24T%02d:39:10.123456000Z", i)))
                  .bind("p8")
                  .to(Date.parseDate(String.format("2022-04-%02d", i + 1)))
                  .bind("p9")
                  .to("test_string" + i)
                  .bind("p10")
                  .to(
                      com.google.cloud.spanner.Value.pgJsonb(
                          String.format("{\"key\": \"value%d\"}", i)))
                  .build(),
              1L));
    }

    String res = pgxTest.TestInsertBatch(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent once
    // to the backend to be described, and then `batchSize` times to be executed (which is sent as
    // one BatchDML request).
    // 1. DescribeStatement (parameters)
    // 2. Execute 10 times.
    assertEquals(1, requests.size());
    ExecuteSqlRequest describeParamsRequest = requests.get(0);
    assertEquals(sql, describeParamsRequest.getSql());
    assertEquals(QueryMode.PLAN, describeParamsRequest.getQueryMode());
    assertTrue(describeParamsRequest.getTransaction().hasBegin());
    assertTrue(describeParamsRequest.getTransaction().getBegin().hasReadWrite());

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchDmlRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(batchSize, batchDmlRequest.getStatementsCount());
    assertTrue(batchDmlRequest.getTransaction().hasBegin());

    // There are two commit requests:
    // 1. One for the analyzeUpdate to describe the insert statement.
    // 2. One for the actual batch.
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testMixedBatch() {
    String insertSql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));

    String selectSql = "select count(*) from all_types where col_bool=$1";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("c")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .build())
            .build();
    ResultSet resultSet =
        ResultSet.newBuilder()
            .setMetadata(metadata)
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("3").build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectSql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql).bind("p1").to(true).build(), resultSet));

    String updateSql = "update all_types set col_bool=false where col_bool=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.BOOL)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(updateSql).bind("p1").to(true).build(), 3L));

    int batchSize = 5;
    for (int i = 0; i < batchSize; i++) {
      mockSpanner.putStatementResult(
          StatementResult.update(
              Statement.newBuilder(insertSql)
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
                  .to(com.google.cloud.spanner.Value.pgNumeric(i == 0 ? "123e-3" : i + "123e-3"))
                  .bind("p7")
                  .to(
                      Timestamp.parseTimestamp(
                          String.format("2022-03-24T%02d:39:10.123456000Z", i)))
                  .bind("p8")
                  .to(Date.parseDate(String.format("2022-04-%02d", i + 1)))
                  .bind("p9")
                  .to("test_string" + i)
                  .bind("p10")
                  .to(
                      com.google.cloud.spanner.Value.pgJsonb(
                          String.format("{\"key\": \"value%d\"}", i)))
                  .build(),
              1L));
    }

    String res = pgxTest.TestMixedBatch(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. In addition, pgx first describes all
    // statements in a batch before it executes the batch. The describe-messages that it sends do
    // not use the current transaction.
    // That means that we get the following list of statements:
    // 1. Describe parameters of insert statement in PLAN mode.
    // 2. Describe parameters and columns of select statement in PLAN mode.
    // 3. Describe parameters of update statement in PLAN mode.
    // 4. Execute select statement.
    // 5. Execute update statement.
    // In addition, we get one ExecuteBatchDml request for the insert statements.
    assertEquals(5, requests.size());

    // NOTE: pgx will first create prepared statements for sql strings that it does not yet know.
    // All those describe statement messages will be executed in separate (single-use) transactions.
    // The order in which the describe statements are executed is random.

    List<ExecuteSqlRequest> describeInsertRequests =
        requests.stream()
            .filter(
                request ->
                    request.getSql().equals(insertSql)
                        && request.getQueryMode().equals(QueryMode.PLAN))
            .collect(Collectors.toList());
    assertEquals(1, describeInsertRequests.size());
    // TODO(#477): These Describe-message flows could use single-use read/write transactions.
    assertTrue(describeInsertRequests.get(0).getTransaction().hasBegin());
    assertTrue(describeInsertRequests.get(0).getTransaction().getBegin().hasReadWrite());

    List<ExecuteSqlRequest> describeSelectRequests =
        requests.stream()
            .filter(
                request ->
                    request.getSql().equals(selectSql) && request.getQueryMode() == QueryMode.PLAN)
            .collect(Collectors.toList());
    assertEquals(1, describeSelectRequests.size());
    assertTrue(describeSelectRequests.get(0).getTransaction().hasId());

    List<ExecuteSqlRequest> describeUpdateRequests =
        requests.stream()
            .filter(
                request ->
                    request.getSql().equals(updateSql) && request.getQueryMode() == QueryMode.PLAN)
            .collect(Collectors.toList());
    assertEquals(1, describeUpdateRequests.size());
    assertTrue(describeUpdateRequests.get(0).getTransaction().hasId());

    // From here we start with the actual statement execution.
    ExecuteSqlRequest executeSelectRequest = requests.get(3);
    assertEquals(selectSql, executeSelectRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeSelectRequest.getQueryMode());
    // The SELECT statement should use the transaction that was started by the BatchDml request.
    assertTrue(executeSelectRequest.getTransaction().hasId());

    ExecuteSqlRequest executeUpdateRequest = requests.get(4);
    assertEquals(updateSql, executeUpdateRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeUpdateRequest.getQueryMode());
    assertTrue(executeUpdateRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchDmlRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(batchSize, batchDmlRequest.getStatementsCount());
    assertTrue(batchDmlRequest.getTransaction().hasBegin());
    for (int i = 0; i < batchSize; i++) {
      assertEquals(insertSql, batchDmlRequest.getStatements(i).getSql());
    }

    // There are two commit requests:
    // 1. Commit the describe requests
    // 2. Commit the actual transaction.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(2, commitRequests.size());

    // Verify that the Batch DML request was sent first, and that the following ExecuteSql requests
    // used the same transaction ids.
    List<AbstractMessage> allRequests =
        mockSpanner.getRequests().stream()
            .filter(
                request ->
                    request instanceof ExecuteSqlRequest
                        || request instanceof ExecuteBatchDmlRequest
                        || request instanceof CommitRequest)
            .collect(Collectors.toList());
    // 9 == 3 Commit + 1 Batch DML + 5 ExecuteSql.
    assertEquals(8, allRequests.size());

    // We don't know the exact order of the DESCRIBE requests.
    // The order of EXECUTE requests is known and fixed.

    // The (theoretical) order of DESCRIBE requests is:
    // 1. Describe parameters of insert statement in PLAN mode.
    // 2. Commit.
    // 3. Describe parameters and columns of select statement in PLAN mode.
    // 4. Describe parameters of update statement in PLAN mode.
    // 5. Commit.

    // The fixed order of EXECUTE requests is:
    // 6. Execute insert batch (ExecuteBatchDml).
    // 7. Execute select statement.
    // 8. Execute update statement.
    // 9. Commit transaction.
    assertEquals(
        1,
        allRequests.subList(0, 4).stream()
            .filter(request -> request instanceof CommitRequest)
            .count());
    assertEquals(
        3,
        allRequests.subList(0, 4).stream()
            .filter(
                request ->
                    request instanceof ExecuteSqlRequest
                        && ((ExecuteSqlRequest) request).getQueryMode() == QueryMode.PLAN)
            .count());
    assertEquals(ExecuteBatchDmlRequest.class, allRequests.get(4).getClass());
    assertEquals(ExecuteSqlRequest.class, allRequests.get(5).getClass());
    assertEquals(ExecuteSqlRequest.class, allRequests.get(6).getClass());
    assertEquals(CommitRequest.class, allRequests.get(7).getClass());

    ByteString transactionId = ((CommitRequest) allRequests.get(7)).getTransactionId();
    assertEquals(transactionId, ((ExecuteSqlRequest) allRequests.get(5)).getTransaction().getId());
    assertEquals(transactionId, ((ExecuteSqlRequest) allRequests.get(6)).getTransaction().getId());
  }

  @Test
  public void testBatchPrepareError() {
    String insertSql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    // This select statement will fail during the PREPARE phase that pgx executes for all statements
    // before actually executing the batch.
    String invalidSelectSql = "select count(*) from non_existent_table where col_bool=$1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(invalidSelectSql), Status.NOT_FOUND.asRuntimeException()));
    // This statement will never be analyzed or executed.
    String updateSql = "update all_types set col_bool=false where col_bool=$1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSql), 0L));

    String res = pgxTest.TestBatchError(createConnString());

    assertNotNull(res);
    assertTrue(res, res.contains("NOT_FOUND"));
    assertTrue(res, res.contains(invalidSelectSql));

    // pgx will not execute any of the statements in the batch, as the select statement failed when
    // it was prepared.
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    // pgx will try to execute PREPARE for all the statements in the batch, but then stop at the
    // first error. There is no guarantee in which order pgx will prepare the statements, so we
    // don't know exactly which ones are being prepared and which ones are not.
    List<ExecuteSqlRequest> prepareRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertTrue(prepareRequests.size() > 0);
    assertTrue(
        prepareRequests.stream()
            .anyMatch(
                request ->
                    request.getSql().equals(invalidSelectSql)
                        && request.getQueryMode() == QueryMode.PLAN));
    assertFalse(
        prepareRequests.stream().anyMatch(request -> request.getQueryMode() == QueryMode.NORMAL));
  }

  @Test
  public void testBatchExecutionError() {
    String insertSql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    int batchSize = 3;
    for (int i = 0; i < batchSize; i++) {
      Statement statement =
          Statement.newBuilder(insertSql)
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
              .to(com.google.cloud.spanner.Value.pgNumeric(i == 0 ? "123e-3" : i + "123e-3"))
              .bind("p7")
              .to(Timestamp.parseTimestamp(String.format("2022-03-24T%02d:39:10.123456000Z", i)))
              .bind("p8")
              .to(Date.parseDate(String.format("2022-04-%02d", i + 1)))
              .bind("p9")
              .to("test_string" + i)
              .bind("p10")
              .to(
                  com.google.cloud.spanner.Value.pgJsonb(
                      String.format("{\"key\": \"value%d\"}", i)))
              .build();
      if (i == 1) {
        mockSpanner.putStatementResult(
            StatementResult.exception(statement, Status.ALREADY_EXISTS.asRuntimeException()));
      } else {
        mockSpanner.putStatementResult(StatementResult.update(statement, 1L));
      }
    }

    String res = pgxTest.TestBatchExecutionError(createConnString());

    assertNotNull(res);
    assertTrue(res, res.contains("closing batch result returned error"));
    assertTrue(res, res.contains("ALREADY_EXISTS"));

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).stream()
            .findAny()
            .orElseThrow(
                () ->
                    SpannerExceptionFactory.newSpannerException(
                        ErrorCode.NOT_FOUND, "ExecuteBatchDmlRequest not found"));
    assertEquals(3, request.getStatementsCount());
    for (ExecuteBatchDmlRequest.Statement statement : request.getStatementsList()) {
      assertEquals(insertSql, statement.getSql());
    }
  }

  @Test
  public void testInsertNullsAllDataTypes() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
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
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb(null))
                .build(),
            1L));

    String res = pgxTest.TestInsertNullsAllDataTypes(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that each request is sent two
    // times to the backend the first time it is executed:
    // 1. DescribeStatement (parameters)
    // 2. Execute
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
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
    } finally {
      mockSpanner.putStatementResult(StatementResult.detectDialectResult(Dialect.POSTGRESQL));
      closeSpannerPool();
    }
  }

  @Test
  public void testCopyIn() {
    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String sql =
        "select \"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\", \"col_jsonb\" "
            + "from \"all_types\"";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .build()));

    String res = pgxTest.TestCopyIn(createConnString());
    assertNull(res);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest request = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, request.getMutationsCount());
    Mutation mutation = request.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(2, mutation.getInsert().getValuesCount());
    assertEquals(10, mutation.getInsert().getColumnsCount());
    ListValue insert = mutation.getInsert().getValues(0);
    assertEquals("1", insert.getValues(0).getStringValue());
    assertTrue(insert.getValues(1).getBoolValue());
    assertEquals(
        Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}),
        insert.getValues(2).getStringValue());
    assertEquals(3.14, insert.getValues(3).getNumberValue(), 0.0);
    assertEquals("10", insert.getValues(4).getStringValue());
    assertEquals("6.626", insert.getValues(5).getStringValue());
    assertEquals("2022-03-24T12:39:10.123456000Z", insert.getValues(6).getStringValue());
    assertEquals("2022-07-01", insert.getValues(7).getStringValue());
    assertEquals("test", insert.getValues(8).getStringValue());
    assertEquals("{\"key\": \"value\"}", insert.getValues(9).getStringValue());

    insert = mutation.getInsert().getValues(1);
    assertEquals("2", insert.getValues(0).getStringValue());
    for (int i = 1; i < insert.getValuesCount(); i++) {
      assertTrue(insert.getValues(i).hasNullValue());
    }
  }

  @Test
  public void testReadWriteTransaction() {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    for (long id : new Long[] {10L, 20L}) {
      mockSpanner.putStatementResult(
          StatementResult.update(
              Statement.newBuilder(sql)
                  .bind("p1")
                  .to(id)
                  .bind("p2")
                  .to(true)
                  .bind("p3")
                  .to(ByteArray.copyFrom("test_bytes"))
                  .bind("p4")
                  .to(3.14d)
                  .bind("p5")
                  .to(1L)
                  .bind("p6")
                  .to(com.google.cloud.spanner.Value.pgNumeric("6626e-3"))
                  .bind("p7")
                  .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                  .bind("p8")
                  .to(Date.parseDate("2022-04-02"))
                  .bind("p9")
                  .to("test_string")
                  .bind("p10")
                  .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                  .build(),
              1L));
    }

    String res = pgxTest.TestReadWriteTransaction(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx by default always uses prepared statements. That means that the first time a SQL
    // statement is executed, it will be sent two times to the backend:
    // 1. DescribeStatement
    // 2. Execute
    // The second time the same statement is executed, it is only sent once.

    assertEquals(5, requests.size());
    ExecuteSqlRequest describeSelect1Request = requests.get(0);
    // The first statement should begin the transaction.
    assertTrue(describeSelect1Request.getTransaction().hasBegin());
    assertEquals(QueryMode.PLAN, describeSelect1Request.getQueryMode());
    ExecuteSqlRequest executeSelect1Request = requests.get(1);
    // All following requests should use the transaction that was started.
    assertTrue(executeSelect1Request.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeSelect1Request.getQueryMode());

    ExecuteSqlRequest describeRequest = requests.get(2);
    assertEquals(sql, describeRequest.getSql());
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.getTransaction().hasId());

    for (int i = 3; i < 5; i++) {
      ExecuteSqlRequest executeRequest = requests.get(i);
      assertEquals(sql, executeRequest.getSql());
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertTrue(executeRequest.getTransaction().hasId());
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest commitRequest = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    // Verify that all execute-requests use the same transaction as the one that was committed.
    for (ExecuteSqlRequest request : requests) {
      if (request.getTransaction().hasId()) {
        assertEquals(request.getTransaction().getId(), commitRequest.getTransactionId());
      }
    }
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() {
    String res = pgxTest.TestReadOnlyTransaction(createConnString());

    assertNull(res);

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginTransactionRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginTransactionRequest.getOptions().hasReadOnly());
    List<ByteString> transactionsStarted = mockSpanner.getTransactionsStarted();
    assertFalse(transactionsStarted.isEmpty());
    ByteString transactionId = transactionsStarted.get(transactionsStarted.size() - 1);

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    for (ExecuteSqlRequest request : requests) {
      assertEquals(transactionId, request.getTransaction().getId());
    }
    // Read-only transactions are not really committed.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testReadWriteTransactionIsolationLevelSerializable() {
    String res = pgxTest.TestReadWriteTransactionIsolationLevelSerializable(createConnString());

    assertNull(res);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    ExecuteSqlRequest executeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);

    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());
    assertTrue(executeRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testReadWriteTransactionIsolationLevelRepeatableRead() {
    String res = pgxTest.TestReadWriteTransactionIsolationLevelRepeatableRead(createConnString());

    assertNull(res);

    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testReadOnlySerializableTransaction() {
    String res = pgxTest.TestReadOnlySerializableTransaction(createConnString());

    assertNull(res);

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginTransactionRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginTransactionRequest.getOptions().hasReadOnly());
    List<ByteString> transactionsStarted = mockSpanner.getTransactionsStarted();
    assertFalse(transactionsStarted.isEmpty());
    ByteString transactionId = transactionsStarted.get(transactionsStarted.size() - 1);

    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    for (ExecuteSqlRequest request : requests) {
      assertEquals(transactionId, request.getTransaction().getId());
    }
    // Read-only transactions are not really committed.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }
}
