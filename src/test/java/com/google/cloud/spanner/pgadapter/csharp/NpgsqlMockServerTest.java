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

package com.google.cloud.spanner.pgadapter.csharp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.CopyInMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
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
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(DotnetTest.class)
@RunWith(Parameterized.class)
public class NpgsqlMockServerTest extends AbstractNpgsqlMockServerTest {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled()
        ? new Object[] {"localhost", "/tmp"}
        : new Object[] {"localhost"};
  }

  private String createConnectionString() {
    return String.format(
        "Host=%s;Port=%d;Database=d;SSL Mode=Disable;Timeout=60;Command Timeout=60",
        host, pgServer.getLocalPort());
  }

  @Test
  public void testShowServerVersion() throws IOException, InterruptedException {
    String result = execute("TestShowServerVersion", createConnectionString());
    assertEquals("14.1\n", result);
  }

  @Test
  public void testShowApplicationName() throws IOException, InterruptedException {
    String result = execute("TestShowApplicationName", createConnectionString());
    assertEquals("npgsql\n", result);
  }

  @Test
  public void testSelect1() throws IOException, InterruptedException {
    String result = execute("TestSelect1", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(SELECT1.getSql()))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testSelectArray() throws IOException, InterruptedException {
    String sql = "SELECT '{1,2}'::bigint[] as c";
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
                                        .setName("c")
                                        .setType(
                                            Type.newBuilder()
                                                .setCode(TypeCode.ARRAY)
                                                .setArrayElementType(
                                                    Type.newBuilder()
                                                        .setCode(TypeCode.INT64)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setListValue(
                                    ListValue.newBuilder()
                                        .addValues(Value.newBuilder().setStringValue("1").build())
                                        .addValues(Value.newBuilder().setStringValue("2").build())
                                        .build())
                                .build())
                        .build())
                .build()));

    String result = execute("TestSelectArray", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testQueryWithParameter() throws IOException, InterruptedException {
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

    String result = execute("TestQueryWithParameter", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(sql, request.getSql());
    assertNotNull(request.getParams());
    assertFalse(request.getParams().getFieldsMap().isEmpty());
    assertEquals("baz", request.getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testQueryAllDataTypes() throws IOException, InterruptedException {
    String sql = "SELECT * FROM all_types WHERE col_bigint=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String result = execute("TestQueryAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testUpdateAllDataTypes() throws IOException, InterruptedException {
    String sql =
        "UPDATE all_types SET col_bool=$1, col_bytea=$2, col_float8=$3, col_int=$4, col_numeric=$5, col_timestamptz=$6, col_date=$7, col_varchar=$8, col_jsonb=$9 WHERE col_varchar = $10";
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(true)
                .bind("p2")
                .to(ByteArray.copyFrom("test_bytes"))
                .bind("p3")
                .to(3.14d)
                .bind("p4")
                .to(1L)
                .bind("p5")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p6")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p7")
                .to(Date.parseDate("2022-04-02"))
                .bind("p8")
                .to("test_string")
                .bind("p9")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                .bind("p10")
                .to("test")
                .build(),
            1L));

    String result = execute("TestUpdateAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testInsertAllDataTypes() throws IOException, InterruptedException {
    String sql = getInsertAllTypesSql();
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
                .to(100)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-04-02"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                .build(),
            1L));

    String result = execute("TestInsertAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testInsertNullsAllDataTypes() throws IOException, InterruptedException {
    String sql = getInsertAllTypesSql();
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(100L)
                .bind("p2")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p3")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p4")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p5")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p6")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p7")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p8")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p9")
                .to((com.google.cloud.spanner.Value) null)
                .bind("p10")
                .to((com.google.cloud.spanner.Value) null)
                .build(),
            1L));

    String result = execute("TestInsertNullsAllDataTypes", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertNotNull(request.getParams());
    assertFalse(request.getParams().getFieldsMap().isEmpty());
    // The ID is typed (INT64)
    assertEquals(1, request.getParamTypesMap().size());
    assertEquals(TypeCode.INT64, request.getParamTypesMap().get("p1").getCode());
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testInsertAllDataTypesReturning() throws IOException, InterruptedException {
    String sql = getInsertAllTypesSql() + " returning *";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
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
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ALL_TYPES_METADATA.toBuilder()
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

    String result = execute("TestInsertAllDataTypesReturning", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testInsertBatch() throws IOException, InterruptedException {
    int batchSize = 10;
    for (int i = 0; i < batchSize; i++) {
      mockSpanner.putStatementResult(StatementResult.update(createBatchInsertStatement(i), 1L));
    }

    String result = execute("TestInsertBatch", createConnectionString());
    assertEquals("Success\n", result);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchDmlRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(batchSize, batchDmlRequest.getStatementsCount());
    assertTrue(batchDmlRequest.getTransaction().hasBegin());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testMixedBatch() throws IOException, InterruptedException {
    String insertSql = getInsertAllTypesSql();
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
            Statement.newBuilder(selectSql).bind("p1").to(true).build(), resultSet));

    String updateSql = "update all_types set col_bool=false where col_bool=$1";
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(updateSql).bind("p1").to(true).build(), 3L));

    int batchSize = 5;
    for (int i = 0; i < batchSize; i++) {
      mockSpanner.putStatementResult(StatementResult.update(createBatchInsertStatement(i), 1L));
    }

    String result = execute("TestMixedBatch", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // Reverse the requests list, so it's a little easier to check what we want to check. There are
    // couple of system queries before the actual queries that we want to ignore.
    requests = Lists.reverse(requests);
    // We should have 2 data queries.
    assertTrue(requests.size() >= 2);
    assertEquals(updateSql, requests.get(0).getSql());
    assertEquals(selectSql, requests.get(1).getSql());
    // The insert statements are executed as Batch DML.
    List<ExecuteBatchDmlRequest> batchRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchRequests.size());
    ExecuteBatchDmlRequest batchRequest = batchRequests.get(0);
    assertEquals(batchSize, batchRequest.getStatementsCount());
    int index = 0;
    for (ExecuteBatchDmlRequest.Statement statement : batchRequest.getStatementsList()) {
      assertEquals(insertSql, statement.getSql());
      assertEquals(
          String.valueOf(100 + index),
          statement.getParams().getFieldsMap().get("p1").getStringValue());
      index++;
    }
  }

  @Test
  public void testBatchExecutionError() throws IOException, InterruptedException {
    String insertSql = getInsertAllTypesSql();
    int batchSize = 3;
    for (int i = 0; i < batchSize; i++) {
      Statement statement = createBatchInsertStatement(i);
      if (i == 1) {
        mockSpanner.putStatementResult(
            StatementResult.exception(statement, Status.ALREADY_EXISTS.asRuntimeException()));
      } else {
        mockSpanner.putStatementResult(StatementResult.update(statement, 1L));
      }
    }

    String result = execute("TestBatchExecutionError", createConnectionString());
    assertTrue(result, result.contains("Executing batch failed with error"));
    assertTrue(result, result.contains("ALREADY_EXISTS"));

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
  public void testDdlBatch() throws IOException, InterruptedException {
    addDdlResponseToSpannerAdmin();

    String result = execute("TestDdlBatch", createConnectionString());
    assertEquals("Success\n", result);

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(request -> (UpdateDatabaseDdlRequest) request)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest request = requests.get(0);
    assertEquals(5, request.getStatementsCount());
  }

  @Test
  public void testDdlScript() throws IOException, InterruptedException {
    addDdlResponseToSpannerAdmin();

    String result = execute("TestDdlScript", createConnectionString());
    assertEquals("Success\n", result);

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(request -> (UpdateDatabaseDdlRequest) request)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest request = requests.get(0);
    assertEquals(5, request.getStatementsCount());
  }

  @Test
  public void testBinaryCopyIn() throws IOException, InterruptedException {
    testCopyIn("TestBinaryCopyIn");
  }

  @Test
  public void testTextCopyIn() throws IOException, InterruptedException {
    testCopyIn("TestTextCopyIn");
  }

  private void testCopyIn(String testMethod) throws IOException, InterruptedException {
    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute(testMethod, createConnectionString());
    assertEquals("Success\n", result);

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
    assertEquals("{\"key\":\"value\"}", insert.getValues(9).getStringValue());

    insert = mutation.getInsert().getValues(1);
    assertEquals("2", insert.getValues(0).getStringValue());
    for (int i = 1; i < insert.getValuesCount(); i++) {
      assertTrue(insert.getValues(i).hasNullValue());
    }
  }

  @Test
  public void testBinaryCopyOut() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types"),
            ALL_TYPES_RESULTSET.toBuilder()
                .addAllRows(ALL_TYPES_NULLS_RESULTSET.getRowsList())
                .build()));

    String result = execute("TestBinaryCopyOut", createConnectionString());
    assertEquals(
        "1\tTrue\tdGVzdA==\t3.14\t100\t6.626\t20220216T131802123456\t20220329\ttest\t{\"key\": \"value\"}\t[1, , 2]\t[True, , False]\t[Ynl0ZXMx, , Ynl0ZXMy]\t[3.14, , -99.99]\t[-100, , -200]\t[6.626, , -3.14]\t[20220216T161802123456, , 20000101T000000]\t[20230220, , 20000101]\t[string1, , string2]\t[{\"key\": \"value1\"}, , {\"key\": \"value2\"}]\n"
            + "NULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\n"
            + "Success\n",
        result);
  }

  @Test
  public void testTextCopyOut() throws IOException, InterruptedException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types"),
            ALL_TYPES_RESULTSET.toBuilder()
                .addAllRows(ALL_TYPES_NULLS_RESULTSET.getRowsList())
                .build()));

    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute("TestTextCopyOut", createConnectionString());
    assertEquals(
        "1\tt\t\\\\x74657374\t3.14\t100\t6.626\t2022-02-16 14:18:02.123456+01\t2022-03-29\ttest\t{\"key\": \"value\"}\t{1,NULL,2}\t{t,NULL,f}\t{\"\\\\\\\\x627974657331\",NULL,\"\\\\\\\\x627974657332\"}\t{3.14,NULL,-99.99}\t{-100,NULL,-200}\t{6.626,NULL,-3.14}\t{\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\t{\"2023-02-20\",NULL,\"2000-01-01\"}\t{\"string1\",NULL,\"string2\"}\t{\"{\\\\\"key\\\\\": \\\\\"value1\\\\\"}\",NULL,\"{\\\\\"key\\\\\": \\\\\"value2\\\\\"}\"}\n"
            + "\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n"
            + "Success\n",
        result);
  }

  @Test
  public void testSimplePrepare() throws IOException, InterruptedException {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));

    String result = execute("TestSimplePrepare", createConnectionString());
    assertEquals("Success\n", result);

    List<ParseMessage> parseMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof ParseMessage)
            .map(msg -> (ParseMessage) msg)
            .filter(msg -> msg.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, parseMessages.size());
    ParseMessage parseMessage = parseMessages.get(0);
    List<DescribeMessage> describeMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof DescribeMessage)
            .map(msg -> (DescribeMessage) msg)
            .filter(msg -> msg.getName().equals(parseMessage.getName()))
            .collect(Collectors.toList());
    assertEquals(1, describeMessages.size());
    DescribeMessage describeMessage = describeMessages.get(0);
    assertEquals(PreparedType.Statement, describeMessage.getType());
  }

  @Test
  public void testPrepareAndExecute() throws IOException, InterruptedException {
    String sql = getInsertAllTypesSql();
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
    for (int i = 0; i < 2; i++) {
      mockSpanner.putStatementResult(
          StatementResult.update(
              Statement.newBuilder(sql)
                  .bind("p1")
                  .to(100L + i)
                  .bind("p2")
                  .to(true)
                  .bind("p3")
                  .to(ByteArray.copyFrom("test_bytes"))
                  .bind("p4")
                  .to(3.14d)
                  .bind("p5")
                  .to(100)
                  .bind("p6")
                  .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                  .bind("p7")
                  .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                  .bind("p8")
                  .to(Date.parseDate("2022-04-02"))
                  .bind("p9")
                  .to("test_string")
                  .bind("p10")
                  .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                  .build(),
              1L));
    }

    String result = execute("TestPrepareAndExecute", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(3, requests.size());
    ExecuteSqlRequest prepareRequest = requests.get(0);
    assertEquals(QueryMode.PLAN, prepareRequest.getQueryMode());
    assertEquals(0, prepareRequest.getParamTypesCount());
    assertEquals(0, prepareRequest.getParams().getFieldsCount());
    for (int i = 1; i <= 2; i++) {
      ExecuteSqlRequest executeRequest = requests.get(i);
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertEquals(10, executeRequest.getParamTypesCount());
      assertEquals(10, executeRequest.getParams().getFieldsCount());
    }

    List<ParseMessage> parseMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof ParseMessage)
            .map(msg -> (ParseMessage) msg)
            .filter(msg -> msg.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, parseMessages.size());
    ParseMessage parseMessage = parseMessages.get(0);
    List<DescribeMessage> describeMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof DescribeMessage)
            .map(msg -> (DescribeMessage) msg)
            .filter(msg -> msg.getName().equals(parseMessage.getName()))
            .collect(Collectors.toList());
    assertEquals(1, describeMessages.size());
    DescribeMessage describeMessage = describeMessages.get(0);
    assertEquals(PreparedType.Statement, describeMessage.getType());
    List<BindMessage> bindMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof BindMessage)
            .map(msg -> (BindMessage) msg)
            .filter(msg -> msg.getStatementName().equals(parseMessage.getName()))
            .collect(Collectors.toList());
    assertEquals(2, bindMessages.size());
    BindMessage bindMessage = bindMessages.get(0);
    assertEquals(10, bindMessage.getParameters().length);
    List<ExecuteMessage> executeMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof ExecuteMessage)
            .map(msg -> (ExecuteMessage) msg)
            .filter(msg -> msg.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeMessages.size());
  }

  @Test
  public void testReadWriteTransaction() throws IOException, InterruptedException {
    String sql = getInsertAllTypesSql();
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
                  .to(100L)
                  .bind("p6")
                  .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                  .bind("p7")
                  .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                  .bind("p8")
                  .to(Date.parseDate("2022-04-02"))
                  .bind("p9")
                  .to("test_string")
                  .bind("p10")
                  .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                  .build(),
              1L));
    }

    String result = execute("TestReadWriteTransaction", createConnectionString());
    assertEquals("Row: 1\n" + "Success\n", result);

    List<ExecuteSqlRequest> select1Requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals("SELECT 1"))
            .collect(Collectors.toList());
    assertEquals(1, select1Requests.size());
    assertTrue(select1Requests.get(0).hasTransaction());
    assertTrue(select1Requests.get(0).getTransaction().hasBegin());
    assertTrue(select1Requests.get(0).getTransaction().getBegin().hasReadWrite());
    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(getInsertAllTypesSql()))
            .collect(Collectors.toList());
    assertEquals(2, insertRequests.size());
    for (ExecuteSqlRequest insertRequest : insertRequests) {
      assertTrue(insertRequest.hasTransaction());
      assertTrue(insertRequest.getTransaction().hasId());
    }
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() throws IOException, InterruptedException {
    String result = execute("TestReadOnlyTransaction", createConnectionString());
    assertEquals("Row: 1\n" + "Row: 2\n" + "Success\n", result);

    // There are two BeginTransaction requests on the server, because the initial metadata queries
    // that are executed by the npgsql driver will also use a read-only transaction.
    assertEquals(2, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginTransactionRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(1);
    assertTrue(beginTransactionRequest.getOptions().hasReadOnly());
    List<ByteString> transactionsStarted = mockSpanner.getTransactionsStarted();
    assertFalse(transactionsStarted.isEmpty());
    ByteString transactionId = transactionsStarted.get(transactionsStarted.size() - 1);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(
                request ->
                    request.getSql().equals("SELECT 1") || request.getSql().equals("SELECT 2"))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());
    for (ExecuteSqlRequest request : requests) {
      assertEquals(transactionId, request.getTransaction().getId());
    }
    // Read-only transactions are not really committed or rolled back.s
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  private static String getInsertAllTypesSql() {
    return "INSERT INTO all_types "
        + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
        + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
  }

  private static Statement createBatchInsertStatement(int index) {
    return Statement.newBuilder(getInsertAllTypesSql())
        .bind("p1")
        .to(100L + index)
        .bind("p2")
        .to(index % 2 == 0)
        .bind("p3")
        .to(ByteArray.copyFrom(index + "test_bytes"))
        .bind("p4")
        .to(3.14d + index)
        .bind("p5")
        .to(index)
        .bind("p6")
        .to(com.google.cloud.spanner.Value.pgNumeric(index + ".123"))
        .bind("p7")
        .to(Timestamp.parseTimestamp(String.format("2022-03-24T%02d:39:10.123456000Z", index)))
        .bind("p8")
        .to(Date.parseDate(String.format("2022-04-%02d", index + 1)))
        .bind("p9")
        .to("test_string" + index)
        .bind("p10")
        .to(com.google.cloud.spanner.Value.pgJsonb(String.format("{\"key\":\"value%d\"}", index)))
        .build();
  }
}
