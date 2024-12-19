// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(NodeJSTest.class)
@RunWith(Parameterized.class)
public class KnexMockServerTest extends AbstractMockServerTest {
  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("knex-tests");
  }

  private String getHost() {
    if (useDomainSocket) {
      return "/tmp";
    }
    return "localhost";
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "select * from (select 1)";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT1_RESULTSET));

    String output = runTest("testSelect1", getHost(), pgServer.getLocalPort());

    assertEquals("SELECT 1 returned: 1\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testSelectUser() throws Exception {
    String sql = setupSelectUserResult();

    String output = runTest("testSelectUser", getHost(), pgServer.getLocalPort());

    assertEquals("{ id: '1', name: 'User 1' }\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // Knex uses the extended protocol and describes the statement first.
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testSelectAllTypes() throws Exception {
    String sql = "select * from \"all_types\" where \"col_bigint\" = $1 limit $2";
    ResultSet resultSet = createAllTypesResultSet("");

    ResultSet metadataResultSet =
        ResultSet.newBuilder()
            .setMetadata(
                resultSet
                    .getMetadata()
                    .toBuilder()
                    .setUndeclaredParameters(
                        createParameterTypesMetadata(
                                ImmutableList.of(TypeCode.INT64, TypeCode.INT64))
                            .getUndeclaredParameters())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), metadataResultSet));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).bind("p2").to(1L).build(), resultSet));

    String output = runTest("testSelectAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{\n"
            + "  col_bigint: '1',\n"
            + "  col_bool: true,\n"
            + "  col_bytea: <Buffer 74 65 73 74>,\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 3.14,\n"
            + "  col_int: '100',\n"
            + "  col_numeric: '6.626',\n"
            + "  col_timestamptz: 2022-02-16T13:18:02.123Z,\n"
            + "  col_date: '2022-03-29',\n"
            + "  col_varchar: 'test',\n"
            + "  col_jsonb: { key: 'value' },\n"
            + "  col_array_bigint: [ '1', null, '2' ],\n"
            + "  col_array_bool: [ true, null, false ],\n"
            + "  col_array_bytea: [ <Buffer 62 79 74 65 73 31>, null, <Buffer 62 79 74 65 73 32> ],\n"
            + "  col_array_float4: [ 3.14, null, -99.99 ],\n"
            + "  col_array_float8: [ 3.14, null, -99.99 ],\n"
            + "  col_array_int: [ '-100', null, '-200' ],\n"
            + "  col_array_numeric: [ 6.626, null, -3.14 ],\n"
            + "  col_array_timestamptz: [ 2022-02-16T16:18:02.123Z, null, 2000-01-01T00:00:00.000Z ],\n"
            + "  col_array_date: '{\"2023-02-20\",NULL,\"2000-01-01\"}',\n"
            + "  col_array_varchar: [ 'string1', null, 'string2' ],\n"
            + "  col_array_jsonb: [ { key: 'value1' }, null, { key: 'value2' } ]\n"
            + "}\n",
        output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // Knex uses the extended protocol and describes the statement first.
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInsertAllTypes() throws IOException, InterruptedException {
    String sql =
        "insert into \"all_types\" "
            + "(\"col_bigint\", \"col_bool\", \"col_bytea\", \"col_date\", \"col_float4\", \"col_float8\", \"col_int\", \"col_jsonb\", \"col_numeric\", \"col_timestamptz\", \"col_varchar\") "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
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
                            TypeCode.DATE,
                            TypeCode.FLOAT32,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.JSON,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.STRING)))
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("some random string".getBytes(StandardCharsets.UTF_8)))
                .bind("p4")
                .to(Date.parseDate("2024-06-10"))
                .bind("p5")
                .to(3.14f)
                .bind("p6")
                .to(3.14d)
                .bind("p7")
                .to(100L)
                .bind("p8")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                .bind("p9")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p10")
                .to(Timestamp.parseTimestamp("2022-07-22T18:15:42.011000000Z"))
                .bind("p11")
                .to("some random string")
                .build(),
            1L));

    String output = runTest("testInsertAllTypes", getHost(), pgServer.getLocalPort());

    assertTrue(output, output.contains("rowCount: 1"));
  }

  @Test
  public void testReadWriteTransaction() throws IOException, InterruptedException {
    String sql = setupSelectUserResult();
    String insertSql = "insert into \"users\" (\"id\", \"value\") values ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertSql).bind("p1").to(1L).bind("p2").to("One").build(), 1L));

    String output = runTest("testReadWriteTransaction", getHost(), pgServer.getLocalPort());

    assertTrue(output, output.contains("{ id: '1', name: 'User 1' }"));
    assertTrue(output, output.contains("rowCount: 1,"));

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, selectRequests.size());
    ExecuteSqlRequest analyzeSelectRequest = selectRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeSelectRequest.getQueryMode());
    assertTrue(analyzeSelectRequest.hasTransaction());
    assertTrue(analyzeSelectRequest.getTransaction().hasBegin());
    assertTrue(analyzeSelectRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeSelectRequest = selectRequests.get(1);
    assertTrue(executeSelectRequest.hasTransaction());
    assertTrue(executeSelectRequest.getTransaction().hasId());

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(2, insertRequests.size());
    ExecuteSqlRequest analyzeInsertRequest = insertRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeInsertRequest.getQueryMode());
    assertTrue(analyzeInsertRequest.hasTransaction());
    assertTrue(analyzeInsertRequest.getTransaction().hasId());

    ExecuteSqlRequest executeInsertRequest = insertRequests.get(1);
    assertTrue(executeInsertRequest.hasTransaction());
    assertTrue(executeInsertRequest.getTransaction().hasId());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testReadWriteTransactionError() throws IOException, InterruptedException {
    String sql = setupSelectUserResult();
    String insertSql = "insert into \"users\" (\"id\", \"value\") values ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(insertSql).bind("p1").to(1L).bind("p2").to("One").build(),
            Status.ALREADY_EXISTS.asRuntimeException()));

    String output = runTest("testReadWriteTransactionError", getHost(), pgServer.getLocalPort());

    assertEquals(
        "Transaction error: error: insert into \"users\" (\"id\", \"value\") values ($1, $2) - "
            + "com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS\n"
            + "{ id: '1', name: 'User 1' }\n",
        output);

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(2, insertRequests.size());
    ExecuteSqlRequest analyzeInsertRequest = insertRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeInsertRequest.getQueryMode());
    assertTrue(analyzeInsertRequest.hasTransaction());
    assertTrue(analyzeInsertRequest.getTransaction().hasBegin());
    assertTrue(analyzeInsertRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeInsertRequest = insertRequests.get(1);
    assertTrue(executeInsertRequest.hasTransaction());
    assertTrue(executeInsertRequest.getTransaction().hasId());

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, selectRequests.size());
    ExecuteSqlRequest analyzeSelectRequest = selectRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeSelectRequest.getQueryMode());
    assertTrue(analyzeSelectRequest.hasTransaction());
    assertTrue(analyzeSelectRequest.getTransaction().hasSingleUse());
    assertTrue(analyzeSelectRequest.getTransaction().getSingleUse().hasReadOnly());

    ExecuteSqlRequest executeSelectRequest = selectRequests.get(1);
    assertTrue(executeSelectRequest.hasTransaction());
    assertTrue(executeSelectRequest.getTransaction().hasSingleUse());
    assertTrue(executeSelectRequest.getTransaction().getSingleUse().hasReadOnly());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() throws IOException, InterruptedException {
    String sql = setupSelectUserResult();

    String output = runTest("testReadOnlyTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("{ id: '1', name: 'User 1' }\n" + "{ id: '1', name: 'User 1' }\n", output);

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginRequest.getOptions().hasReadOnly());

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(3, selectRequests.size());
    ExecuteSqlRequest analyzeSelectRequest = selectRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeSelectRequest.getQueryMode());
    for (ExecuteSqlRequest request : selectRequests) {
      assertTrue(request.getTransaction().hasId());
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  static String setupSelectUserResult() {
    String sql = "select * from \"users\" where \"id\" = $1 limit $2";
    ResultSetMetadata metadata =
        createMetadata(
                ImmutableList.of(TypeCode.INT64, TypeCode.STRING), ImmutableList.of("id", "name"))
            .toBuilder()
            .setUndeclaredParameters(
                createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.INT64))
                    .getUndeclaredParameters())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ResultSet.newBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).bind("p2").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("User 1"))
                        .build())
                .build()));
    return sql;
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("knex-tests", testName, host, port, "db");
  }
}
