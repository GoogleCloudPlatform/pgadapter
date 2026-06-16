// Copyright 2026 Google LLC
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
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
public class DrizzleMockServerTest extends AbstractMockServerTest {
  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("drizzle-tests");
  }

  private String getHost() {
    if (useDomainSocket) {
      return "/tmp";
    }
    return "localhost";
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

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
  public void testInsert() throws Exception {
    String sql = "insert into \"users\" (\"name\") values ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));

    String output = runTest("testInsert", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());

    // 1 for Describe Statement, 1 for Execute Statement
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testUpdate() throws Exception {
    String sql =
        "update \"alltypes\" set \"col_varchar\" = $1 where \"alltypes\".\"col_bigint\" = $2";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to("bar").bind("p2").to(1L).build(), 1L));

    String output = runTest("testUpdate", getHost(), pgServer.getLocalPort());

    assertEquals("Updated 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());

    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(2, executeRequest.getParamTypesCount());
  }

  @Test
  public void testDelete() throws Exception {
    String sql = "delete from \"users\" where \"users\".\"name\" = $1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("bar").build(), 1L));

    String output = runTest("testDelete", getHost(), pgServer.getLocalPort());

    assertEquals("Deleted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());

    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(1, executeRequest.getParamTypesCount());
  }

  @Test
  public void testInsertTwice() throws Exception {
    String sql = "insert into \"users\" (\"name\") values ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("bar").build(), 1L));

    String output = runTest("testInsertTwice", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\nInserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());

    // Describe, and then two executions
    assertEquals(3, executeSqlRequests.size());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAutoCommit() throws Exception {
    String sql = "insert into \"users\" (\"name\") values ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));

    String output = runTest("testInsertAutoCommit", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());

    assertEquals(2, executeSqlRequests.size());
    // Auto-commit mode triggers a separate commit for the insert (describe + commit, insert +
    // commit)
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAllTypes() throws Exception {
    String sql =
        "insert into \"alltypes\" "
            + "(\"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float4\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\", \"col_jsonb\") "
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
                            TypeCode.FLOAT64,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    StatementResult updateResult =
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("some random string"))
                .bind("p4")
                .to(3.14d) // doublePrecision
                .bind("p5")
                .to(3.14d)
                .bind("p6")
                .to(100L)
                .bind("p7")
                .to(Value.pgNumeric("234.54235"))
                .bind("p8")
                .to(Timestamp.parseTimestamp("2022-07-22T20:15:42.011+02:00"))
                .bind("p9")
                .to(Date.parseDate("2022-07-22"))
                .bind("p10")
                .to("some-random-string")
                .bind("p11")
                .to(Value.pgJsonb("{\"my_key\":\"my-value\"}"))
                .build(),
            1L);
    mockSpanner.putStatementResult(updateResult);

    String output = runTest("testInsertAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);
  }

  @Test
  public void testSelectAllTypes() throws Exception {
    String sql =
        "select \"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float4\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\", \"col_jsonb\" from \"alltypes\"";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createDrizzleAllTypesResultSet()));

    String output = runTest("testSelectAllTypes", getHost(), pgServer.getLocalPort());

    // We expect the selected row to match the JSON structure printed in our index.ts:
    assertTrue(output.contains("Selected {"));
    assertTrue(output.contains("\"col_bigint\":\"1\""));
    assertTrue(output.contains("\"col_bool\":true"));
  }

  @Test
  public void testSelectRelationalQueries() throws Exception {
    String usersSql = "select \"name\" from \"users\"";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(usersSql),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    StructType.Field.newBuilder()
                                        .setName("name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            com.google.protobuf.Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            com.google.protobuf.Value.newBuilder().setStringValue("Bob").build())
                        .build())
                .build()));

    String relationalSql =
        "select \"users\".\"name\", \"users_posts\".\"data\" as \"posts\" from \"users\" \"users\" left join lateral (select coalesce(json_agg(json_build_array(\"users_posts\".\"id\", \"users_posts\".\"title\", \"users_posts\".\"user_name\")), '[]'::json) as \"data\" from \"posts\" \"users_posts\" where \"users_posts\".\"user_name\" = \"users\".\"name\") \"users_posts\" on true";

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(relationalSql),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    StructType.Field.newBuilder()
                                        .setName("name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    StructType.Field.newBuilder()
                                        .setName("posts")
                                        .setType(
                                            Type.newBuilder()
                                                .setCode(TypeCode.JSON)
                                                .setTypeAnnotation(TypeAnnotationCode.PG_JSONB)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            com.google.protobuf.Value.newBuilder().setStringValue("Alice").build())
                        .addValues(
                            com.google.protobuf.Value.newBuilder()
                                .setStringValue(
                                    "[[1,\"First Post\",\"Alice\"],[2,\"Second Post\",\"Alice\"]]")
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            com.google.protobuf.Value.newBuilder().setStringValue("Bob").build())
                        .addValues(
                            com.google.protobuf.Value.newBuilder().setStringValue("[]").build())
                        .build())
                .build()));

    try {
      String output = runTest("testSelectRelationalQueries", getHost(), pgServer.getLocalPort());
      assertEquals(
          "Relational query returned: [{\"name\":\"Alice\",\"posts\":[{\"id\":1,\"title\":\"First Post\",\"user_name\":\"Alice\"},{\"id\":2,\"title\":\"Second Post\",\"user_name\":\"Alice\"}]},{\"name\":\"Bob\",\"posts\":[]}]\n",
          output);
    } catch (Throwable t) {
      System.err.println("MOCK SPANNER RECEIVED REQUESTS:");
      for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
        System.err.println("REQUEST SQL: " + request.getSql());
      }
      throw t;
    }
  }

  @Test
  public void testBatchDml() throws Exception {
    String sql = "insert into \"users\" (\"name\") values ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("batch-foo").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("batch-bar").build(), 1L));

    String output = runTest("testBatchDml", getHost(), pgServer.getLocalPort());

    assertEquals("Executed Batch DML\n", output);

    List<ExecuteBatchDmlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(sql, request.getStatements(0).getSql());
    assertEquals(
        "batch-foo", request.getStatements(0).getParams().getFieldsOrThrow("p1").getStringValue());
    assertEquals(sql, request.getStatements(1).getSql());
    assertEquals(
        "batch-bar", request.getStatements(1).getParams().getFieldsOrThrow("p1").getStringValue());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String output = runTest("testReadOnlyTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("executed read-only transaction\n", output);
  }

  private static ResultSet createDrizzleAllTypesResultSet() {
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_bigint")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_bool")
                            .setType(Type.newBuilder().setCode(TypeCode.BOOL).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_bytea")
                            .setType(Type.newBuilder().setCode(TypeCode.BYTES).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_float4")
                            .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_float8")
                            .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_int")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_numeric")
                            .setType(
                                Type.newBuilder()
                                    .setCode(TypeCode.NUMERIC)
                                    .setTypeAnnotation(TypeAnnotationCode.PG_NUMERIC)
                                    .build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_timestamptz")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_date")
                            .setType(Type.newBuilder().setCode(TypeCode.DATE).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_varchar")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build()))
                    .addFields(
                        StructType.Field.newBuilder()
                            .setName("col_jsonb")
                            .setType(
                                Type.newBuilder()
                                    .setCode(TypeCode.JSON)
                                    .setTypeAnnotation(TypeAnnotationCode.PG_JSONB)
                                    .build()))
                    .build())
            .build();
    return ResultSet.newBuilder()
        .setMetadata(metadata)
        .addRows(
            ListValue.newBuilder()
                .addValues(com.google.protobuf.Value.newBuilder().setStringValue("1").build())
                .addValues(com.google.protobuf.Value.newBuilder().setBoolValue(true).build())
                .addValues(
                    com.google.protobuf.Value.newBuilder()
                        .setStringValue(
                            Base64.getEncoder()
                                .encodeToString("test".getBytes(StandardCharsets.UTF_8)))
                        .build())
                .addValues(com.google.protobuf.Value.newBuilder().setNumberValue(3.14d).build())
                .addValues(com.google.protobuf.Value.newBuilder().setNumberValue(3.14d).build())
                .addValues(com.google.protobuf.Value.newBuilder().setStringValue("100").build())
                .addValues(com.google.protobuf.Value.newBuilder().setStringValue("6.626").build())
                .addValues(
                    com.google.protobuf.Value.newBuilder()
                        .setStringValue("2022-02-16T13:18:02.123456Z")
                        .build())
                .addValues(
                    com.google.protobuf.Value.newBuilder().setStringValue("2022-03-29").build())
                .addValues(com.google.protobuf.Value.newBuilder().setStringValue("testÄ").build())
                .addValues(
                    com.google.protobuf.Value.newBuilder()
                        .setStringValue("{\"key\": \"value\"}")
                        .build())
                .build())
        .build();
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("drizzle-tests", testName, host, port, "db");
  }
}
