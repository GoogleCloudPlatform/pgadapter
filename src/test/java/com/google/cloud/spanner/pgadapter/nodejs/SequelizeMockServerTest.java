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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import com.google.spanner.v1.Type;
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
import org.junit.runners.JUnit4;

@Category(NodeJSTest.class)
@RunWith(JUnit4.class)
public class SequelizeMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("sequelize-tests");

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "with pg_range as (\n"
                    + "select * from (select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, 0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
                    + ") range where false),\n"
                    + "pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + "),\n"
                    + PgCatalog.PgType.PG_TYPE_CTE
                    + ",\n"
                    + " ranges AS (  SELECT pg_range.rngtypid, pg_type.typname AS rngtypname,         pg_type.typarray AS rngtyparray, pg_range.rngsubtype    FROM pg_range LEFT OUTER JOIN pg_type ON pg_type.oid = pg_range.rngtypid)SELECT pg_type.typname, pg_type.typtype, pg_type.oid, pg_type.typarray,       ranges.rngtypname, ranges.rngtypid, ranges.rngtyparray  FROM pg_type LEFT OUTER JOIN ranges ON pg_type.oid = ranges.rngsubtype WHERE (pg_type.typtype IN('b', 'e'))"),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.BOOL)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("SELECT 1+1 AS result"), SELECT2_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("SELECT * FROM users"),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64, TypeCode.STRING),
                        ImmutableList.of("id", "name")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));
  }

  private String getHost() {
    return "localhost";
  }

  @Test
  public void testSelectUsers() throws Exception {
    String sql = "SELECT * FROM users";

    String output = runTest("testSelectUsers", getHost(), pgServer.getLocalPort());

    assertEquals("Users: 1,Alice\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    assertFalse(request.getTransaction().getSingleUse().getReadOnly().hasStrong());
    assertTrue(request.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
    assertEquals(
        request.getTransaction().getSingleUse().getReadOnly().getMaxStaleness(),
        Duration.newBuilder().setSeconds(15L).build());
  }

  @Test
  public void testSelectUsersInTransaction() throws Exception {
    String sql = "SELECT * FROM users";

    String output = runTest("testSelectUsersInTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("Users: 1,Alice\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testErrorInTransaction() throws Exception {
    String sql = "SELECT * FROM non_existing_table";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.NOT_FOUND.withDescription("Table not found").asRuntimeException()));

    String output = runTest("testErrorInTransaction", getHost(), pgServer.getLocalPort());

    assertEquals(
        "Users: 1,Alice\n"
            + "Transaction error: SequelizeDatabaseError: Table not found - Statement: 'SELECT * FROM non_existing_table'\n",
        output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasId());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testSelectAllTypes() throws Exception {
    String sql =
        "SELECT \"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float4\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\", \"col_jsonb\" "
            + "FROM \"all_types\" AS \"all_types\" "
            + "WHERE col_bigint = $1;";
    ResultSet resultSet = createAllTypesResultSet("");
    ResultSet metadataResultSet =
        ResultSet.newBuilder()
            .setMetadata(
                resultSet.getMetadata().toBuilder()
                    .setUndeclaredParameters(
                        createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64))
                            .getUndeclaredParameters())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), metadataResultSet));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.newBuilder(sql).bind("p1").to(1L).build(), resultSet));

    String output = runTest("testSelectAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "[\n"
            + "  {\n"
            + "    \"col_bigint\": \"1\",\n"
            + "    \"col_bool\": true,\n"
            + "    \"col_bytea\": {\n"
            + "      \"type\": \"Buffer\",\n"
            + "      \"data\": [\n"
            + "        116,\n"
            + "        101,\n"
            + "        115,\n"
            + "        116\n"
            + "      ]\n"
            + "    },\n"
            + "    \"col_float4\": 3.14,\n"
            + "    \"col_float8\": 3.14,\n"
            + "    \"col_int\": \"100\",\n"
            + "    \"col_numeric\": \"6.626\",\n"
            + "    \"col_timestamptz\": \"2022-02-16T13:18:02.123Z\",\n"
            + "    \"col_date\": \"2022-03-29\",\n"
            + "    \"col_varchar\": \"testÄ\",\n"
            + "    \"col_jsonb\": {\n"
            + "      \"key\": \"value\"\n"
            + "    }\n"
            + "  }\n"
            + "]\n",
        output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest analyzeRequest = executeSqlRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeRequest.getQueryMode());
    assertTrue(analyzeRequest.getTransaction().hasSingleUse());
    assertTrue(analyzeRequest.getTransaction().getSingleUse().hasReadOnly());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInsertAllTypes() throws Exception {
    String sql =
        "INSERT INTO \"all_types\" (\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float4\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\",\"col_jsonb\") "
            + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) "
            + "RETURNING \"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float4\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\",\"col_jsonb\";";
    ResultSet resultSet = createAllTypesResultSet("");
    ResultSet metadataResultSet =
        ResultSet.newBuilder()
            .setMetadata(
                resultSet.getMetadata().toBuilder()
                    .setUndeclaredParameters(
                        createParameterTypesMetadata(
                                ImmutableList.of(
                                    TypeCode.INT64,
                                    TypeCode.BOOL,
                                    TypeCode.BYTES,
                                    TypeCode.FLOAT32,
                                    TypeCode.FLOAT64,
                                    TypeCode.INT64,
                                    TypeCode.NUMERIC,
                                    TypeCode.TIMESTAMP,
                                    TypeCode.DATE,
                                    TypeCode.STRING,
                                    TypeCode.JSON))
                            .getUndeclaredParameters())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), metadataResultSet));
    for (int i = 0; i < 2; i++) {
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(sql)
                  .bind("p1")
                  .to(1L + i)
                  .bind("p2")
                  .to(true)
                  .bind("p3")
                  .to(ByteArray.copyFrom("some random string".getBytes(StandardCharsets.UTF_8)))
                  .bind("p4")
                  .to(3.14f)
                  .bind("p5")
                  .to(3.14d)
                  .bind("p6")
                  .to(100L)
                  .bind("p7")
                  .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                  .bind("p8")
                  .to(Timestamp.parseTimestamp("2022-07-22T18:15:42.011000000Z"))
                  .bind("p9")
                  .to(Date.parseDate("2024-06-10"))
                  .bind("p10")
                  .to("some random string")
                  .bind("p11")
                  .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                  .build(),
              resultSet.toBuilder()
                  .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                  .build()));
    }
    String output = runTest("testInsertAllTypes", getHost(), pgServer.getLocalPort());

    // Because the returned result set is the same for both rows, both rows seem to be assigned the
    // ID 1.
    assertEquals("Inserted row with id 1\n" + "Inserted row with id 1\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // PGAdapter does an auto-describe of this statement once. The result of that is cached.
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest analyzeRequest = executeSqlRequests.get(0);
    assertEquals(QueryMode.PLAN, analyzeRequest.getQueryMode());
    assertTrue(analyzeRequest.getTransaction().hasBegin());
    assertTrue(analyzeRequest.getTransaction().getBegin().hasReadWrite());

    for (int i = 1; i < executeSqlRequests.size(); i++) {
      ExecuteSqlRequest executeRequest = executeSqlRequests.get(i);
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertTrue(executeRequest.getTransaction().hasBegin());
      assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    }

    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testUnmanagedReadWriteTransaction() throws IOException, InterruptedException {
    testTransaction("testUnmanagedReadWriteTransaction", IsolationLevel.SERIALIZABLE);
  }

  @Test
  public void testManagedReadWriteTransaction() throws IOException, InterruptedException {
    testTransaction("testManagedReadWriteTransaction", IsolationLevel.SERIALIZABLE);
  }

  @Test
  public void testManagedReadWriteTransactionWithIsolationLevel()
      throws IOException, InterruptedException {
    testTransaction(
        "testManagedReadWriteTransactionWithIsolationLevel", IsolationLevel.REPEATABLE_READ);
  }

  private void testTransaction(String methodName, IsolationLevel expectedIsolationLevel)
      throws IOException, InterruptedException {
    String selectSql =
        "SELECT \"id\", \"name\", \"createdAt\", \"updatedAt\" FROM \"users\" AS \"users\" WHERE \"users\".\"id\" = $1 LIMIT 1;";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("id")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("name")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("createdAt")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("updatedAt")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectSql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql).bind("p1").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2024-06-11T10:00:00Z").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2024-06-11T10:00:00Z").build())
                        .build())
                .build()));

    String insertSql =
        "INSERT INTO \"users\" (\"id\",\"name\",\"createdAt\",\"updatedAt\") VALUES (DEFAULT,$1,$2,$3) RETURNING \"id\",\"name\",\"createdAt\",\"updatedAt\";";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.STRING, TypeCode.TIMESTAMP, TypeCode.TIMESTAMP))
                                .getUndeclaredParameters())
                        .build())
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.STRING, TypeCode.TIMESTAMP, TypeCode.TIMESTAMP))
                                .getUndeclaredParameters())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("Test").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2024-06-11T10:00:00Z").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2024-06-11T10:00:00Z").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String output = runTest(methodName, getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  \"id\": \"1\",\n"
            + "  \"name\": \"One\",\n"
            + "  \"createdAt\": \"2024-06-11T10:00:00.000Z\",\n"
            + "  \"updatedAt\": \"2024-06-11T10:00:00.000Z\"\n"
            + "}\n"
            + "{\n"
            + "  \"id\": \"2\",\n"
            + "  \"name\": \"Test\",\n"
            + "  \"updatedAt\": \"2024-06-11T10:00:00.000Z\",\n"
            + "  \"createdAt\": \"2024-06-11T10:00:00.000Z\"\n"
            + "}\n",
        output);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(selectSql))
            .collect(Collectors.toList());
    assertEquals(2, selectRequests.size());
    assertEquals(QueryMode.PLAN, selectRequests.get(0).getQueryMode());
    assertTrue(selectRequests.get(0).getTransaction().hasBegin());
    assertTrue(selectRequests.get(0).getTransaction().getBegin().hasReadWrite());
    assertEquals(
        expectedIsolationLevel,
        selectRequests.get(0).getTransaction().getBegin().getIsolationLevel());

    assertEquals(QueryMode.NORMAL, selectRequests.get(1).getQueryMode());
    assertTrue(selectRequests.get(1).getTransaction().hasId());

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    for (ExecuteSqlRequest request : insertRequests) {
      assertTrue(request.getTransaction().hasId());
    }
  }

  @Test
  public void testContinueAfterFailedTransaction() throws IOException, InterruptedException {
    String insertSql =
        "INSERT INTO \"users\" (\"id\",\"name\",\"createdAt\",\"updatedAt\") VALUES (DEFAULT,$1,$2,$3) RETURNING \"id\",\"name\",\"createdAt\",\"updatedAt\";";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(insertSql), Status.INVALID_ARGUMENT.asRuntimeException()));

    String selectSql =
        "SELECT \"id\", \"name\", \"createdAt\", \"updatedAt\" FROM \"users\" AS \"users\" WHERE \"users\".\"id\" = $1 LIMIT 1;";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("id")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("name")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("createdAt")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("updatedAt")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectSql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql).bind("p1").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2024-06-11T10:00:00Z").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2024-06-11T10:00:00Z").build())
                        .build())
                .build()));

    String output =
        runTest("testContinueAfterFailedTransaction", getHost(), pgServer.getLocalPort());
    assertEquals(
        "Transaction error: SequelizeDatabaseError: io.grpc.StatusRuntimeException: INVALID_ARGUMENT - Statement: 'INSERT INTO \"users\" (\"id\",\"name\",\"createdAt\",\"updatedAt\") VALUES (DEFAULT,$1,$2,$3) RETURNING \"id\",\"name\",\"createdAt\",\"updatedAt\";'\n"
            + "{\n"
            + "  \"id\": \"1\",\n"
            + "  \"name\": \"One\",\n"
            + "  \"createdAt\": \"2024-06-11T10:00:00.000Z\",\n"
            + "  \"updatedAt\": \"2024-06-11T10:00:00.000Z\"\n"
            + "}\n",
        output);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("sequelize-tests", testName, host, port, "db");
  }
}
