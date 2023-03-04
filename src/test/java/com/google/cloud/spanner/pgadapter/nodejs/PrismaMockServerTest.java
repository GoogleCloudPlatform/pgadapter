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
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
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
public class PrismaMockServerTest extends AbstractMockServerTest {
  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("prisma-tests");
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

    assertEquals("[ { C: 1n } ]\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
  }

  @Test
  public void testFindAllUsers() throws Exception {
    String sql =
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" "
            + "FROM \"public\".\"User\" WHERE 1=1 "
            + "ORDER BY \"public\".\"User\".\"id\" ASC "
            + "LIMIT $1 OFFSET $2";
    ResultSetMetadata metadata =
        createMetadata(
            ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
            ImmutableList.of("id", "email", "name"));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(TypeCode.INT64, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(10L).bind("p2").to(0L).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Peter").build())
                        .addValues(Value.newBuilder().setStringValue("peter@prisma.com").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.com").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .addValues(Value.newBuilder().setStringValue("Hannah").build())
                        .addValues(Value.newBuilder().setStringValue("hannah@prisma.com").build())
                        .build())
                .build()));

    String output = runTest("testFindAllUsers", getHost(), pgServer.getLocalPort());

    assertEquals(
        "[\n"
            + "  { id: '1', email: 'Peter', name: 'peter@prisma.com' },\n"
            + "  { id: '2', email: 'Alice', name: 'alice@prisma.com' },\n"
            + "  { id: '3', email: 'Hannah', name: 'hannah@prisma.com' }\n"
            + "]\n",
        output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testCreateUser() throws Exception {
    String sql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(createMetadata(ImmutableList.of(TypeCode.STRING)).getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("2373a81d-772c-4221-adf0-06965bc02c2c")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2373a81d-772c-4221-adf0-06965bc02c2c")
                                .build())
                        .build())
                .build()));

    String selectSql =
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" FROM \"public\".\"User\" WHERE \"public\".\"User\".\"id\" = $1 LIMIT $2 OFFSET $3";
    ResultSetMetadata selectMetadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.INT64, TypeCode.INT64))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectSql), ResultSet.newBuilder().setMetadata(selectMetadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql)
                .bind("p1")
                .to("2373a81d-772c-4221-adf0-06965bc02c2c")
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(selectMetadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2373a81d-772c-4221-adf0-06965bc02c2c")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));

    String output = runTest("testCreateUser", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{\n"
            + "  id: '2373a81d-772c-4221-adf0-06965bc02c2c',\n"
            + "  email: 'alice@prisma.io',\n"
            + "  name: 'Alice'\n"
            + "}\n",
        output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(selectSql))
            .collect(Collectors.toList());
    assertEquals(2, selectRequests.size());
    ExecuteSqlRequest planSelectRequest = selectRequests.get(0);
    assertTrue(planSelectRequest.getTransaction().hasId());
    assertEquals(QueryMode.PLAN, planSelectRequest.getQueryMode());
    ExecuteSqlRequest executeSelectRequest = selectRequests.get(1);
    assertTrue(executeSelectRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeSelectRequest.getQueryMode());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testCreateAllTypes() throws IOException, InterruptedException {
    String insertSql =
        "INSERT INTO \"public\".\"AllTypes\" (\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\",\"col_jsonb\") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING \"public\".\"AllTypes\".\"col_bigint\"";
    ResultSetMetadata insertMetadata =
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
            .toBuilder()
            .setRowType(createMetadata(ImmutableList.of(TypeCode.INT64)).getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(insertMetadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100)
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
                .setMetadata(insertMetadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String selectSql =
        "SELECT \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\" FROM \"public\".\"AllTypes\" WHERE \"public\".\"AllTypes\".\"col_bigint\" = $1 LIMIT $2 OFFSET $3";
    ResultSetMetadata allTypesMetadata = createAllTypesResultSetMetadata("");
    ResultSetMetadata allArrayTypesMetadata = createAllArrayTypesResultSetMetadata("");
    ResultSetMetadata selectMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addAllFields(allTypesMetadata.getRowType().getFieldsList())
                    .addAllFields(allArrayTypesMetadata.getRowType().getFieldsList())
                    .build())
            .build();
    ListValue row =
        createAllTypesResultSet("")
            .getRows(0)
            .toBuilder()
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectSql),
            ResultSet.newBuilder()
                .setMetadata(
                    selectMetadata
                        .toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.INT64, TypeCode.INT64, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder().setMetadata(selectMetadata).addRows(row).build()));

    String output = runTest("testCreateAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: true,\n"
            + "  col_bytea: <Buffer 74 65 73 74>,\n"
            + "  col_float8: 3.14,\n"
            + "  col_int: 100,\n"
            + "  col_numeric: 6.626,\n"
            + "  col_timestamptz: 2022-02-16T13:18:02.123Z,\n"
            + "  col_date: 2022-03-29T00:00:00.000Z,\n"
            + "  col_varchar: 'test',\n"
            + "  col_jsonb: { key: 'value' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("prisma-tests", testName, host, port, "db");
  }
}
