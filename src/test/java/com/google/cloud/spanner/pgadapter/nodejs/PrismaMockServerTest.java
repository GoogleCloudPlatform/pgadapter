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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

  @Test
  public void testUpdateAllTypes() throws IOException, InterruptedException {
    String selectIdSql =
        "SELECT \"public\".\"AllTypes\".\"col_bigint\" FROM \"public\".\"AllTypes\" WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $1 AND 1=1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectIdSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64),
                        false,
                        ImmutableList.of("col_bigint"),
                        ImmutableList.of(TypeCode.INT64)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectIdSql).bind("p1").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64), ImmutableList.of("col_bigint")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .build()));

    String updateSql =
        "UPDATE \"public\".\"AllTypes\" SET \"col_bool\" = $1, \"col_bytea\" = $2, \"col_float8\" = $3, \"col_int\" = $4, \"col_numeric\" = $5, \"col_timestamptz\" = $6, \"col_date\" = $7, \"col_varchar\" = $8, \"col_jsonb\" = $9 WHERE (\"public\".\"AllTypes\".\"col_bigint\" IN ($10) AND (\"public\".\"AllTypes\".\"col_bigint\" = $11 AND 1=1))";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON,
                            TypeCode.INT64,
                            TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(updateSql)
                .bind("p1")
                .to(false)
                .bind("p2")
                .to(ByteArray.copyFrom("updated"))
                .bind("p3")
                .to(6.626d)
                .bind("p4")
                .to(-100)
                .bind("p5")
                .to(com.google.cloud.spanner.Value.pgNumeric("3.14"))
                .bind("p6")
                .to(Timestamp.parseTimestamp("2023-03-13T05:40:02.123456000Z"))
                .bind("p7")
                .to(Date.parseDate("2023-03-13"))
                .bind("p8")
                .to("updated")
                .bind("p9")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"updated\"}"))
                .bind("p10")
                .to(1L)
                .bind("p11")
                .to(1L)
                .build(),
            1L));

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
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setBoolValue(false).build())
            .addValues(
                Value.newBuilder()
                    .setStringValue(
                        Base64.getEncoder()
                            .encodeToString("updated".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .addValues(Value.newBuilder().setNumberValue(6.626d).build())
            .addValues(Value.newBuilder().setStringValue("-100").build())
            .addValues(Value.newBuilder().setStringValue("3.14").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13T05:40:02.123456000Z").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13").build())
            .addValues(Value.newBuilder().setStringValue("updated").build())
            .addValues(Value.newBuilder().setStringValue("{\"key\":\"updated\"}").build())
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

    String output = runTest("testUpdateAllTypes", getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: <Buffer 75 70 64 61 74 65 64>,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
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

    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(selectIdSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(selectIdSql, requests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());
    assertEquals(updateSql, requests.get(2).getSql());
    assertEquals(QueryMode.PLAN, requests.get(2).getQueryMode());
    assertEquals(updateSql, requests.get(3).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(3).getQueryMode());
    assertEquals(selectSql, requests.get(4).getSql());
    assertEquals(QueryMode.PLAN, requests.get(4).getQueryMode());
    assertEquals(selectSql, requests.get(5).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(5).getQueryMode());
  }

  @Test
  public void testUpsertAllTypes() throws IOException, InterruptedException {
    String upsertSql =
        "INSERT INTO \"public\".\"AllTypes\" (\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_date\",\"col_varchar\",\"col_jsonb\") "
            + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (\"col_bigint\") "
            + "DO UPDATE SET \"col_bool\" = $11, \"col_bytea\" = $12, \"col_float8\" = $13, \"col_int\" = $14, \"col_numeric\" = $15, \"col_timestamptz\" = $16, \"col_date\" = $17, \"col_varchar\" = $18, \"col_jsonb\" = $19 "
            + "WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $20 AND 1=1) "
            + "RETURNING \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\"";
    ResultSetMetadata metadata =
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
                TypeCode.JSON),
            true,
            ImmutableList.of(
                "col_bigint",
                "col_bool",
                "col_bytea",
                "col_float8",
                "col_int",
                "col_numeric",
                "col_timestamptz",
                "col_date",
                "col_varchar",
                "col_jsonb",
                "col_array_bigint",
                "col_array_bool",
                "col_array_bytea",
                "col_array_float8",
                "col_array_int",
                "col_array_numeric",
                "col_array_timestamptz",
                "col_array_date",
                "col_array_varchar",
                "col_array_jsonb"),
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
                TypeCode.BOOL,
                TypeCode.BYTES,
                TypeCode.FLOAT64,
                TypeCode.INT64,
                TypeCode.NUMERIC,
                TypeCode.TIMESTAMP,
                TypeCode.DATE,
                TypeCode.STRING,
                TypeCode.JSON,
                TypeCode.INT64));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(upsertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));

    ListValue row =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setBoolValue(false).build())
            .addValues(
                Value.newBuilder()
                    .setStringValue(
                        Base64.getEncoder()
                            .encodeToString("updated".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .addValues(Value.newBuilder().setNumberValue(6.626d).build())
            .addValues(Value.newBuilder().setStringValue("-100").build())
            .addValues(Value.newBuilder().setStringValue("3.14").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13T05:40:02.123456000Z").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13").build())
            .addValues(Value.newBuilder().setStringValue("updated").build())
            .addValues(Value.newBuilder().setStringValue("{\"key\":\"updated\"}").build())
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
            Statement.newBuilder(upsertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(false)
                .bind("p3")
                .to(ByteArray.copyFrom("updated"))
                .bind("p4")
                .to(6.626d)
                .bind("p5")
                .to(-100)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("3.14"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2023-03-13T05:40:02.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2023-03-13"))
                .bind("p9")
                .to("updated")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"updated\"}"))
                .bind("p11")
                .to(false)
                .bind("p12")
                .to(ByteArray.copyFrom("updated"))
                .bind("p13")
                .to(6.626d)
                .bind("p14")
                .to(-100)
                .bind("p15")
                .to(com.google.cloud.spanner.Value.pgNumeric("3.14"))
                .bind("p16")
                .to(Timestamp.parseTimestamp("2023-03-13T05:40:02.123456000Z"))
                .bind("p17")
                .to(Date.parseDate("2023-03-13"))
                .bind("p18")
                .to("updated")
                .bind("p19")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"updated\"}"))
                .bind("p20")
                .to(1L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(row)
                .build()));

    String output = runTest("testUpsertAllTypes", getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: <Buffer 75 70 64 61 74 65 64>,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
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

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(upsertSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(upsertSql, requests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());
  }

  @Test
  public void testDeleteAllTypes() throws IOException, InterruptedException {
    String selectIdSql =
        "SELECT \"public\".\"AllTypes\".\"col_bigint\" FROM \"public\".\"AllTypes\" WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $1 AND 1=1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectIdSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64),
                        false,
                        ImmutableList.of("col_bigint"),
                        ImmutableList.of(TypeCode.INT64)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectIdSql).bind("p1").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(TypeCode.INT64), ImmutableList.of("col_bigint")))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .build()));

    String deleteSql =
        "DELETE FROM \"public\".\"AllTypes\" WHERE (\"public\".\"AllTypes\".\"col_bigint\" IN ($1) AND (\"public\".\"AllTypes\".\"col_bigint\" = $2 AND 1=1))";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(deleteSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(deleteSql).bind("p1").to(1L).bind("p2").to(1L).build(), 1L));

    String selectSql =
        "SELECT \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\" FROM \"public\".\"AllTypes\" WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $1 AND 1=1) LIMIT $2 OFFSET $3";
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
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setBoolValue(false).build())
            .addValues(
                Value.newBuilder()
                    .setStringValue(
                        Base64.getEncoder()
                            .encodeToString("updated".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .addValues(Value.newBuilder().setNumberValue(6.626d).build())
            .addValues(Value.newBuilder().setStringValue("-100").build())
            .addValues(Value.newBuilder().setStringValue("3.14").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13T05:40:02.123456000Z").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13").build())
            .addValues(Value.newBuilder().setStringValue("updated").build())
            .addValues(Value.newBuilder().setStringValue("{\"key\":\"updated\"}").build())
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

    String output = runTest("testDeleteAllTypes", getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: <Buffer 75 70 64 61 74 65 64>,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
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

    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(selectSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(selectSql, requests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());
    assertEquals(selectIdSql, requests.get(2).getSql());
    assertEquals(QueryMode.PLAN, requests.get(2).getQueryMode());
    assertEquals(selectIdSql, requests.get(3).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(3).getQueryMode());
    assertEquals(deleteSql, requests.get(4).getSql());
    assertEquals(QueryMode.PLAN, requests.get(4).getQueryMode());
    assertEquals(deleteSql, requests.get(5).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(5).getQueryMode());
  }

  @Ignore("Skip this, as the SQL generated by createMany contains the columns in random order")
  @Test
  public void testCreateManyAllTypes() throws IOException, InterruptedException {
    String insertSql =
        "INSERT INTO \"public\".\"AllTypes\" (\"col_numeric\",\"col_bigint\",\"col_date\",\"col_float8\",\"col_varchar\",\"col_bool\",\"col_int\",\"col_bytea\",\"col_jsonb\",\"col_timestamptz\") "
            + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10), ($11,$12,$13,$14,$15,$16,$17,$18,$19,$20)";
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
                TypeCode.JSON,
                TypeCode.INT64,
                TypeCode.BOOL,
                TypeCode.BYTES,
                TypeCode.FLOAT64,
                TypeCode.INT64,
                TypeCode.NUMERIC,
                TypeCode.TIMESTAMP,
                TypeCode.DATE,
                TypeCode.STRING,
                TypeCode.JSON));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(insertMetadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test1"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T12:18:02.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test1")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value1\"}"))
                .bind("p11")
                .to(2L)
                .bind("p12")
                .to(false)
                .bind("p13")
                .to(ByteArray.copyFrom("test2"))
                .bind("p14")
                .to(-3.14d)
                .bind("p15")
                .to(-100)
                .bind("p16")
                .to(com.google.cloud.spanner.Value.pgNumeric("-6.626"))
                .bind("p17")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02.123456000Z"))
                .bind("p18")
                .to(Date.parseDate("2022-03-30"))
                .bind("p19")
                .to("test2")
                .bind("p20")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value2\"}"))
                .build(),
            2L));

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

    String output = runTest("testCreateManyAllTypes", getHost(), pgServer.getLocalPort());

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
