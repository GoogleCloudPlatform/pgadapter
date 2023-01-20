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
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
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
        "Host=%s;Port=%d;Database=d;SSL Mode=Disable", host, pgServer.getLocalPort());
  }

  @Test
  public void testShowServerVersion() throws IOException, InterruptedException {
    String result = execute("TestShowServerVersion", createConnectionString());
    assertEquals("14.1\n", result);
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
        "UPDATE all_types SET col_bigint=$1, col_bool=$2, col_bytea=$3, col_float8=$4, col_int=$5, col_numeric=$6, col_timestamptz=$7, col_date=$8, col_varchar=$9, col_jsonb=$10 WHERE col_varchar = $11";
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
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                .bind("p11")
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
  }

  @Test
  public void testInsertAllDataTypes() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
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
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
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
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning *";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
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
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
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
    String sql =
        "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
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
                  .bind("p10")
                  .to(
                      com.google.cloud.spanner.Value.pgJsonb(
                          String.format("{\"key\":\"value%d\"}", i)))
                  .build(),
              1L));
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
}
