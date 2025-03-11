// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.php.pdo;

import static com.google.cloud.spanner.pgadapter.python.PythonTestUtil.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.CopyInMockServerTest;
import com.google.cloud.spanner.pgadapter.php.PhpTest;
import com.google.common.collect.ImmutableList;
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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PhpTest.class)
public class PdoMockServerTest extends AbstractMockServerTest {
  static final String DIRECTORY_NAME = "./src/test/php/pdo";

  @BeforeClass
  public static void installDependencies() throws Exception {
    run(new String[] {"composer", "install"}, DIRECTORY_NAME);
  }

  static String execute(String method) throws Exception {
    return run(
        new String[] {
          "php", "pdo_test.php", method, "/tmp", String.valueOf(pgServer.getLocalPort())
        },
        DIRECTORY_NAME);
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    String actualOutput = execute("select1");
    String expectedOutput = "Result: 1\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testShowServerVersion() throws Exception {
    String actualOutput = execute("show_server_version");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [server_version] => 14.1\n"
            + "            [0] => 14.1\n"
            + "        )\n"
            + "\n"
            + ")\n",
        actualOutput);
  }

  @Test
  public void testShowApplicationName() throws Exception {
    String actualOutput = execute("show_application_name");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [application_name] => php_pdo\n"
            + "            [0] => php_pdo\n"
            + "        )\n"
            + "\n"
            + ")\n",
        actualOutput);
  }

  @Test
  public void testQueryWithParameter() throws Exception {
    String sql = "SELECT * FROM FOO WHERE BAR=$1";
    Statement statement = Statement.newBuilder(sql).bind("p1").to("baz").build();

    ResultSetMetadata metadata =
        createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
            .toBuilder()
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

    String result = execute("query_with_parameter");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [BAR] => baz\n"
            + "            [0] => baz\n"
            + "        )\n"
            + "\n"
            + ")\n",
        result);

    // PDO uses named prepared statements.
    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());

    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(0, describeRequest.getParamTypesCount());
    assertEquals(0, describeRequest.getParams().getFieldsCount());

    ExecuteSqlRequest request = requests.get(1);
    assertNotNull(request.getParams());
    assertFalse(request.getParams().getFieldsMap().isEmpty());
    assertEquals("baz", request.getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals(
        Type.newBuilder().setCode(TypeCode.STRING).build(), request.getParamTypesMap().get("p1"));
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testQueryWithParameterTwice() throws Exception {
    // Verifies that executing the same query twice will only describe it once.
    String sql = "SELECT * FROM FOO WHERE BAR=$1";
    Statement selectBaz = Statement.newBuilder(sql).bind("p1").to("baz").build();
    Statement selectFoo = Statement.newBuilder(sql).bind("p1").to("foo").build();

    ResultSetMetadata metadata =
        createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING))
            .toBuilder()
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
            selectBaz,
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("baz").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            selectFoo,
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("foo").build())
                        .build())
                .build()));

    String result = execute("query_with_parameter_twice");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [BAR] => baz\n"
            + "            [0] => baz\n"
            + "        )\n"
            + "\n"
            + ")\n"
            + "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [BAR] => foo\n"
            + "            [0] => foo\n"
            + "        )\n"
            + "\n"
            + ")\n",
        result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // PDO should re-use the prepared statement.
    assertEquals(3, requests.size());

    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertEquals(0, describeRequest.getParamTypesCount());
    assertEquals(0, describeRequest.getParams().getFieldsCount());

    for (int i = 1; i <= 2; i++) {
      ExecuteSqlRequest request = requests.get(i);
      assertNotNull(request.getParams());
      assertFalse(request.getParams().getFieldsMap().isEmpty());
      assertEquals(
          i == 1 ? "baz" : "foo", request.getParams().getFieldsMap().get("p1").getStringValue());
      assertEquals(
          Type.newBuilder().setCode(TypeCode.STRING).build(), request.getParamTypesMap().get("p1"));
      assertEquals(QueryMode.NORMAL, request.getQueryMode());
    }
  }

  @Test
  public void testQueryAllDataTypes() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String result = execute("query_all_data_types");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [col_bigint] => 1\n"
            + "            [0] => 1\n"
            + "            [col_bool] => 1\n"
            + "            [1] => 1\n"
            + "            [col_bytea] => Resource id #4\n"
            + "            [2] => Resource id #4\n"
            + "            [col_float4] => 3.14\n"
            + "            [3] => 3.14\n"
            + "            [col_float8] => 3.14\n"
            + "            [4] => 3.14\n"
            + "            [col_int] => 100\n"
            + "            [5] => 100\n"
            + "            [col_numeric] => 6.626\n"
            + "            [6] => 6.626\n"
            + "            [col_timestamptz] => 2022-02-16 14:18:02.123456+01\n"
            + "            [7] => 2022-02-16 14:18:02.123456+01\n"
            + "            [col_date] => 2022-03-29\n"
            + "            [8] => 2022-03-29\n"
            + "            [col_varchar] => test\n"
            + "            [9] => test\n"
            + "            [col_jsonb] => {\"key\": \"value\"}\n"
            + "            [10] => {\"key\": \"value\"}\n"
            + "            [col_array_bigint] => {1,NULL,2}\n"
            + "            [11] => {1,NULL,2}\n"
            + "            [col_array_bool] => {t,NULL,f}\n"
            + "            [12] => {t,NULL,f}\n"
            + "            [col_array_bytea] => {\"\\\\x627974657331\",NULL,\"\\\\x627974657332\"}\n"
            + "            [13] => {\"\\\\x627974657331\",NULL,\"\\\\x627974657332\"}\n"
            + "            [col_array_float4] => {3.14,NULL,-99.99}\n"
            + "            [14] => {3.14,NULL,-99.99}\n"
            + "            [col_array_float8] => {3.14,NULL,-99.99}\n"
            + "            [15] => {3.14,NULL,-99.99}\n"
            + "            [col_array_int] => {-100,NULL,-200}\n"
            + "            [16] => {-100,NULL,-200}\n"
            + "            [col_array_numeric] => {6.626,NULL,-3.14}\n"
            + "            [17] => {6.626,NULL,-3.14}\n"
            + "            [col_array_timestamptz] => {\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\n"
            + "            [18] => {\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\n"
            + "            [col_array_date] => {\"2023-02-20\",NULL,\"2000-01-01\"}\n"
            + "            [19] => {\"2023-02-20\",NULL,\"2000-01-01\"}\n"
            + "            [col_array_varchar] => {\"string1\",NULL,\"string2\"}\n"
            + "            [20] => {\"string1\",NULL,\"string2\"}\n"
            + "            [col_array_jsonb] => {\"{\\\"key\\\": \\\"value1\\\"}\",NULL,\"{\\\"key\\\": \\\"value2\\\"}\"}\n"
            + "            [21] => {\"{\\\"key\\\": \\\"value1\\\"}\",NULL,\"{\\\"key\\\": \\\"value2\\\"}\"}\n"
            + "        )\n"
            + "\n"
            + ")\n",
        result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testQueryAllDataTypesWithParameter() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String result = execute("query_all_data_types_with_parameter");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [col_bigint] => 1\n"
            + "            [0] => 1\n"
            + "            [col_bool] => 1\n"
            + "            [1] => 1\n"
            + "            [col_bytea] => Resource id #4\n"
            + "            [2] => Resource id #4\n"
            + "            [col_float4] => 3.14\n"
            + "            [3] => 3.14\n"
            + "            [col_float8] => 3.14\n"
            + "            [4] => 3.14\n"
            + "            [col_int] => 100\n"
            + "            [5] => 100\n"
            + "            [col_numeric] => 6.626\n"
            + "            [6] => 6.626\n"
            + "            [col_timestamptz] => 2022-02-16 14:18:02.123456+01\n"
            + "            [7] => 2022-02-16 14:18:02.123456+01\n"
            + "            [col_date] => 2022-03-29\n"
            + "            [8] => 2022-03-29\n"
            + "            [col_varchar] => test\n"
            + "            [9] => test\n"
            + "            [col_jsonb] => {\"key\": \"value\"}\n"
            + "            [10] => {\"key\": \"value\"}\n"
            + "            [col_array_bigint] => {1,NULL,2}\n"
            + "            [11] => {1,NULL,2}\n"
            + "            [col_array_bool] => {t,NULL,f}\n"
            + "            [12] => {t,NULL,f}\n"
            + "            [col_array_bytea] => {\"\\\\x627974657331\",NULL,\"\\\\x627974657332\"}\n"
            + "            [13] => {\"\\\\x627974657331\",NULL,\"\\\\x627974657332\"}\n"
            + "            [col_array_float4] => {3.14,NULL,-99.99}\n"
            + "            [14] => {3.14,NULL,-99.99}\n"
            + "            [col_array_float8] => {3.14,NULL,-99.99}\n"
            + "            [15] => {3.14,NULL,-99.99}\n"
            + "            [col_array_int] => {-100,NULL,-200}\n"
            + "            [16] => {-100,NULL,-200}\n"
            + "            [col_array_numeric] => {6.626,NULL,-3.14}\n"
            + "            [17] => {6.626,NULL,-3.14}\n"
            + "            [col_array_timestamptz] => {\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\n"
            + "            [18] => {\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\n"
            + "            [col_array_date] => {\"2023-02-20\",NULL,\"2000-01-01\"}\n"
            + "            [19] => {\"2023-02-20\",NULL,\"2000-01-01\"}\n"
            + "            [col_array_varchar] => {\"string1\",NULL,\"string2\"}\n"
            + "            [20] => {\"string1\",NULL,\"string2\"}\n"
            + "            [col_array_jsonb] => {\"{\\\"key\\\": \\\"value1\\\"}\",NULL,\"{\\\"key\\\": \\\"value2\\\"}\"}\n"
            + "            [21] => {\"{\\\"key\\\": \\\"value1\\\"}\",NULL,\"{\\\"key\\\": \\\"value2\\\"}\"}\n"
            + "        )\n"
            + "\n"
            + ")\n",
        result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());
    ExecuteSqlRequest planRequest = requests.get(0);
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
  }

  @Test
  public void testUpdateAllDataTypes() throws Exception {
    String sql =
        "UPDATE all_types SET col_bool=$1, col_bytea=$2, col_float4=$3, \n"
            + "                     col_float8=$4, col_int=$5, col_numeric=$6, col_timestamptz=$7, \n"
            + "                     col_date=$8, col_varchar=$9, col_jsonb=$10 WHERE col_varchar = $11";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT32,
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
                .to(true)
                .bind("p2")
                .to(ByteArray.copyFrom("test_bytes"))
                .bind("p3")
                .to(3.14f)
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(1L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-24"))
                .bind("p9")
                .to("test_string")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .bind("p11")
                .to("test")
                .build(),
            1L));

    String result = execute("update_all_data_types");
    assertEquals("Update count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    // As the test runs in auto-commit mode, both the Describe and the Execute statement are
    // committed.
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testInsertAllDataTypes() throws Exception {
    String sql = getInsertAllTypesSql();
    addDescribeInsertAllTypesResult();
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
                .to(3.14f)
                .bind("p5")
                .to(3.14d)
                .bind("p6")
                .to(1)
                .bind("p7")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p8")
                .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                .bind("p9")
                .to(Date.parseDate("2022-03-24"))
                .bind("p10")
                .to("test_string")
                .bind("p11")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            1L));

    String result = execute("insert_all_data_types");
    assertEquals("Insert count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());
    ExecuteSqlRequest describeRequest = requests.get(0);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest executeRequest = requests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    // As the test runs in auto-commit mode, both the Describe and the Execute statement are
    // committed.
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testInsertNullsAllDataTypes() throws Exception {
    String sql = getInsertAllTypesSql();
    addDescribeInsertAllTypesResult();
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
                .to((Float) null)
                .bind("p5")
                .to((Double) null)
                .bind("p6")
                .to((Long) null)
                .bind("p7")
                .to(com.google.cloud.spanner.Value.pgNumeric(null))
                .bind("p8")
                .to((Timestamp) null)
                .bind("p9")
                .to((Date) null)
                .bind("p10")
                .to((String) null)
                .bind("p11")
                .to(com.google.cloud.spanner.Value.pgJsonb(null))
                .build(),
            1L));

    String result = execute("insert_nulls_all_data_types");
    assertEquals("Insert count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());
  }

  @Test
  public void testTextCopyIn() throws Exception {
    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute("text_copy_in");
    assertEquals("Copy result: 1\n", result);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    CommitRequest request = mockSpanner.getRequestsOfType(CommitRequest.class).get(0);
    assertEquals(1, request.getMutationsCount());
    Mutation mutation = request.getMutations(0);
    assertEquals(OperationCase.INSERT, mutation.getOperationCase());
    assertEquals(2, mutation.getInsert().getValuesCount());
    assertEquals(11, mutation.getInsert().getColumnsCount());
    ListValue insert = mutation.getInsert().getValues(0);

    int index = -1;
    assertEquals("1", insert.getValues(++index).getStringValue());
    assertTrue(insert.getValues(++index).getBoolValue());
    assertEquals(
        Base64.getEncoder()
            .encodeToString("e80f8bc445eccfba5acc37d31c257213".getBytes(StandardCharsets.UTF_8)),
        insert.getValues(++index).getStringValue());
    assertEquals(3.14f, (float) insert.getValues(++index).getNumberValue(), 0.0f);
    assertEquals(3.14, insert.getValues(++index).getNumberValue(), 0.0);
    assertEquals("10", insert.getValues(++index).getStringValue());
    assertEquals("6.626", insert.getValues(++index).getStringValue());
    assertEquals("2022-03-24T06:39:10.123456000Z", insert.getValues(++index).getStringValue());
    assertEquals("2022-03-24", insert.getValues(++index).getStringValue());
    assertEquals("test_string", insert.getValues(++index).getStringValue());
    assertEquals("{\"key\": \"value\"}", insert.getValues(++index).getStringValue());

    insert = mutation.getInsert().getValues(1);
    assertEquals("2", insert.getValues(0).getStringValue());
    for (int i = 1; i < insert.getValuesCount(); i++) {
      assertTrue(insert.getValues(i).hasNullValue());
    }
  }

  @Test
  public void testTextCopyOut() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types"),
            ALL_TYPES_RESULTSET
                .toBuilder()
                .addAllRows(ALL_TYPES_NULLS_RESULTSET.getRowsList())
                .build()));

    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute("text_copy_out");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => 1\tt\t\\\\x74657374\t3.14\t3.14\t100\t6.626\t2022-02-16 14:18:02.123456+01\t2022-03-29\ttest\t{\"key\": \"value\"}\t{1,NULL,2}\t{t,NULL,f}\t{\"\\\\\\\\x627974657331\",NULL,\"\\\\\\\\x627974657332\"}\t{3.14,NULL,-99.99}\t{3.14,NULL,-99.99}\t{-100,NULL,-200}\t{6.626,NULL,-3.14}\t{\"2022-02-16 17:18:02.123456+01\",NULL,\"2000-01-01 01:00:00+01\"}\t{\"2023-02-20\",NULL,\"2000-01-01\"}\t{\"string1\",NULL,\"string2\"}\t{\"{\\\\\"key\\\\\": \\\\\"value1\\\\\"}\",NULL,\"{\\\\\"key\\\\\": \\\\\"value2\\\\\"}\"}\n"
            + "\n"
            + "    [1] => \\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n"
            + "\n"
            + ")\n",
        result);
  }

  @Test
  public void testReadWriteTransaction() throws Exception {
    addDescribeInsertAllTypesResult();
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
                  .to(3.14f)
                  .bind("p5")
                  .to(3.14d)
                  .bind("p6")
                  .to(1)
                  .bind("p7")
                  .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                  .bind("p8")
                  .to(Timestamp.parseTimestamp("2022-03-24T06:39:10.123456000Z"))
                  .bind("p9")
                  .to(Date.parseDate("2022-03-24"))
                  .bind("p10")
                  .to("test_string")
                  .bind("p11")
                  .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                  .build(),
              1L));
    }

    String result = execute("read_write_transaction");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [C] => 1\n"
            + "            [0] => 1\n"
            + "        )\n"
            + "\n"
            + ")\n"
            + "Insert count: 1\n"
            + "Insert count: 1\n",
        result);

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

    // We get the insert request 3 times, as it needs to be auto-described once to get all param
    // types.
    assertEquals(3, insertRequests.size());
    assertEquals(QueryMode.PLAN, insertRequests.get(0).getQueryMode());
    for (ExecuteSqlRequest insertRequest : insertRequests) {
      assertTrue(insertRequest.hasTransaction());
      assertTrue(insertRequest.getTransaction().hasId());
    }
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String result = execute("read_only_transaction");
    assertEquals(
        "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [C] => 1\n"
            + "            [0] => 1\n"
            + "        )\n"
            + "\n"
            + ")\n"
            + "Array\n"
            + "(\n"
            + "    [0] => Array\n"
            + "        (\n"
            + "            [C] => 2\n"
            + "            [0] => 2\n"
            + "        )\n"
            + "\n"
            + ")\n",
        result);

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginTransactionRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
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
    // Read-only transactions are not really committed or rolled back.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testBatchDml() throws Exception {
    String sql = "insert into my_table (id, value) values ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(1L).bind("p2").to("One").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(2L).bind("p2").to("Two").build(), 1L));

    String actualOutput = execute("batch_dml");
    String expectedOutput = "Inserted two rows\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
  }

  @Test
  public void testBatchDmlInTransaction() throws Exception {
    String sql = "insert into my_table (id, value) values ($1, $2)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64, TypeCode.STRING)))
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(1L).bind("p2").to("One").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql).bind("p1").to(2L).bind("p2").to("Two").build(), 1L));

    String actualOutput = execute("batch_dml_in_transaction");
    String expectedOutput = "Inserted two rows\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
  }

  @Test
  public void testBatchDdl() throws Exception {
    addDdlResponseToSpannerAdmin();

    String actualOutput = execute("batch_ddl");
    String expectedOutput = "Created a table and an index in one batch\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockDatabaseAdmin.getRequests().size());
    assertEquals(
        2,
        ((UpdateDatabaseDdlRequest) mockDatabaseAdmin.getRequests().get(0)).getStatementsCount());
  }

  private static String getInsertAllTypesSql() {
    return "INSERT INTO all_types\n"
        + "        (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int,\n"
        + "        col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)\n"
        + "        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
  }

  private void addDescribeInsertAllTypesResult() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(getInsertAllTypesSql()),
            ResultSet.newBuilder()
                .setMetadata(
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
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
  }
}
