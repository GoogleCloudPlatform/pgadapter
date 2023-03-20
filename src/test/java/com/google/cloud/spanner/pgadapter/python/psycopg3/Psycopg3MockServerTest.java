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

package com.google.cloud.spanner.pgadapter.python.psycopg3;

import static com.google.cloud.spanner.pgadapter.python.PythonTestUtil.run;
import static org.junit.Assert.assertArrayEquals;
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
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.CopyInMockServerTest;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
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
import java.io.File;
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
import org.postgresql.util.ByteConverter;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class Psycopg3MockServerTest extends AbstractMockServerTest {
  static final String DIRECTORY_NAME = "./src/test/python/psycopg3";

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  @BeforeClass
  public static void createVirtualEnv() throws Exception {
    PythonTestUtil.createVirtualEnv(DIRECTORY_NAME);
  }

  String createConnectionString() {
    return String.format("host=%s port=%d dbname=d sslmode=disable", host, pgServer.getLocalPort());
  }

  String execute(String method) throws Exception {
    return execute(method, createConnectionString());
  }

  static String execute(String method, String connectionString) throws Exception {
    File directory = new File(DIRECTORY_NAME);
    // Make sure to run the python executable in the specific virtual environment.
    return run(
        new String[] {
          directory.getAbsolutePath() + "/venv/bin/python3",
          "psycopg3_tests.py",
          method,
          connectionString
        },
        DIRECTORY_NAME);
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    String actualOutput = execute("select1");
    String expectedOutput = "(1,)\n";
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
    assertEquals("14.1\n", actualOutput);
  }

  @Test
  public void testShowApplicationName() throws Exception {
    String actualOutput = execute("show_application_name");
    assertEquals("None\n", actualOutput);
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
    assertEquals("('baz',)\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does not include parameter types, only parameter values. This means that PGAdapter
    // will do an auto-describe the first time it sees a statement.
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
    // Verifies that executing the same query twice will only auto-describe it once.
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
    assertEquals("('baz',)\n('foo',)\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does not include parameter types, only parameter values. This means that PGAdapter
    // will do an auto-describe the first time it sees a statement. So there should be one describe
    // request and two execute requests.
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
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n",
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
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(), ALL_TYPES_RESULTSET));

    String result = execute("query_all_data_types_with_parameter");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n",
        result);

    // psycopg3 does include the type of the parameter when it is an int, so PGAdapter does not
    // need to do an auto-describe roundtrip for this statement.
    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
  }

  @Test
  public void testQueryAllDataTypesText() throws Exception {
    testQueryAllDataTypesWithFixedFormat(DataFormat.POSTGRESQL_TEXT);
  }

  @Test
  public void testQueryAllDataTypesBinary() throws Exception {
    testQueryAllDataTypesWithFixedFormat(DataFormat.POSTGRESQL_BINARY);
  }

  private void testQueryAllDataTypesWithFixedFormat(DataFormat format) throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(), ALL_TYPES_RESULTSET));

    String result =
        execute(
            "query_all_data_types_" + (format == DataFormat.POSTGRESQL_BINARY ? "binary" : "text"));
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n",
        result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());

    List<BindMessage> bindMessages =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof BindMessage)
            .map(message -> (BindMessage) message)
            .collect(Collectors.toList());
    assertEquals(1, bindMessages.size());
    BindMessage bindMessage = bindMessages.get(0);
    assertEquals(1, bindMessage.getResultFormatCodes().size());
    assertEquals(format.getCode(), bindMessage.getResultFormatCodes().get(0).shortValue());
  }

  @Test
  public void testUpdateAllDataTypes() throws Exception {
    String sql =
        "UPDATE all_types SET col_bool=$1, col_bytea=$2, col_float8=$3, col_int=$4, col_numeric=$5, col_timestamptz=$6, col_date=$7, col_varchar=$8, col_jsonb=$9 WHERE col_varchar = $10";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .bind("p10")
                .to("test")
                .build(),
            1L));

    String result = execute("update_all_data_types");
    assertEquals("Update count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does not include an explicit type for string parameters. This causes PGAdapter to do
    // an auto-describe roundtrip the first time it sees the statement.
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
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            1L));

    String result = execute("insert_all_data_types");
    assertEquals("Insert count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does not include an explicit type for string parameters. This causes PGAdapter to do
    // an auto-describe roundtrip the first time it sees the statement.
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
  public void testInsertAllDataTypesBinary() throws Exception {
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
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            1L));

    String result = execute("insert_all_data_types_binary");
    assertEquals("Insert count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does include a type code for strings when using the binary protocol. We therefore do
    // not need to do an auto-describe roundtrip in this case.
    assertEquals(1, requests.size());
    ExecuteSqlRequest executeRequest = requests.get(0);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.hasTransaction());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));

    List<BindMessage> bindMessages =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof BindMessage)
            .map(message -> (BindMessage) message)
            .collect(Collectors.toList());
    assertEquals(1, bindMessages.size());
    BindMessage bindMessage = bindMessages.get(0);
    assertEquals(10, bindMessage.getFormatCodes().size());
    assertArrayEquals(
        new Short[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
        bindMessage.getFormatCodes().toArray(new Short[0]));
  }

  @Test
  public void testInsertAllDataTypesText() throws Exception {
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
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            1L));

    String result = execute("insert_all_data_types_text");
    assertEquals("Insert count: 1\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does not include an explicit type for string parameters when they are sent as text.
    // This causes PGAdapter to do an auto-describe roundtrip the first time it sees the statement.
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

    List<BindMessage> bindMessages =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof BindMessage)
            .map(message -> (BindMessage) message)
            .collect(Collectors.toList());
    assertEquals(1, bindMessages.size());
    BindMessage bindMessage = bindMessages.get(0);
    assertEquals(10, bindMessage.getFormatCodes().size());
    assertArrayEquals(
        new Short[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        bindMessage.getFormatCodes().toArray(new Short[0]));
  }

  @Test
  public void testInsertNullsAllDataTypes() throws Exception {
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

    String result = execute("insert_nulls_all_data_types");
    assertEquals("Insert count: 1\n", result);

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
  public void testInsertAllDataTypesReturning() throws Exception {
    String sql = getInsertAllTypesSql() + " returning *";
    ResultSetMetadata metadata =
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
                    .getUndeclaredParameters())
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
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(ALL_TYPES_RESULTSET.getRows(0))
                .build()));

    String result = execute("insert_all_data_types_returning");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n",
        result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    // psycopg3 does not include an explicit type for string parameters. This causes PGAdapter to do
    // an auto-describe roundtrip the first time it sees the statement.
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
  public void testInsertBatch() throws Exception {
    addDescribeInsertAllTypesResult();
    int batchSize = 10;
    for (int i = 0; i < batchSize; i++) {
      mockSpanner.putStatementResult(StatementResult.update(createBatchInsertStatement(i), 1L));
    }

    String result = execute("insert_batch");
    assertEquals("Insert count: 10\n", result);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest batchDmlRequest =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(batchSize, batchDmlRequest.getStatementsCount());
    assertTrue(batchDmlRequest.getTransaction().hasBegin());
    // We get two commits, as PGAdapter does an auto-describe roundtrip for the SQL statement.
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testMixedBatch() throws Exception {
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

    addDescribeInsertAllTypesResult();
    int batchSize = 5;
    for (int i = 0; i < batchSize; i++) {
      mockSpanner.putStatementResult(StatementResult.update(createBatchInsertStatement(i), 1L));
    }

    String result = execute("mixed_batch");
    assertEquals("Insert count: 5\n" + "Count: (3,)\n" + "Update count: 3\n", result);

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
  public void testBatchExecutionError() throws Exception {
    addDescribeInsertAllTypesResult();
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

    String result = execute("batch_execution_error");
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
  public void testDdlBatch() throws Exception {
    addDdlResponseToSpannerAdmin();

    String result = execute("ddl_batch");
    assertEquals("Update count: -1\n", result);

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
  public void testDdlScript() throws Exception {
    addDdlResponseToSpannerAdmin();

    String result = execute("ddl_script");
    assertEquals("Update count: -1\n", result);

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
  public void testBinaryCopyIn() throws Exception {
    testCopyIn("binary_copy_in");
  }

  @Test
  public void testTextCopyIn() throws Exception {
    testCopyIn("text_copy_in");
  }

  private void testCopyIn(String testMethod) throws Exception {
    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute(testMethod);
    assertEquals("Copy count: 2\n", result);

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
        Base64.getEncoder().encodeToString("test_bytes".getBytes(StandardCharsets.UTF_8)),
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
  public void testBinaryCopyOut() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb from all_types"),
            ALL_TYPES_RESULTSET
                .toBuilder()
                .addAllRows(ALL_TYPES_NULLS_RESULTSET.getRowsList())
                .build()));

    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute("binary_copy_out");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n"
            + "col_bigint: None\n"
            + "col_bool: None\n"
            + "col_bytea: None\n"
            + "col_float8: None\n"
            + "col_int: None\n"
            + "col_numeric: None\n"
            + "col_timestamptz: None\n"
            + "col_date: None\n"
            + "col_string: None\n"
            + "col_jsonb: None\n"
            + "col_array_bigint: None\n"
            + "col_array_bool: None\n"
            + "col_array_bytea: None\n"
            + "col_array_float8: None\n"
            + "col_array_int: None\n"
            + "col_array_numeric: None\n"
            + "col_array_timestamptz: None\n"
            + "col_array_date: None\n"
            + "col_array_string: None\n"
            + "col_array_jsonb: None\n",
        result);
  }

  @Test
  public void testTextCopyOut() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, "
                    + "col_timestamptz, col_date, col_varchar, col_jsonb, "
                    + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, "
                    + "col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, "
                    + "col_array_varchar, col_array_jsonb "
                    + "from all_types"),
            ALL_TYPES_RESULTSET
                .toBuilder()
                .addAllRows(ALL_TYPES_NULLS_RESULTSET.getRowsList())
                .build()));

    CopyInMockServerTest.setupCopyInformationSchemaResults(
        mockSpanner, "public", "all_types", true);

    String result = execute("text_copy_out");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n"
            + "col_bigint: None\n"
            + "col_bool: None\n"
            + "col_bytea: None\n"
            + "col_float8: None\n"
            + "col_int: None\n"
            + "col_numeric: None\n"
            + "col_timestamptz: None\n"
            + "col_date: None\n"
            + "col_string: None\n"
            + "col_jsonb: None\n"
            + "col_array_bigint: None\n"
            + "col_array_bool: None\n"
            + "col_array_bytea: None\n"
            + "col_array_float8: None\n"
            + "col_array_int: None\n"
            + "col_array_numeric: None\n"
            + "col_array_timestamptz: None\n"
            + "col_array_date: None\n"
            + "col_array_string: None\n"
            + "col_array_jsonb: None\n",
        result);
  }

  @Test
  public void testPrepareQuery() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(), ALL_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(2L).build(), ALL_TYPES_NULLS_RESULTSET));

    String result = execute("prepare_query");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n"
            + "col_bigint: None\n"
            + "col_bool: None\n"
            + "col_bytea: None\n"
            + "col_float8: None\n"
            + "col_int: None\n"
            + "col_numeric: None\n"
            + "col_timestamptz: None\n"
            + "col_date: None\n"
            + "col_string: None\n"
            + "col_jsonb: None\n"
            + "col_array_bigint: None\n"
            + "col_array_bool: None\n"
            + "col_array_bytea: None\n"
            + "col_array_float8: None\n"
            + "col_array_int: None\n"
            + "col_array_numeric: None\n"
            + "col_array_timestamptz: None\n"
            + "col_array_date: None\n"
            + "col_array_string: None\n"
            + "col_array_jsonb: None\n",
        result);

    List<ParseMessage> parseMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof ParseMessage)
            .map(msg -> (ParseMessage) msg)
            .filter(msg -> msg.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, parseMessages.size());
    ParseMessage parseMessage = parseMessages.get(0);
    List<BindMessage> bindMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof BindMessage)
            .map(msg -> (BindMessage) msg)
            .filter(msg -> msg.getStatementName().equals(parseMessage.getName()))
            .collect(Collectors.toList());
    assertEquals(2, bindMessages.size());
    // psycopg3 sends small integer values as int2 values.
    assertEquals(1, ByteConverter.int2(bindMessages.get(0).getParameters()[0], 0));
    assertEquals(2, ByteConverter.int2(bindMessages.get(1).getParameters()[0], 0));
  }

  @Test
  public void testInt8Param() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to((long) Integer.MAX_VALUE + 1L).build(),
            ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));

    String result = execute("int8_param");
    assertEquals("None\n", result);

    List<ParseMessage> parseMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof ParseMessage)
            .map(msg -> (ParseMessage) msg)
            .filter(msg -> msg.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, parseMessages.size());
    ParseMessage parseMessage = parseMessages.get(0);
    List<BindMessage> bindMessages =
        pgServer.getDebugMessages().stream()
            .filter(msg -> msg instanceof BindMessage)
            .map(msg -> (BindMessage) msg)
            .filter(msg -> msg.getStatementName().equals(parseMessage.getName()))
            .collect(Collectors.toList());
    assertEquals(1, bindMessages.size());
    // psycopg3 sends large integer values as int8 values.
    assertEquals(
        (long) Integer.MAX_VALUE + 1L,
        ByteConverter.int8(bindMessages.get(0).getParameters()[0], 0));
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
                  .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\": \"value\"}"))
                  .build(),
              1L));
    }

    String result = execute("read_write_transaction");
    assertEquals("(1,)\n" + "Insert count: 1\n" + "Insert count: 1\n", result);

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
    assertEquals("(1,)\n" + "(2,)\n", result);

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
  @Ignore("Named cursors are not yet supported")
  public void testNamedCursor() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(), ALL_TYPES_RESULTSET));

    String result = execute("named_cursor");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n",
        result);
  }

  @Test
  public void testNestedTransaction() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(), ALL_TYPES_RESULTSET));

    String result = execute("nested_transaction");
    assertEquals(
        "col_bigint: 1\n"
            + "col_bool: True\n"
            + "col_bytea: b'test'\n"
            + "col_float8: 3.14\n"
            + "col_int: 100\n"
            + "col_numeric: 6.626\n"
            + "col_timestamptz: 2022-02-16 13:18:02.123456+00:00\n"
            + "col_date: 2022-03-29\n"
            + "col_string: test\n"
            + "col_jsonb: {'key': 'value'}\n"
            + "col_array_bigint: [1, None, 2]\n"
            + "col_array_bool: [True, None, False]\n"
            + "col_array_bytea: [b'bytes1', None, b'bytes2']\n"
            + "col_array_float8: [3.14, None, -99.99]\n"
            + "col_array_int: [-100, None, -200]\n"
            + "col_array_numeric: [Decimal('6.626'), None, Decimal('-3.14')]\n"
            + "col_array_timestamptz: [datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=<UTC>), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=<UTC>)]\n"
            + "col_array_date: [datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)]\n"
            + "col_array_string: ['string1', None, 'string2']\n"
            + "col_array_jsonb: [{'key': 'value1'}, None, {'key': 'value2'}]\n",
        result);
  }

  @Test
  public void testRollbackNestedTransaction() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql), ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(), ALL_TYPES_RESULTSET));

    String result = execute("rollback_nested_transaction");
    assertEquals(
        "Nested transaction failed with error: Test rollback of savepoint\n"
            + "Outer transaction failed with error: current transaction is aborted, commands ignored until end of transaction block\n",
        result);
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
        .to(com.google.cloud.spanner.Value.pgJsonb(String.format("{\"key\": \"value%d\"}", index)))
        .build();
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
