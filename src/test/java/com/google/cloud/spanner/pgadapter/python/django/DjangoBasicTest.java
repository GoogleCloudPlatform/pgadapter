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

package com.google.cloud.spanner.pgadapter.python.django;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class DjangoBasicTest extends DjangoTestSetup {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  private ResultSet createResultSet(List<String> rows) {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .setName("singerid")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("firstname")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("lastname")
                            .build())
                    .build())
            .build());
    for (int i = 0; i < rows.size(); i += 3) {
      String singerid = rows.get(i), firstname = rows.get(i + 1), lastname = rows.get(i + 2);
      resultSetBuilder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(singerid).build())
              .addValues(Value.newBuilder().setStringValue(firstname).build())
              .addValues(Value.newBuilder().setStringValue(lastname).build())
              .build());
    }
    return resultSetBuilder.build();
  }

  @Test
  public void testSelectAll() throws IOException, InterruptedException {
    String sqlSelectAll =
        "SELECT \"singers\".\"singerid\", \"singers\".\"firstname\", \"singers\".\"lastname\" FROM \"singers\"";

    List<String> result =
        new ArrayList<>(Arrays.asList("1", "hello", "world", "2", "hello", "django"));

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelectAll), createResultSet(result)));
    String expectedOutput =
        "{'singerid': 1, 'firstname': 'hello', 'lastname': 'world'}\n"
            + "{'singerid': 2, 'firstname': 'hello', 'lastname': 'django'}\n";
    List<String> options = new ArrayList<>(Arrays.asList("all"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(
        sqlSelectAll, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testInsert() throws IOException, InterruptedException {
    String sqlUpdate =
        "UPDATE \"singers\" SET \"firstname\" = 'john', \"lastname\" = 'doe' WHERE \"singers\".\"singerid\" = 4";
    String sqlInsert =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (4, 'john', 'doe')";

    // Django first sends an update statement
    // If the updated_rowcount returned by the server is 0, then only it sends the insert statement
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlUpdate), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlInsert), 1));
    String expectedOutput = "Save Successful For 4 john doe\n";
    List<String> options = new ArrayList<>(Arrays.asList("insert", "4", "john", "doe"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlUpdate, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
    assertEquals(sqlInsert, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1).getSql());
  }

  @Test
  public void testUpdate() throws IOException, InterruptedException {
    String sqlUpdate =
        "UPDATE \"singers\" SET \"firstname\" = 'john', \"lastname\" = 'doe' WHERE \"singers\".\"singerid\" = 4";

    // Django first sends an update statement
    // If the updated_rowcount returned by the server is nonzero, then it won't send the insert
    // statement
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlUpdate), 1));

    String expectedOutput = "Save Successful For 4 john doe\n";
    List<String> options = new ArrayList<>(Arrays.asList("insert", "4", "john", "doe"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlUpdate, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testSimpleFilter() throws IOException, InterruptedException {
    String sqlSelect =
        "SELECT \"singers\".\"singerid\", \"singers\".\"firstname\", \"singers\".\"lastname\" FROM \"singers\" WHERE \"singers\".\"firstname\" = 'hello'";

    List<String> result =
        new ArrayList<>(Arrays.asList("1", "hello", "world", "2", "hello", "django"));

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelect), createResultSet(result)));

    String expectedOutput =
        "{'singerid': 1, 'firstname': 'hello', 'lastname': 'world'}\n"
            + "{'singerid': 2, 'firstname': 'hello', 'lastname': 'django'}\n";
    List<String> options = new ArrayList<>(Arrays.asList("filter", "firstname = 'hello'"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlSelect, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());

    mockSpanner.clearRequests();

    result = new ArrayList<>(Arrays.asList("2", "hello", "django"));

    sqlSelect =
        "SELECT \"singers\".\"singerid\", \"singers\".\"firstname\", \"singers\".\"lastname\" FROM \"singers\" WHERE (\"singers\".\"firstname\" = 'hello' AND \"singers\".\"singerid\" = 2)";

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelect), createResultSet(result)));

    expectedOutput = "{'singerid': 2, 'firstname': 'hello', 'lastname': 'django'}\n";
    options = new ArrayList<>(Arrays.asList("filter", "firstname = 'hello'", "singerid = 2"));
    actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlSelect, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testInsertAllTypes() throws IOException, InterruptedException {

    String sqlUpdate =
        "UPDATE \"all_types\" "
            + "SET \"col_bool\" = true, "
            + "\"col_bytea\" = '\\x68656c6c6f'::bytea, "
            + "\"col_float8\" = 26.8, "
            + "\"col_int\" = 13, "
            + "\"col_numeric\" = 95.6000000000000, "
            + "\"col_timestamptz\" = '2018-12-25T09:29:36+00:00'::timestamptz, "
            + "\"col_date\" = '1998-10-02'::date, "
            + "\"col_varchar\" = 'some text', "
            + "\"col_jsonb\" = '{\"key\": \"value\"}' "
            + "WHERE \"all_types\".\"col_bigint\" = 6";
    String sqlInsert =
        "INSERT INTO \"all_types\" "
            + "(\"col_bigint\", "
            + "\"col_bool\", "
            + "\"col_bytea\", "
            + "\"col_float8\", "
            + "\"col_int\", "
            + "\"col_numeric\", "
            + "\"col_timestamptz\", "
            + "\"col_date\", "
            + "\"col_varchar\", "
            + "\"col_jsonb\") "
            + "VALUES (6, "
            + "true, "
            + "'\\x68656c6c6f'::bytea, 26.8, 13, "
            + "95.6000000000000, "
            + "'2018-12-25T09:29:36+00:00'::timestamptz, '1998-10-02'::date, "
            + "'some text', "
            + "'{\"key\": \"value\"}')";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlUpdate), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlInsert), 1));
    String expectedOutput = "Insert Successful\n";
    List<String> options = new ArrayList<>(Arrays.asList("all_types_insert"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlUpdate, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
    assertEquals(sqlInsert, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1).getSql());
  }

  @Test
  public void testInsertAllTypesAllNull() throws IOException, InterruptedException {

    String sqlInsert =
        "INSERT INTO \"all_types\" "
            + "(\"col_bigint\", "
            + "\"col_bool\", "
            + "\"col_bytea\", "
            + "\"col_float8\", "
            + "\"col_int\", "
            + "\"col_numeric\", "
            + "\"col_timestamptz\", "
            + "\"col_date\", "
            + "\"col_varchar\", "
            + "\"col_jsonb\") "
            + "VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlInsert), 1));
    String expectedOutput = "Insert Successful\n";
    List<String> options = new ArrayList<>(Arrays.asList("all_types_insert_null"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);

    assertNotNull(request.getTransaction().getBegin());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(sqlInsert, request.getSql());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSelectAllNull() throws IOException, InterruptedException {

    String sqlQuery =
        "SELECT "
            + "\"all_types\".\"col_bigint\", "
            + "\"all_types\".\"col_bool\", "
            + "\"all_types\".\"col_bytea\", "
            + "\"all_types\".\"col_float8\", "
            + "\"all_types\".\"col_int\", "
            + "\"all_types\".\"col_numeric\", "
            + "\"all_types\".\"col_timestamptz\", "
            + "\"all_types\".\"col_date\", "
            + "\"all_types\".\"col_varchar\", "
            + "\"all_types\".\"col_jsonb\" "
            + "FROM \"all_types\" "
            + "WHERE "
            + "(\"all_types\".\"col_bigint\" IS NULL AND "
            + "\"all_types\".\"col_bool\" IS NULL AND "
            + "\"all_types\".\"col_bytea\" IS NULL AND "
            + "\"all_types\".\"col_date\" IS NULL AND "
            + "\"all_types\".\"col_float8\" IS NULL AND "
            + "\"all_types\".\"col_int\" IS NULL AND "
            + "\"all_types\".\"col_jsonb\" = 'null' AND "
            + "\"all_types\".\"col_numeric\" IS NULL AND "
            + "\"all_types\".\"col_timestamptz\" IS NULL AND "
            + "\"all_types\".\"col_varchar\" IS NULL)";

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlQuery), ALL_TYPES_NULLS_RESULTSET));

    String expectedOutput =
        "{'col_bigint': None, "
            + "'col_bool': None, "
            + "'col_bytea': None, "
            + "'col_float8': None, "
            + "'col_int': None, "
            + "'col_numeric': None, "
            + "'col_timestamptz': None, "
            + "'col_date': None, "
            + "'col_varchar': None, "
            + "'col_jsonb': None}\n";
    List<String> options = new ArrayList<>(Arrays.asList("select_all_null"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlQuery, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testSelectAllTypes() throws IOException, InterruptedException {

    String sqlQuery =
        "SELECT \"all_types\".\"col_bigint\", "
            + "\"all_types\".\"col_bool\", "
            + "\"all_types\".\"col_bytea\", "
            + "\"all_types\".\"col_float8\", "
            + "\"all_types\".\"col_int\", "
            + "\"all_types\".\"col_numeric\", "
            + "\"all_types\".\"col_timestamptz\", "
            + "\"all_types\".\"col_date\", "
            + "\"all_types\".\"col_varchar\", "
            + "\"all_types\".\"col_jsonb\" "
            + "FROM \"all_types\"";

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlQuery), ALL_TYPES_RESULTSET));
    System.out.println(ALL_TYPES_RESULTSET.getRows(0).getValues(6).getStringValue());
    String expectedOutput =
        "col_bigint : 1 , "
            + "col_bool : True , "
            + "col_bytea : b'test' , "
            + "col_float8 : 3.14 , "
            + "col_int : 100 , "
            + "col_numeric : 6.626 , "
            + "col_timestamptz : 2022-02-16 13:18:02.123456+00:00 , "
            + "col_date : 2022-03-29 , "
            + "col_varchar : test , "
            + "col_jsonb : {'key': 'value'} , \n";
    List<String> options = new ArrayList<>(Arrays.asList("select_all_types"));
    String actualOutput = executeBasicTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertEquals(sqlQuery, request.getSql());
  }
}
