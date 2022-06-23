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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PythonTest.class)
public class PythonBasicTests extends PythonTestSetup {

  private ResultSet createResultSet() {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .setName("Id")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("Name")
                            .build())
                    .build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue(String.valueOf(1)).build())
            .addValues(Value.newBuilder().setStringValue("abcd").build())
            .build());
    return resultSetBuilder.build();
  }

  @Test
  public void testBasicSelect() throws IOException, InterruptedException {
    String sql = "SELECT * FROM some_table";

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), createResultSet()));

    String actualOutput = executeWithoutParameters(pgServer.getLocalPort(), sql, "query");
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicUpdate() throws IOException, InterruptedException {
    String sql = "UPDATE SET column_name='value' where column_name2 = 'value2'";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10));

    String actualOutput = executeWithoutParameters(pgServer.getLocalPort(), sql, "update");
    String expectedOutput = "10\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicInsert() throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE(COLUMN_NAME) VALUES ('VALUE')";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1));

    String actualOutput = executeWithoutParameters(pgServer.getLocalPort(), sql, "update");
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicDelete() throws IOException, InterruptedException {
    String sql = "DELETE FROM SOME_TABLE WHERE COLUMN_NAME = VALUE";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 12));

    String actualOutput = executeWithoutParameters(pgServer.getLocalPort(), sql, "update");
    String expectedOutput = "12\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithParameters() throws IOException, InterruptedException {
    String sql = "SELECT * FROM some_table where COLUMN_NAME1 = %s and COLUMN_NAME2 = %s";

    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("VALUE1");
    parameters.add("VALUE2");

    String sql2 =
        "SELECT * FROM some_table where COLUMN_NAME1 = 'VALUE1' and COLUMN_NAME2 = 'VALUE2'";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput = executeWithParameters(pgServer.getLocalPort(), sql, "query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicUpdateWithParameters() throws IOException, InterruptedException {
    String sql = "UPDATE SET column_name=%s where column_name2 = %s";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("VALUE1");
    parameters.add("VALUE2");

    String sql2 = "UPDATE SET column_name='VALUE1' where column_name2 = 'VALUE2'";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 10));

    String actualOutput = executeWithParameters(pgServer.getLocalPort(), sql, "update", parameters);
    String expectedOutput = "10\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicInsertWithParameters() throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE(COLUMN_NAME) VALUES (%s)";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("VALUE");

    String sql2 = "INSERT INTO SOME_TABLE(COLUMN_NAME) VALUES ('VALUE')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));

    String actualOutput = executeWithParameters(pgServer.getLocalPort(), sql, "update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicDeleteWithParameters() throws IOException, InterruptedException {
    String sql = "DELETE FROM SOME_TABLE WHERE COLUMN_NAME = %s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("VALUE");

    String sql2 = "DELETE FROM SOME_TABLE WHERE COLUMN_NAME = 'VALUE'";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 12));

    String actualOutput = executeWithParameters(pgServer.getLocalPort(), sql, "update", parameters);
    String expectedOutput = "12\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParameters() throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 = %(NAME1)s and COLUMN_NAME2 = %(NAME2)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("VALUE1");
    parameters.add("NAME2");
    parameters.add("VALUE2");

    String sql2 =
        "SELECT * FROM some_table where COLUMN_NAME1 = 'VALUE1' and COLUMN_NAME2 = 'VALUE2'";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicUpdateWithNamedParameters() throws IOException, InterruptedException {
    String sql = "UPDATE SET column_name=%(NAME1)s where column_name2 = %(NAME2)s";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("NAME1");
    parameters.add("VALUE1");
    parameters.add("NAME2");
    parameters.add("VALUE2");

    String sql2 = "UPDATE SET column_name='VALUE1' where column_name2 = 'VALUE2'";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 10));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "update", parameters);
    String expectedOutput = "10\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicInsertWithNamedParameters() throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE(COLUMN_NAME) VALUES (%(NAME)s)";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME");
    parameters.add("VALUE");

    String sql2 = "INSERT INTO SOME_TABLE(COLUMN_NAME) VALUES ('VALUE')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicDeleteWithNamedParameters() throws IOException, InterruptedException {
    String sql = "DELETE FROM SOME_TABLE WHERE COLUMN_NAME = %(NAME)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME");
    parameters.add("VALUE");

    String sql2 = "DELETE FROM SOME_TABLE WHERE COLUMN_NAME = 'VALUE'";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 12));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "update", parameters);
    String expectedOutput = "12\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParametersIntDataType()
      throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 = %(NAME1)s and COLUMN_NAME2 = %(NAME2)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("int");
    parameters.add("2");
    parameters.add("NAME2");
    parameters.add("int");
    parameters.add("3");

    String sql2 = "SELECT * FROM some_table where COLUMN_NAME1 = 2 and COLUMN_NAME2 = 3";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "data_type_query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParametersBoolDataType()
      throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 = %(NAME1)s and COLUMN_NAME2 = %(NAME2)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("bool");
    parameters.add("True");
    parameters.add("NAME2");
    parameters.add("bool");
    parameters.add("False");

    String sql2 = "SELECT * FROM some_table where COLUMN_NAME1 = true and COLUMN_NAME2 = false";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "data_type_query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParametersFloatDataType()
      throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 = %(NAME1)s and COLUMN_NAME2 = %(NAME2)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("float");
    parameters.add("2.24");
    parameters.add("NAME2");
    parameters.add("float");
    parameters.add("3.14");

    String sql2 = "SELECT * FROM some_table where COLUMN_NAME1 = 2.24 and COLUMN_NAME2 = 3.14";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "data_type_query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParametersByteaDataType()
      throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 = %(NAME1)s and COLUMN_NAME2 = %(NAME2)s or COLUMN_NAME3 = %(NAME3)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("memoryview");
    parameters.add("b'VALUE1'");
    parameters.add("NAME2");
    parameters.add("bytearray");
    parameters.add("b'VALUE2'");
    parameters.add("NAME3");
    parameters.add("bytes");
    parameters.add("b'VALUE3'");

    String sql2 =
        "SELECT * FROM some_table where COLUMN_NAME1 = 'VALUE1'::bytea and COLUMN_NAME2 = 'VALUE2'::bytea or COLUMN_NAME3 = 'VALUE3'::bytea";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "data_type_query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParametersDateDataType()
      throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 = %(NAME1)s and COLUMN_NAME2 = %(NAME2)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("datetime.date");
    parameters.add("2022, 10, 2");
    parameters.add("NAME2");
    parameters.add("datetime.datetime");
    parameters.add("2019, 2, 3, 6, 30, 15, 0, pytz.UTC");

    String sql2 =
        "SELECT * FROM some_table where COLUMN_NAME1 = '2022-10-02'::date and COLUMN_NAME2 = '2019-02-03T06:30:15+00:00'::timestamptz";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "data_type_query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testBasicSelectWithNamedParametersTupleListDataType()
      throws IOException, InterruptedException {
    String sql =
        "SELECT * FROM some_table where COLUMN_NAME1 in %(NAME1)s and COLUMN_NAME2 = ANY(%(NAME2)s)";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("NAME1");
    parameters.add("tuple");
    parameters.add("(1 , 2, 3)");
    parameters.add("NAME2");
    parameters.add("list");
    parameters.add("[1, 2, 3]");

    String sql2 =
        "SELECT * FROM some_table where COLUMN_NAME1 in (1, 2, 3) and COLUMN_NAME2 = ANY(ARRAY[1,2,3])";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), createResultSet()));

    String actualOutput =
        executeWithNamedParameters(pgServer.getLocalPort(), sql, "data_type_query", parameters);
    String expectedOutput = "(1, 'abcd')\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }
}
