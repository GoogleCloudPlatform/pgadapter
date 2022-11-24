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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
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
    String sqlSelectAll =
        "SELECT \"singers\".\"singerid\", \"singers\".\"firstname\", \"singers\".\"lastname\" FROM \"singers\"";

    List<String> result =
        new ArrayList<>(Arrays.asList("1", "hello", "world", "2", "hello", "django"));

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelectAll), createResultSet(result)));
    String expectedOutput =
        "{'singerid': 1, 'firstname': 'hello', 'lastname': 'world'}\n"
            + "{'singerid': 2, 'firstname': 'hello', 'lastname': 'django'}\n";
    List<String> options = new ArrayList<>(Arrays.asList("insert"));
    String actualOutput = executeAllTypeTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(
        sqlSelectAll, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());

  }



}
