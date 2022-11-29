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
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class DjangoConditionalExpressionsTest extends DjangoTestSetup {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  private ResultSet createResultSet(List<String> rows, int numCols) {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    StructType.Builder builder = StructType.newBuilder();

    for (int i = 0; i < 2 * numCols; i += 2) {
      if (rows.get(i).equals("int")) {
        builder.addFields(
            Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                .setName(rows.get(i + 1))
                .build());

      } else if (rows.get(i).equals("string")) {
        builder.addFields(
            Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                .setName(rows.get(i + 1))
                .build());
      }
    }
    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder().setRowType(builder.build()).build());
    for (int i = 2 * numCols; i < rows.size(); i += numCols) {
      ListValue.Builder rowBuilder = ListValue.newBuilder();
      for (int j = i; j < i + numCols; ++j)
        rowBuilder.addValues(Value.newBuilder().setStringValue(rows.get(j)).build());

      resultSetBuilder.addRows(rowBuilder.build());
    }
    return resultSetBuilder.build();
  }

  @Test
  public void testConditionalUpdate() throws IOException, InterruptedException {
    String sqlUpdate =
        "UPDATE \"singers\" SET \"firstname\" = CASE WHEN (\"singers\".\"firstname\" = 'hello') THEN 'h' WHEN (\"singers\".\"firstname\" = 'world') THEN 'w' ELSE 'n' END";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlUpdate), 17));

    String expectedOutput = "17\n";
    List<String> options = new ArrayList<>();
    options.add("update");
    String actualOutput = executeConditionalTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlUpdate, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testConditionalAggregation() throws IOException, InterruptedException {
    String sqlSelect =
        "SELECT COUNT(\"singers\".\"singerid\") FILTER (WHERE \"singers\".\"firstname\" = 'hello') AS \"hello_count\", COUNT(\"singers\".\"singerid\") FILTER (WHERE \"singers\".\"firstname\" = 'world') AS \"world_count\" FROM \"singers\"";

    List<String> result = new ArrayList<>();

    result.add("int");
    result.add("hello_count");

    result.add("int");
    result.add("world_count");

    result.add("24");
    result.add("26");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelect), createResultSet(result, 2)));

    String expectedOutput = "{'hello_count': 24, 'world_count': 26}\n";
    List<String> options = new ArrayList<>();
    options.add("aggregation");
    String actualOutput = executeConditionalTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlSelect, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testConditionalFilter() throws IOException, InterruptedException {
    String sqlSelect =
        "SELECT \"singers\".\"singerid\", \"singers\".\"firstname\", \"singers\".\"lastname\" FROM \"singers\" WHERE UPPER(\"singers\".\"firstname\"::text) LIKE '%' || UPPER(REPLACE(REPLACE(REPLACE((CASE WHEN \"singers\".\"lastname\" = 'world' THEN 'w' WHEN \"singers\".\"lastname\" = 'hello' THEN 'h' ELSE NULL END), E'\\\\', E'\\\\\\\\'), E'%', E'\\\\%'), E'_', E'\\\\_')) || '%'";

    List<String> result = new ArrayList<>();

    result.add("int");
    result.add("singerid");

    result.add("string");
    result.add("firstname");

    result.add("string");
    result.add("lastname");

    result.add("1");
    result.add("Hero");
    result.add("hello");

    result.add("2");
    result.add("Wake");
    result.add("world");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelect), createResultSet(result, 3)));

    String expectedOutput = "1 Hero hello\n" + "2 Wake world\n";
    List<String> options = new ArrayList<>();
    options.add("filter");
    String actualOutput = executeConditionalTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlSelect, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testConditionalAnnotation() throws IOException, InterruptedException {
    String sqlSelect =
        "SELECT \"singers\".\"singerid\", \"singers\".\"firstname\", CASE WHEN UPPER(\"singers\".\"firstname\"::text) LIKE UPPER('%hello%') THEN 'hello singer' WHEN UPPER(\"singers\".\"firstname\"::text) LIKE UPPER('%world%') THEN 'world singer' ELSE NULL END AS \"type\" FROM \"singers\" LIMIT 21";

    List<String> result = new ArrayList<>();

    result.add("int");
    result.add("singerid");

    result.add("string");
    result.add("firstname");

    result.add("string");
    result.add("type");

    result.add("1");
    result.add("Mr Hello");
    result.add("hello singer");

    result.add("2");
    result.add("Wake up World");
    result.add("world singer");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelect), createResultSet(result, 3)));

    String expectedOutput =
        "<QuerySet [(1, 'Mr Hello', 'hello singer'), (2, 'Wake up World', 'world singer')]>\n";
    List<String> options = new ArrayList<>();
    options.add("annotation");
    String actualOutput = executeConditionalTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sqlSelect, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }
}
