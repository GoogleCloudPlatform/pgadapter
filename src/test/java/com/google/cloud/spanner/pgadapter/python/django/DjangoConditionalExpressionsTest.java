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
public class DjangoConditionalExpressionsTest extends DjangoTestSetup {

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
  public void testConditionalUpdate() throws IOException, InterruptedException {
    String sqlUpdate = "UPDATE \"singers\" SET \"firstname\" = CASE WHEN (\"singers\".\"firstname\" = 'hello') THEN 'h' WHEN (\"singers\".\"firstname\" = 'world') THEN 'w' ELSE 'n' END";

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sqlUpdate), 17));


    String expectedOutput = "17\n";
    List<String> options = new ArrayList<>();
    options.add("update");
    String actualOutput = executeConditionalTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(
        sqlUpdate, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
    }

  @Test
  public void testConditionalAggregation() throws IOException, InterruptedException {
    String sqlSelect = "SELECT COUNT(\"singers\".\"singerid\") FILTER (WHERE \"singers\".\"firstname\" = 'hello') AS \"hello_count\", COUNT(\"singers\".\"singerid\") FILTER (WHERE \"singers\".\"firstname\" = 'world') AS \"world_count\" FROM \"singers\"";

    List<String> result = new ArrayList<>();

    result.add()

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sqlSelect), createResultSet(result)));


    String expectedOutput = "17\n";
    List<String> options = new ArrayList<>();
    options.add("aggregation");
    String actualOutput = executeConditionalTests(pgServer.getLocalPort(), host, options);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(
        sqlUpdate, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }



}
