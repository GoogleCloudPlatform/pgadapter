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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;

public class DjangoTestSetup extends AbstractMockServerTest {

  public static ResultSet createResultSet(List<String> rows, int numCols) {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    StructType.Builder builder = StructType.newBuilder();

    for (int i = 0; i < 2 * numCols; i += 2) {
      String colType = rows.get(i);
      if (colType.equals("int")) {
        builder.addFields(
            Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                .setName(rows.get(i + 1))
                .build());

      } else if (colType.equals("string")) {
        builder.addFields(
            Field.newBuilder()
                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                .setName(rows.get(i + 1))
                .build());
      } else if (colType.equals("array")) {
        builder.addFields(
            Field.newBuilder()
                .setType(
                    Type.newBuilder()
                        .setCode(TypeCode.ARRAY)
                        .setArrayElementType(Type.newBuilder().setCode(TypeCode.STRING))
                        .build())
                .setName(rows.get(i + 1))
                .build());
      }
    }
    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder().setRowType(builder.build()).build());
    for (int i = 2 * numCols; i < rows.size(); i += numCols) {
      ListValue.Builder rowBuilder = ListValue.newBuilder();
      for (int j = i; j < i + numCols; ++j) {
        if (rows.get(j).charAt(0) == '[') {
          String[] array = rows.get(j).substring(1, rows.get(j).length() - 1).split(",");

          ListValue.Builder listValueBuilder = ListValue.newBuilder();
          for (String arrElement : array) {
            listValueBuilder.addValues(Value.newBuilder().setStringValue(arrElement).build());
          }
          Value value = Value.newBuilder().setListValue(listValueBuilder.build()).build();
          rowBuilder.addValues(value);
        } else rowBuilder.addValues(Value.newBuilder().setStringValue(rows.get(j)).build());
      }
      resultSetBuilder.addRows(rowBuilder.build());
    }
    return resultSetBuilder.build();
  }

  @BeforeClass
  public static void createVirtualEnv() throws Exception {
    PythonTestUtil.createVirtualEnv(DJANGO_UNIT_PATH);
  }

  @BeforeClass
  public static void mockDriverInternalSql() {
    String unnecessarySql =
        "with "
            + PG_TYPE_PREFIX
            + "\nSELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'";

    String unnecessarySql2 =
        "with " + PG_TYPE_PREFIX + "\nSELECT typarray FROM pg_type WHERE typname = 'citext'";

    List<String> result1 = new ArrayList<>();
    result1.add("string");
    result1.add("some_column");

    ResultSet rs1 = createResultSet(result1, 1);

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(unnecessarySql), rs1));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(unnecessarySql2), rs1));
  }

  public static boolean isPythonAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] pythonCommand = new String[] {"python3", "--version"};
    builder.command(pythonCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  static final String DJANGO_UNIT_PATH = "./src/test/python/django";
  private static final String DJANGO_IT_PATH = "./samples/python/django";

  private static String execute(
      int port, String host, List<String> options, String testFileName, String djangoPath)
      throws Exception {
    File directory = new File(djangoPath);
    List<String> runCommand =
        new ArrayList<>(
            Arrays.asList(
                directory.getAbsolutePath() + "/venv/bin/python3",
                testFileName,
                host,
                Integer.toString(port)));
    runCommand.addAll(options);
    return PythonTestUtil.run(runCommand.toArray(new String[0]), djangoPath);
  }

  public String executeBasicTests(int port, String host, List<String> options) throws Exception {
    return execute(port, host, options, "basic_test.py", DJANGO_UNIT_PATH);
  }

  public String executeTransactionTests(int port, String host, List<String> options)
      throws Exception {
    return execute(port, host, options, "transaction_test.py", DJANGO_UNIT_PATH);
  }

  public String executePgAggregateTests(int port, String host, List<String> options)
      throws Exception {
    return execute(port, host, options, "pg_aggregates_test.py", DJANGO_UNIT_PATH);
  }

  public String executeConditionalTests(int port, String host, List<String> options)
      throws Exception {
    return execute(port, host, options, "conditional_expressions_test.py", DJANGO_UNIT_PATH);
  }

  public String executeAggregationTests(int port, String host, List<String> options)
      throws Exception {
    return execute(port, host, options, "aggregation_tests.py", DJANGO_UNIT_PATH);
  }

  public String executeIntegrationTests() throws Exception {
    return execute(0, "", Collections.emptyList(), "sample.py", DJANGO_IT_PATH);
  }
}
