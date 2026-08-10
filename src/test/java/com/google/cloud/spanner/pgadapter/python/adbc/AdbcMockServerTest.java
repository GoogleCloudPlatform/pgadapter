// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.python.adbc;

import static com.google.cloud.spanner.pgadapter.python.PythonTestUtil.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgAttribute;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgNamespace;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PythonTest.class)
public class AdbcMockServerTest extends AbstractMockServerTest {
  static final String DIRECTORY_NAME = "./src/test/python/adbc_driver_postgresql";

  @BeforeClass
  public static void createVirtualEnv() throws Exception {
    PythonTestUtil.createVirtualEnv(DIRECTORY_NAME);
  }

  String createConnectionString() {
    return String.format("postgresql://localhost:%d/d?sslmode=disable", pgServer.getLocalPort());
  }

  String execute(String method) throws Exception {
    return execute(method, createConnectionString());
  }

  static String execute(String method, String connectionString) throws Exception {
    File directory = new File(DIRECTORY_NAME);
    return run(
        new String[] {
          directory.getAbsolutePath() + "/venv/bin/python3",
          "adbc_tests.py",
          method,
          connectionString
        },
        DIRECTORY_NAME);
  }

  private ResultSet createResultSet() {
    return ResultSet.newBuilder()
        .setMetadata(
            ResultSetMetadata.newBuilder()
                .setRowType(
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .setName("")
                                .build())
                        .build())
                .build())
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue(String.valueOf(1)).build())
                .build())
        .build();
  }

  @Before
  public void setupStartupQueries() {
    mockStartupQueries();
  }

  private void mockStartupQueries() {
    String sql =
        "with "
            + PgAttribute.PG_ATTRIBUTE_CTE
            + "\n\n"
            + "SELECT\n"
            + "    attrelid,\n"
            + "    attname,\n"
            + "    atttypid\n"
            + "FROM\n"
            + "    pg_attribute\n"
            + "ORDER BY\n"
            + "    attrelid, attnum\n";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createEmptyAttributeResultSet()));

    String typeSql =
        "with "
            + PgNamespace.PG_NAMESPACE_CTE
            + ",\n"
            + PgCatalog.PgType.PG_TYPE_CTE
            + "\n"
            + "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM pg_type WHERE (typreceive != '0'::varchar OR typsend != '0'::varchar) AND typtype != 'r' AND typreceive::TEXT != 'array_recv'";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(typeSql), createEmptyTypeResultSet()));
  }

  private ResultSet createEmptyTypeResultSet() {
    return ResultSet.newBuilder()
        .setMetadata(
            ResultSetMetadata.newBuilder()
                .setRowType(
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .setName("oid")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                .setName("typname")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                .setName("typreceive")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .setName("typbasetype")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                .setName("typrelid")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .setName("typarray")
                                .build())
                        .build())
                .build())
        .build();
  }

  private ResultSet createEmptyAttributeResultSet() {
    return ResultSet.newBuilder()
        .setMetadata(
            ResultSetMetadata.newBuilder()
                .setRowType(
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                .setName("attrelid")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                .setName("attname")
                                .build())
                        .addFields(
                            Field.newBuilder()
                                .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                .setName("atttypid")
                                .build())
                        .build())
                .build())
        .build();
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), createResultSet()));

    String actualOutput = execute("select1");
    // ADBC by default uses the binary protocol and returns the binary data
    // when we just fetch it like this.
    String expectedOutput = "(b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01',)\n";
    assertEquals(expectedOutput, actualOutput);

    ExecuteSqlRequest request =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(r -> r.getSql().equals(sql))
            .findFirst()
            .orElse(null);
    assertNotNull("No ExecuteSqlRequest found for " + sql, request);
    assertEquals(sql, request.getSql());

    assertTrue(
        "ADBC client should be detected and its startup query rewritten",
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .anyMatch(r -> r.getSql().contains("typreceive != '0'::varchar")));
  }

  @Test
  public void testSelectString() throws Exception {
    String sql = "SELECT 'foo'";

    com.google.spanner.v1.ResultSet resultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                ResultSetMetadata.newBuilder()
                    .setRowType(
                        StructType.newBuilder()
                            .addFields(
                                Field.newBuilder()
                                    .setName("C")
                                    .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                    .build())
                            .build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("foo").build())
                    .build())
            .build();

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), resultSet));

    String actualOutput = execute("select_string");
    String expectedOutput = "(b'foo',)\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testSelectBoolean() throws Exception {
    String sql = "SELECT true";

    com.google.spanner.v1.ResultSet resultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                ResultSetMetadata.newBuilder()
                    .setRowType(
                        StructType.newBuilder()
                            .addFields(
                                Field.newBuilder()
                                    .setName("C")
                                    .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                                    .build())
                            .build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setBoolValue(true).build())
                    .build())
            .build();

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), resultSet));

    String actualOutput = execute("select_boolean");
    String expectedOutput = "(b'\\x01',)\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testSelectTimestamp() throws Exception {
    String sql = "SELECT '2020-01-01T00:00:00Z'::timestamp";

    com.google.spanner.v1.ResultSet resultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                ResultSetMetadata.newBuilder()
                    .setRowType(
                        StructType.newBuilder()
                            .addFields(
                                Field.newBuilder()
                                    .setName("C")
                                    .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                    .build())
                            .build())
                    .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("2020-01-01T00:00:00Z").build())
                    .build())
            .build();

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), resultSet));

    String actualOutput = execute("select_timestamp");
    String expectedOutput = "(b'\\x00\\x02>\\x07\\x86\\xc2`\\x00',)\n";
    assertEquals(expectedOutput, actualOutput);
  }
}
