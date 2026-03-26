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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgAttribute;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgNamespace;
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
import java.io.File;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class AdbcMockServerTest extends AbstractMockServerTest {
  static final String DIRECTORY_NAME = "./src/test/python/adbc_driver_postgresql";

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
    if ("localhost".equals(host)) {
      return String.format("postgresql://localhost:%d/d?sslmode=disable", pgServer.getLocalPort());
    } else {
      // For unix socket, let's see if ADBC supports it. Standard URI format for unix socket is not
      // perfectly standardized.
      // Let's try to pass the path as a query parameter or similar if the driver supports it, or
      // just use host=... if it accepts it.
      // If it fails, we will find out.
      return String.format(
          "postgresql://localhost:%d/d?sslmode=disable&host=%s", pgServer.getLocalPort(), host);
    }
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
            + PgCatalog.PG_TYPE_CTE_EMULATED
            + "\n"
            + "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM pg_type  WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND typreceive::TEXT != 'array_recv'";
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
    // Depending on ADBC output, it might be (1,) or something similar.
    // Let's verify what it is. For psycopg3 it was (1,).
    // Let's assume it is similar.
    String expectedOutput = "(b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01',)\n";
    assertEquals(expectedOutput, actualOutput);

    ExecuteSqlRequest request =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(r -> r.getSql().equals(sql))
            .findFirst()
            .orElse(null);
    assertNotNull("No ExecuteSqlRequest found for " + sql, request);
    assertEquals(sql, request.getSql());
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
    // We expect it to be a tuple with bytes or string. Let's see.
    // Assuming binary protocol, it might be (b'foo',). Or just ('foo',).
    // Let's print actual output in the failure message if it fails.
    String expectedOutput = "(b'foo',)\n"; // Assuming bytes as a starting point.
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
    // We expect it to be a tuple with bytes or boolean. Let's see.
    // Assuming binary protocol, it might be (b'\x01',). Or just (True,).
    // Let's print actual output in the failure message if it fails.
    String expectedOutput = "(b'\\x01',)\n";
    // Let's guess (True,) first, if it fails we will know the exact format.
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
    // We expect it to be a tuple with bytes or datetime. Let's see.
    // Assuming binary protocol, it might be bytes.
    // Let's print actual output in the failure message if it fails.
    String expectedOutput = "(b'\\x00\\x02>\\x07\\x86\\xc2`\\x00',)\n";
    assertEquals(expectedOutput, actualOutput);
  }
}
