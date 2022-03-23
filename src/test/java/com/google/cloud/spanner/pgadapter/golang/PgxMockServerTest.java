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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.common.base.Preconditions;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests PGAdapter using the native Go pgx driver. The Go code can be found in
 * src/test/golang/pgx.go.
 */
@Category(GolangTest.class)
@RunWith(JUnit4.class)
public class PgxMockServerTest extends AbstractMockServerTest {
  public static class GoString extends Structure implements Structure.ByValue {
    // JNA does not allow these fields to be final.
    public String p;
    public long n;

    public GoString(String p) {
      this.p = Preconditions.checkNotNull(p);
      this.n = p.length();
    }

    protected List<String> getFieldOrder() {
      return Arrays.asList("p", "n");
    }
  }

  public interface PgxTest extends Library {
    String TestHelloWorld(GoString connString);

    String TestSelect1(GoString connString);

    String TestQueryWithParameter(GoString connString);
  }

  private static PgxTest pgxTest;

  @BeforeClass
  public static void compile() throws IOException, InterruptedException {
    // Compile the Go code to ensure that we always have the most recent test code.
    ProcessBuilder builder = new ProcessBuilder();
    String[] compileCommand = "go build -o pgx_test.so -buildmode=c-shared pgx.go".split(" ");
    builder.command(compileCommand);
    builder.directory(new File("./src/test/golang"));
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    // We explicitly use the full path to force JNA to look in a specific directory, instead of in
    // standard library directories.
    String currentPath = new java.io.File(".").getCanonicalPath();
    pgxTest =
        Native.load(String.format("%s/src/test/golang/pgx_test.so", currentPath), PgxTest.class);
  }

  private GoString createConnString() {
    return new GoString(
        String.format("postgres://uid:pwd@localhost:%d/test-db", pgServer.getLocalPort()));
  }

  @Test
  public void testHelloWorld() {
    String sql = "select 'Hello world!' as hello";

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("hello")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("Hello world!").build())
                        .build())
                .build()));

    String res = pgxTest.TestHelloWorld(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx sends the query three times:
    // 1. DESCRIBE statement
    // 2. DESCRIBE portal
    // 3. EXECUTE portal
    assertEquals(3, requests.size());
    int index = 0;
    for (ExecuteSqlRequest request : requests) {
      assertEquals(sql, request.getSql());
      if (index < 2) {
        assertEquals(QueryMode.PLAN, request.getQueryMode());
      } else {
        assertEquals(QueryMode.NORMAL, request.getQueryMode());
      }
      index++;
    }
  }

  @Test
  public void testSelect1() {
    String sql = "SELECT 1";

    String res = pgxTest.TestSelect1(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx sends the query three times:
    // 1. DESCRIBE statement
    // 2. DESCRIBE portal
    // 3. EXECUTE portal
    assertEquals(3, requests.size());
    int index = 0;
    for (ExecuteSqlRequest request : requests) {
      assertEquals(sql, request.getSql());
      if (index < 2) {
        assertEquals(QueryMode.PLAN, request.getQueryMode());
      } else {
        assertEquals(QueryMode.NORMAL, request.getQueryMode());
      }
      index++;
    }
  }

  @Test
  public void testQueryWithParameter() {
    String sql = "SELECT * FROM FOO WHERE BAR=$1";
    Statement statement = Statement.newBuilder(sql).bind("p1").to("baz").build();

    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
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

    String res = pgxTest.TestQueryWithParameter(createConnString());

    assertNull(res);
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    // pgx sends the query three times:
    // 1. DESCRIBE statement
    // 2. DESCRIBE portal
    // 3. EXECUTE portal
    assertEquals(3, requests.size());
    int index = 0;
    for (ExecuteSqlRequest request : requests) {
      assertEquals(sql, request.getSql());
      if (index < 2) {
        assertEquals(QueryMode.PLAN, request.getQueryMode());
      } else {
        assertEquals(QueryMode.NORMAL, request.getQueryMode());
      }
      index++;
    }
  }
}
