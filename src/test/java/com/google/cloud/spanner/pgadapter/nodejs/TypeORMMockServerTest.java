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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(NodeJSTest.class)
@RunWith(JUnit4.class)
public class TypeORMMockServerTest extends AbstractMockServerTest {
  private static final ResultSetMetadata USERS_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("User_id")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("User_firstName")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("User_lastName")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("User_age")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/nodejs/typeorm/data-test", currentPath);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command("npm", "install");
    builder.directory(new File(testFilePath));

    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);
  }

  @Test
  public void testFindOneUser() throws IOException, InterruptedException {
    String sql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE (\"User\".\"id\" = $1) LIMIT 1";
    // The parameter is sent as an untyped parameter, and therefore not included in the statement
    // lookup on the mock server, hence the Statement.of(sql) instead of building a statement that
    // does include the parameter value.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(USERS_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Timber").build())
                        .addValues(Value.newBuilder().setStringValue("Saw").build())
                        .addValues(Value.newBuilder().setStringValue("25").build())
                        .build())
                .build()));

    String output = runTest("findOneUser");

    assertEquals("\n\nFound user 1 with name Timber Saw\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    // The TypeORM PostgreSQL driver sends both a Flush and a Sync message. The Flush message does
    // a look-ahead to determine if the next message is a Sync, and if it is, executes a Sync on the
    // backend connection. This is a lot more efficient, as it means that we can use single-use
    // read-only transactions for single queries.
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testCreateUser() throws IOException, InterruptedException {
    String existsSql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE \"User\".\"id\" IN ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(existsSql), ResultSet.newBuilder().setMetadata(USERS_METADATA).build()));
    String insertSql =
        "INSERT INTO \"user\"(\"id\", \"firstName\", \"lastName\", \"age\") VALUES ($1, $2, $3, $4)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSql), 1L));

    String sql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE (\"User\".\"firstName\" = $1 AND \"User\".\"lastName\" = $2) "
            + "LIMIT 1";
    // The parameter is sent as an untyped parameter, and therefore not included in the statement
    // lookup on the mock server, hence the Statement.of(sql) instead of building a statement that
    // does include the parameter value.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(USERS_METADATA)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Timber").build())
                        .addValues(Value.newBuilder().setStringValue("Saw").build())
                        .addValues(Value.newBuilder().setStringValue("25").build())
                        .build())
                .build()));

    String output = runTest("createUser");

    assertEquals("\n\nFound user 1 with name Timber Saw\n", output);

    // Creating the user will use a read/write transaction. The query that checks whether the record
    // already exists will however not use that transaction, as each statement is executed in
    // auto-commit mode.
    List<ExecuteSqlRequest> checkExistsRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(existsSql))
            .collect(Collectors.toList());
    assertEquals(1, checkExistsRequests.size());
    ExecuteSqlRequest checkExistsRequest = checkExistsRequests.get(0);
    assertTrue(checkExistsRequest.getTransaction().hasSingleUse());
    assertTrue(checkExistsRequest.getTransaction().getSingleUse().hasReadOnly());
    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(1, insertRequests.size());
    ExecuteSqlRequest insertRequest = insertRequests.get(0);
    assertTrue(insertRequest.getTransaction().hasBegin());
    assertTrue(insertRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));

    // Loading the user after having saved it will be done in a single-use read-only transaction.
    List<ExecuteSqlRequest> loadRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, loadRequests.size());
    ExecuteSqlRequest loadRequest = loadRequests.get(0);
    assertTrue(loadRequest.getTransaction().hasSingleUse());
    assertTrue(loadRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  static String runTest(String testName) throws IOException, InterruptedException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/nodejs/typeorm/data-test", currentPath);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command("npm", "start", testName, String.format("%d", pgServer.getLocalPort()));
    builder.directory(new File(testFilePath));

    Process process = builder.start();
    InputStream inputStream = process.getInputStream();
    InputStream errorStream = process.getErrorStream();
    int res = process.waitFor();

    String output = readAll(inputStream);
    String errors = readAll(errorStream);
    assertEquals("", errors);
    assertEquals(0, res);

    return output;
  }

  static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        // Skip lines that are generated by npm / node.
        if (!line.startsWith(">")) {
          result.append(line).append("\n");
        }
      }
    }
    return result.toString();
  }
}
