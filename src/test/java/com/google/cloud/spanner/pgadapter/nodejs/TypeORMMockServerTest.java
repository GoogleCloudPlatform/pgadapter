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
import static org.junit.Assert.fail;

import com.google.cloud.Timestamp;
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
import java.io.IOException;
import java.util.List;
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
    NodeJSTest.installDependencies("typeorm/data-test");
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

    String output = runTest("findOneUser", pgServer.getLocalPort());

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
    // There is however no guarantee that the server will see the Sync message in time to do this
    // optimization, so in some cases the single query will be using a read/write transaction.
    int commitRequestCount = mockSpanner.countRequestsOfType(CommitRequest.class);
    if (commitRequestCount == 0) {
      assertTrue(request.getTransaction().hasSingleUse());
      assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    } else if (commitRequestCount == 1) {
      assertTrue(request.getTransaction().hasBegin());
      assertTrue(request.getTransaction().getBegin().hasReadWrite());
    } else {
      fail("Invalid commit count: " + commitRequestCount);
    }
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

    String output = runTest("createUser", pgServer.getLocalPort());

    assertEquals("\n\nFound user 1 with name Timber Saw\n", output);

    // Creating the user will use a read/write transaction. The query that checks whether the record
    // already exists will however not use that transaction, as each statement is executed in
    // auto-commit mode.
    int expectedCommitCount = 0;
    List<ExecuteSqlRequest> checkExistsRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(existsSql))
            .collect(Collectors.toList());
    assertEquals(1, checkExistsRequests.size());
    ExecuteSqlRequest checkExistsRequest = checkExistsRequests.get(0);
    if (checkExistsRequest.getTransaction().hasSingleUse()) {
      assertTrue(checkExistsRequest.getTransaction().getSingleUse().hasReadOnly());
    } else if (checkExistsRequest.getTransaction().hasBegin()) {
      assertTrue(checkExistsRequest.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + checkExistsRequest.getTransaction());
    }

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(1, insertRequests.size());
    ExecuteSqlRequest insertRequest = insertRequests.get(0);
    assertTrue(insertRequest.getTransaction().hasBegin());
    assertTrue(insertRequest.getTransaction().getBegin().hasReadWrite());
    expectedCommitCount++;

    // Loading the user after having saved it will be done in a single-use read-only transaction.
    List<ExecuteSqlRequest> loadRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, loadRequests.size());
    ExecuteSqlRequest loadRequest = loadRequests.get(0);
    if (loadRequest.getTransaction().hasSingleUse()) {
      assertTrue(loadRequest.getTransaction().getSingleUse().hasReadOnly());
    } else if (loadRequest.getTransaction().hasBegin()) {
      assertTrue(loadRequest.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + loadRequest.getTransaction());
    }
    assertEquals(expectedCommitCount, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testUpdateUser() throws IOException, InterruptedException {
    String loadSql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE (\"User\".\"id\" = $1) LIMIT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(loadSql),
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
    String existsSql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE \"User\".\"id\" IN ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(existsSql),
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

    String updateSql =
        "UPDATE \"user\" SET \"firstName\" = $1, \"lastName\" = $2, \"age\" = $3 WHERE \"id\" IN ($4)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSql), 1L));

    String output = runTest("updateUser", pgServer.getLocalPort());

    assertEquals("\n\nUpdated user 1\n", output);

    // Updating the user will use a read/write transaction. The query that checks whether the record
    // already exists will however not use that transaction, as each statement is executed in
    // auto-commit mode.
    int expectedCommitCount = 0;
    List<ExecuteSqlRequest> loadRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(loadSql))
            .collect(Collectors.toList());
    assertEquals(1, loadRequests.size());
    ExecuteSqlRequest loadRequest = loadRequests.get(0);
    if (loadRequest.getTransaction().hasSingleUse()) {
      assertTrue(loadRequest.getTransaction().getSingleUse().hasReadOnly());
    } else if (loadRequest.getTransaction().hasBegin()) {
      assertTrue(loadRequest.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + loadRequest.getTransaction());
    }

    List<ExecuteSqlRequest> checkExistsRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(existsSql))
            .collect(Collectors.toList());
    assertEquals(1, checkExistsRequests.size());
    ExecuteSqlRequest checkExistsRequest = checkExistsRequests.get(0);
    if (checkExistsRequest.getTransaction().hasSingleUse()) {
      assertTrue(checkExistsRequest.getTransaction().getSingleUse().hasReadOnly());
    } else if (checkExistsRequest.getTransaction().hasBegin()) {
      assertTrue(checkExistsRequest.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + checkExistsRequest.getTransaction());
    }

    List<ExecuteSqlRequest> updateRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(updateSql))
            .collect(Collectors.toList());
    assertEquals(1, updateRequests.size());
    ExecuteSqlRequest updateRequest = updateRequests.get(0);
    assertTrue(updateRequest.getTransaction().hasBegin());
    assertTrue(updateRequest.getTransaction().getBegin().hasReadWrite());
    expectedCommitCount++;

    assertEquals(expectedCommitCount, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testDeleteUser() throws IOException, InterruptedException {
    String loadSql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE (\"User\".\"id\" = $1) LIMIT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(loadSql),
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
    String existsSql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE \"User\".\"id\" IN ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(existsSql),
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

    String deleteSql = "DELETE FROM \"user\" WHERE \"id\" = $1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(deleteSql), 1L));

    String output = runTest("deleteUser", pgServer.getLocalPort());

    assertEquals("\n\nDeleted user 1\n", output);

    // Deleting the user will use a read/write transaction. The query that checks whether the record
    // already exists will however not use that transaction, as each statement is executed in
    // auto-commit mode.
    int expectedCommitCount = 0;
    List<ExecuteSqlRequest> loadRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(loadSql))
            .collect(Collectors.toList());
    assertEquals(1, loadRequests.size());
    ExecuteSqlRequest loadRequest = loadRequests.get(0);
    if (loadRequest.getTransaction().hasSingleUse()) {
      assertTrue(loadRequest.getTransaction().getSingleUse().hasReadOnly());
    } else if (loadRequest.getTransaction().hasBegin()) {
      assertTrue(loadRequest.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + loadRequest.getTransaction());
    }

    List<ExecuteSqlRequest> checkExistsRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(existsSql))
            .collect(Collectors.toList());
    assertEquals(1, checkExistsRequests.size());
    ExecuteSqlRequest checkExistsRequest = checkExistsRequests.get(0);
    if (checkExistsRequest.getTransaction().hasSingleUse()) {
      assertTrue(checkExistsRequest.getTransaction().getSingleUse().hasReadOnly());
    } else if (checkExistsRequest.getTransaction().hasBegin()) {
      assertTrue(checkExistsRequest.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + checkExistsRequest.getTransaction());
    }

    List<ExecuteSqlRequest> deleteRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(deleteSql))
            .collect(Collectors.toList());
    assertEquals(1, deleteRequests.size());
    ExecuteSqlRequest deleteRequest = deleteRequests.get(0);
    assertTrue(deleteRequest.getTransaction().hasBegin());
    assertTrue(deleteRequest.getTransaction().getBegin().hasReadWrite());
    expectedCommitCount++;

    assertEquals(expectedCommitCount, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testFindOneAllTypes() throws IOException, InterruptedException {
    String sql =
        "SELECT \"AllTypes\".\"col_bigint\" AS \"AllTypes_col_bigint\", \"AllTypes\".\"col_bool\" AS \"AllTypes_col_bool\", "
            + "\"AllTypes\".\"col_bytea\" AS \"AllTypes_col_bytea\", \"AllTypes\".\"col_float8\" AS \"AllTypes_col_float8\", "
            + "\"AllTypes\".\"col_int\" AS \"AllTypes_col_int\", \"AllTypes\".\"col_numeric\" AS \"AllTypes_col_numeric\", "
            + "\"AllTypes\".\"col_timestamptz\" AS \"AllTypes_col_timestamptz\", \"AllTypes\".\"col_date\" AS \"AllTypes_col_date\", "
            + "\"AllTypes\".\"col_varchar\" AS \"AllTypes_col_varchar\" "
            + "FROM \"all_types\" \"AllTypes\" "
            + "WHERE (\"AllTypes\".\"col_bigint\" = $1) LIMIT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("AllTypes_")));

    String output = runTest("findOneAllTypes", pgServer.getLocalPort());

    assertEquals(
        "\n\nFound row 1\n"
            + "AllTypes {\n"
            + "  col_bigint: '1',\n"
            + "  col_bool: true,\n"
            + "  col_bytea: <Buffer 74 65 73 74>,\n"
            + "  col_float8: 3.14,\n"
            + "  col_int: '100',\n"
            + "  col_numeric: '6.626',\n"
            + "  col_timestamptz: 2022-02-16T13:18:02.123Z,\n"
            + "  col_date: '2022-03-29',\n"
            + "  col_varchar: 'test'\n"
            + "}\n",
        output);

    int expectedCommitCount = 0;
    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    if (request.getTransaction().hasSingleUse()) {
      assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
    } else if (request.getTransaction().hasBegin()) {
      assertTrue(request.getTransaction().getBegin().hasReadWrite());
      expectedCommitCount++;
    } else {
      fail("missing or invalid transaction option: " + request.getTransaction());
    }
    assertEquals(expectedCommitCount, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testCreateAllTypes() throws IOException, InterruptedException {
    String existsSql =
        "SELECT \"AllTypes\".\"col_bigint\" AS \"AllTypes_col_bigint\", \"AllTypes\".\"col_bool\" AS \"AllTypes_col_bool\", "
            + "\"AllTypes\".\"col_bytea\" AS \"AllTypes_col_bytea\", \"AllTypes\".\"col_float8\" AS \"AllTypes_col_float8\", "
            + "\"AllTypes\".\"col_int\" AS \"AllTypes_col_int\", \"AllTypes\".\"col_numeric\" AS \"AllTypes_col_numeric\", "
            + "\"AllTypes\".\"col_timestamptz\" AS \"AllTypes_col_timestamptz\", \"AllTypes\".\"col_date\" AS \"AllTypes_col_date\", "
            + "\"AllTypes\".\"col_varchar\" AS \"AllTypes_col_varchar\" "
            + "FROM \"all_types\" \"AllTypes\" "
            + "WHERE \"AllTypes\".\"col_bigint\" IN ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(existsSql),
            ResultSet.newBuilder()
                .setMetadata(createAllTypesResultSetMetadata("AllTypes_"))
                .build()));

    String insertSql =
        "INSERT INTO \"all_types\""
            + "(\"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\") "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSql), 1L));

    String output = runTest("createAllTypes", pgServer.getLocalPort());

    assertEquals("\n\nCreated one record\n", output);

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(1, insertRequests.size());
    ExecuteSqlRequest insertRequest = insertRequests.get(0);
    assertTrue(insertRequest.getTransaction().hasBegin());
    assertTrue(insertRequest.getTransaction().getBegin().hasReadWrite());

    // The NodeJS PostgreSQL driver sends parameters without any type information to the backend.
    // This means that all parameters are sent as untyped string values.
    assertEquals(0, insertRequest.getParamTypesMap().size());
    assertEquals(9, insertRequest.getParams().getFieldsCount());
    assertEquals("2", insertRequest.getParams().getFieldsMap().get("p1").getStringValue());
    assertEquals("true", insertRequest.getParams().getFieldsMap().get("p2").getStringValue());
    assertEquals(
        "c29tZSByYW5kb20gc3RyaW5n",
        insertRequest.getParams().getFieldsMap().get("p3").getStringValue());
    assertEquals(
        "0.123456789", insertRequest.getParams().getFieldsMap().get("p4").getStringValue());
    assertEquals("123456789", insertRequest.getParams().getFieldsMap().get("p5").getStringValue());
    assertEquals("234.54235", insertRequest.getParams().getFieldsMap().get("p6").getStringValue());
    assertEquals(
        Timestamp.parseTimestamp("2022-07-22T20:15:42.011+02:00"),
        Timestamp.parseTimestamp(
            insertRequest.getParams().getFieldsMap().get("p7").getStringValue()));
    assertEquals("2022-07-22", insertRequest.getParams().getFieldsMap().get("p8").getStringValue());
    assertEquals(
        "some random string", insertRequest.getParams().getFieldsMap().get("p9").getStringValue());
  }

  static String runTest(String testName, int port) throws IOException, InterruptedException {
    return NodeJSTest.runTest("typeorm/data-test", testName, "localhost", port, "db");
  }
}
