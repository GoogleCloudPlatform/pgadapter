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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
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
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
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
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    // The TypeORM PostgreSQL driver sends both a Flush and a Sync message. The Flush message does
    // a look-ahead to determine if the next message is a Sync, and if it is, executes a Sync on the
    // backend connection. This is a lot more efficient, as it means that we can use a read-only
    // transaction for transactions that only contains queries.
    // There is however no guarantee that the server will see the Sync message in time to do this
    // optimization, so in some cases the single query will be using a read/write transaction, as we
    // don't know what might be following the current query.
    // This behavior in node-postgres has been fixed in
    // https://github.com/brianc/node-postgres/pull/2842,
    // but has not yet been released.
    int commitRequestCount = mockSpanner.countRequestsOfType(CommitRequest.class);
    if (commitRequestCount == 0) {
      assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
      assertTrue(
          mockSpanner
              .getRequestsOfType(BeginTransactionRequest.class)
              .get(0)
              .getOptions()
              .hasReadOnly());
      assertTrue(describeRequest.getTransaction().hasId());
      assertTrue(executeRequest.getTransaction().hasId());
    } else if (commitRequestCount == 1) {
      assertTrue(describeRequest.getTransaction().hasBegin());
      assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());
      assertTrue(executeRequest.getTransaction().hasId());
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
            Statement.of(existsSql),
            ResultSet.newBuilder()
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(existsSql).bind("p1").to(1L).build(),
            ResultSet.newBuilder().setMetadata(USERS_METADATA).build()));
    String insertSql =
        "INSERT INTO \"user\"(\"id\", \"firstName\", \"lastName\", \"age\") VALUES ($1, $2, $3, $4)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64, TypeCode.STRING, TypeCode.STRING, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to("Timber")
                .bind("p3")
                .to("Saw")
                .bind("p4")
                .to(25L)
                .build(),
            1L));

    String sql =
        "SELECT \"User\".\"id\" AS \"User_id\", \"User\".\"firstName\" AS \"User_firstName\", "
            + "\"User\".\"lastName\" AS \"User_lastName\", \"User\".\"age\" AS \"User_age\" "
            + "FROM \"user\" \"User\" WHERE (\"User\".\"firstName\" = $1 AND \"User\".\"lastName\" = $2) "
            + "LIMIT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p2")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("Timber").bind("p2").to("Saw").build(),
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
    assertEquals(2, checkExistsRequests.size());
    ExecuteSqlRequest describeCheckExistsRequest = checkExistsRequests.get(0);
    ExecuteSqlRequest executeCheckExistsRequest = checkExistsRequests.get(1);
    assertEquals(QueryMode.PLAN, describeCheckExistsRequest.getQueryMode());
    assertEquals(QueryMode.NORMAL, executeCheckExistsRequest.getQueryMode());
    if (describeCheckExistsRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
    }

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(2, insertRequests.size());
    ExecuteSqlRequest describeInsertRequest = insertRequests.get(0);
    assertEquals(QueryMode.PLAN, describeInsertRequest.getQueryMode());
    assertTrue(describeInsertRequest.getTransaction().hasBegin());
    assertTrue(describeInsertRequest.getTransaction().getBegin().hasReadWrite());
    ExecuteSqlRequest executeInsertRequest = insertRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeInsertRequest.getQueryMode());
    assertTrue(executeInsertRequest.getTransaction().hasId());
    expectedCommitCount++;

    // Loading the user after having saved it will be done in a single-use read-only transaction.
    List<ExecuteSqlRequest> loadRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, loadRequests.size());
    ExecuteSqlRequest describeLoadRequest = loadRequests.get(0);
    assertEquals(QueryMode.PLAN, describeLoadRequest.getQueryMode());
    ExecuteSqlRequest executeLoadRequest = loadRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeLoadRequest.getQueryMode());
    if (describeLoadRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
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
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(loadSql).bind("p1").to(1L).build(),
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
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(existsSql).bind("p1").to(1L).build(),
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
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.STRING, TypeCode.STRING, TypeCode.INT64, TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(updateSql)
                .bind("p1")
                .to("Lumber")
                .bind("p2")
                .to("Jack")
                .bind("p3")
                .to(45L)
                .bind("p4")
                .to(1L)
                .build(),
            1L));

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
    assertEquals(2, loadRequests.size());
    ExecuteSqlRequest describeLoadRequest = loadRequests.get(0);
    assertEquals(QueryMode.PLAN, describeLoadRequest.getQueryMode());
    ExecuteSqlRequest executeLoadRequest = loadRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeLoadRequest.getQueryMode());
    if (describeLoadRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
    }

    List<ExecuteSqlRequest> checkExistsRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(existsSql))
            .collect(Collectors.toList());
    assertEquals(2, checkExistsRequests.size());
    ExecuteSqlRequest describeCheckExistsRequest = checkExistsRequests.get(0);
    assertEquals(QueryMode.PLAN, describeCheckExistsRequest.getQueryMode());
    ExecuteSqlRequest executeCheckExistsRequest = checkExistsRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeCheckExistsRequest.getQueryMode());
    if (describeCheckExistsRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
    }

    List<ExecuteSqlRequest> updateRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(updateSql))
            .collect(Collectors.toList());
    assertEquals(2, updateRequests.size());
    ExecuteSqlRequest describeUpdateRequest = updateRequests.get(0);
    ExecuteSqlRequest executeUpdateRequest = updateRequests.get(1);
    assertTrue(describeUpdateRequest.getTransaction().hasBegin());
    assertTrue(describeUpdateRequest.getTransaction().getBegin().hasReadWrite());
    assertTrue(executeUpdateRequest.getTransaction().hasId());
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
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(loadSql).bind("p1").to(1L).build(),
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
                .setMetadata(
                    USERS_METADATA
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build()))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(existsSql).bind("p1").to(1L).build(),
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
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(deleteSql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(deleteSql).bind("p1").to(1L).build(), 1L));

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
    assertEquals(2, loadRequests.size());
    ExecuteSqlRequest describeLoadRequest = loadRequests.get(0);
    assertEquals(QueryMode.PLAN, describeLoadRequest.getQueryMode());
    ExecuteSqlRequest executeLoadRequest = loadRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeLoadRequest.getQueryMode());
    if (describeLoadRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
    }

    List<ExecuteSqlRequest> checkExistsRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(existsSql))
            .collect(Collectors.toList());
    assertEquals(2, checkExistsRequests.size());
    ExecuteSqlRequest describeCheckExistsRequest = checkExistsRequests.get(0);
    assertEquals(QueryMode.PLAN, describeCheckExistsRequest.getQueryMode());
    ExecuteSqlRequest executeCheckExistsRequest = checkExistsRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeCheckExistsRequest.getQueryMode());
    if (describeCheckExistsRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
    }

    List<ExecuteSqlRequest> deleteRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(deleteSql))
            .collect(Collectors.toList());
    assertEquals(2, deleteRequests.size());
    ExecuteSqlRequest describeDeleteRequest = deleteRequests.get(0);
    assertEquals(QueryMode.PLAN, describeDeleteRequest.getQueryMode());
    ExecuteSqlRequest executeDeleteRequest = deleteRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeDeleteRequest.getQueryMode());
    assertTrue(describeDeleteRequest.getTransaction().hasBegin());
    assertTrue(describeDeleteRequest.getTransaction().getBegin().hasReadWrite());
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
            + "\"AllTypes\".\"col_varchar\" AS \"AllTypes_col_varchar\", \"AllTypes\".\"col_jsonb\" AS \"AllTypes_col_jsonb\" "
            + "FROM \"all_types\" \"AllTypes\" "
            + "WHERE (\"AllTypes\".\"col_bigint\" = $1) LIMIT 1";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("AllTypes_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(1L).build(),
            createAllTypesResultSet("AllTypes_")));

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
            + "  col_varchar: 'test',\n"
            + "  col_jsonb: { key: 'value' }\n"
            + "}\n",
        output);

    int expectedCommitCount = 0;
    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(QueryMode.PLAN, describeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    if (describeRequest.getTransaction().hasBegin()) {
      expectedCommitCount++;
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
            + "\"AllTypes\".\"col_varchar\" AS \"AllTypes_col_varchar\", \"AllTypes\".\"col_jsonb\" AS \"AllTypes_col_jsonb\" "
            + "FROM \"all_types\" \"AllTypes\" "
            + "WHERE \"AllTypes\".\"col_bigint\" IN ($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(existsSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createAllTypesResultSetMetadata("AllTypes_")
                        .toBuilder()
                        .setUndeclaredParameters(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("p1")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(existsSql).bind("p1").to(2L).build(),
            ResultSet.newBuilder()
                .setMetadata(createAllTypesResultSetMetadata("AllTypes_"))
                .build()));

    String insertSql =
        "INSERT INTO \"all_types\""
            + "(\"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\", \"col_jsonb\") "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("some random string"))
                .bind("p4")
                .to(0.123456789d)
                .bind("p5")
                .to(123456789L)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("234.54235"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-07-22T18:15:42.011Z"))
                .bind("p8")
                .to(Date.parseDate("2022-07-22"))
                .bind("p9")
                .to("some random string")
                // TODO: Change to JSONB
                .bind("p10")
                .to("{\"key\":\"value\"}")
                .build(),
            1L));

    String output = runTest("createAllTypes", pgServer.getLocalPort());

    assertEquals("\n\nCreated one record\n", output);

    List<ExecuteSqlRequest> insertRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(2, insertRequests.size());
    ExecuteSqlRequest describeInsertRequest = insertRequests.get(0);
    assertEquals(QueryMode.PLAN, describeInsertRequest.getQueryMode());
    ExecuteSqlRequest executeInsertRequest = insertRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeInsertRequest.getQueryMode());
    assertTrue(describeInsertRequest.getTransaction().hasBegin());
    assertTrue(describeInsertRequest.getTransaction().getBegin().hasReadWrite());
    assertTrue(executeInsertRequest.getTransaction().hasId());

    assertEquals(10, executeInsertRequest.getParamTypesMap().size());
    assertEquals(10, executeInsertRequest.getParams().getFieldsCount());
    assertEquals("2", executeInsertRequest.getParams().getFieldsMap().get("p1").getStringValue());
    assertTrue(executeInsertRequest.getParams().getFieldsMap().get("p2").getBoolValue());
    assertEquals(
        "c29tZSByYW5kb20gc3RyaW5n",
        executeInsertRequest.getParams().getFieldsMap().get("p3").getStringValue());
    assertEquals(
        0.123456789d,
        executeInsertRequest.getParams().getFieldsMap().get("p4").getNumberValue(),
        0.0d);
    assertEquals(
        "123456789", executeInsertRequest.getParams().getFieldsMap().get("p5").getStringValue());
    assertEquals(
        "234.54235", executeInsertRequest.getParams().getFieldsMap().get("p6").getStringValue());
    assertEquals(
        Timestamp.parseTimestamp("2022-07-22T20:15:42.011+02:00"),
        Timestamp.parseTimestamp(
            executeInsertRequest.getParams().getFieldsMap().get("p7").getStringValue()));
    assertEquals(
        "2022-07-22", executeInsertRequest.getParams().getFieldsMap().get("p8").getStringValue());
    assertEquals(
        "some random string",
        executeInsertRequest.getParams().getFieldsMap().get("p9").getStringValue());
    assertEquals(
        "{\"key\":\"value\"}",
        executeInsertRequest.getParams().getFieldsMap().get("p10").getStringValue());
  }

  static String runTest(String testName, int port) throws IOException, InterruptedException {
    return NodeJSTest.runTest("typeorm/data-test", testName, "localhost", port, "db");
  }
}
