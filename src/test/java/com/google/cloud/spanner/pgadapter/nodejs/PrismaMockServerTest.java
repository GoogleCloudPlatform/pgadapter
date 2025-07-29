// Copyright 2023 Google LLC
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(NodeJSTest.class)
@RunWith(Parameterized.class)
public class PrismaMockServerTest extends AbstractMockServerTest {
  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("prisma-tests");
  }

  private String getHost() {
    if (useDomainSocket) {
      return "/tmp";
    }
    return "localhost";
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    String output = runTest("testSelect1", getHost(), pgServer.getLocalPort());

    assertEquals("[ { C: 1n } ]\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
  }

  @Test
  public void testShowWellKnownClient() throws Exception {
    String output = runTest("testShowWellKnownClient", getHost(), pgServer.getLocalPort());

    assertEquals("[ { 'spanner.well_known_client': 'PRISMA' } ]\n", output);
  }

  @Test
  public void testPgAdvisoryLock() throws Exception {
    String output = runTest("testPgAdvisoryLock", getHost(), pgServer.getLocalPort());

    assertEquals(
        "[ { pg_advisory_lock: '72707369' } ]\n" + "[ { pg_advisory_unlock: true } ]\n", output);
  }

  @Test
  public void testShowAutoAddLimitClause() throws Exception {
    String output = runTest("testShowAutoAddLimitClause", getHost(), pgServer.getLocalPort());

    assertEquals("[ { 'spanner.auto_add_limit_clause': 'true' } ]\n", output);
  }

  @Test
  public void testFindAllUsers() throws Exception {
    String sql =
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" "
            + "FROM \"public\".\"User\" WHERE 1=1 "
            + "ORDER BY \"public\".\"User\".\"id\" ASC "
            + "LIMIT $1 OFFSET $2";
    ResultSetMetadata metadata =
        createMetadata(
            ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
            ImmutableList.of("id", "email", "name"));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(TypeCode.INT64, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to(10L).bind("p2").to(0L).build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Peter").build())
                        .addValues(Value.newBuilder().setStringValue("peter@prisma.com").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.com").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .addValues(Value.newBuilder().setStringValue("Hannah").build())
                        .addValues(Value.newBuilder().setStringValue("hannah@prisma.com").build())
                        .build())
                .build()));

    String output = runTest("testFindAllUsers", getHost(), pgServer.getLocalPort());

    assertEquals(
        "[\n"
            + "  { id: '1', email: 'Peter', name: 'peter@prisma.com' },\n"
            + "  { id: '2', email: 'Alice', name: 'alice@prisma.com' },\n"
            + "  { id: '3', email: 'Hannah', name: 'hannah@prisma.com' }\n"
            + "]\n",
        output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testFindUniqueUser() throws Exception {
    String sql =
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" "
            + "FROM \"public\".\"User\" "
            + "WHERE (\"public\".\"User\".\"id\" = $1 AND 1=1) "
            + "LIMIT $2 OFFSET $3";
    ResultSetMetadata metadata =
        createMetadata(
            ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
            ImmutableList.of("id", "email", "name"));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.STRING, TypeCode.INT64, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Peter").build())
                        .addValues(Value.newBuilder().setStringValue("peter@prisma.com").build())
                        .build())
                .build()));

    String output = runTest("testFindUniqueUser", getHost(), pgServer.getLocalPort());

    assertEquals("{ id: '1', email: 'Peter', name: 'peter@prisma.com' }\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasSingleUse());
    assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasSingleUse());
    assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testFindTwoUniqueUsers() throws Exception {
    for (boolean stale : new boolean[] {false, true}) {
      String sql =
          "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" "
              + "FROM \"public\".\"User\" "
              + "WHERE (\"public\".\"User\".\"id\" = $1 AND 1=1) "
              + "LIMIT $2 OFFSET $3";
      ResultSetMetadata metadata =
          createMetadata(
              ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
              ImmutableList.of("id", "email", "name"));

      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.of(sql),
              ResultSet.newBuilder()
                  .setMetadata(
                      metadata.toBuilder()
                          .setUndeclaredParameters(
                              createParameterTypesMetadata(
                                      ImmutableList.of(
                                          TypeCode.STRING, TypeCode.INT64, TypeCode.INT64))
                                  .getUndeclaredParameters())
                          .build())
                  .build()));
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(sql)
                  .bind("p1")
                  .to("1")
                  .bind("p2")
                  .to(1L)
                  .bind("p3")
                  .to(0L)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(metadata)
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setStringValue("1").build())
                          .addValues(Value.newBuilder().setStringValue("Peter").build())
                          .addValues(Value.newBuilder().setStringValue("peter@prisma.com").build())
                          .build())
                  .build()));
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(sql)
                  .bind("p1")
                  .to("2")
                  .bind("p2")
                  .to(1L)
                  .bind("p3")
                  .to(0L)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(metadata)
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setStringValue("2").build())
                          .addValues(Value.newBuilder().setStringValue("Alice").build())
                          .addValues(Value.newBuilder().setStringValue("alice@prisma.com").build())
                          .build())
                  .build()));

      String output =
          runTest(
              stale ? "testFindTwoUniqueUsersUsingStaleRead" : "testFindTwoUniqueUsers",
              getHost(),
              pgServer.getLocalPort());

      assertEquals(
          "{ id: '1', email: 'Peter', name: 'peter@prisma.com' }\n"
              + "{ id: '2', email: 'Alice', name: 'alice@prisma.com' }\n",
          output);

      List<ExecuteSqlRequest> executeSqlRequests =
          mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
              .filter(request -> request.getSql().equals(sql))
              .collect(Collectors.toList());
      assertEquals(3, executeSqlRequests.size());
      ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
      assertTrue(planRequest.getTransaction().hasSingleUse());
      assertTrue(planRequest.getTransaction().getSingleUse().hasReadOnly());
      assertEquals(
          stale, planRequest.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
      assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
      ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
      assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
      assertTrue(executeRequest.getTransaction().hasSingleUse());
      assertTrue(executeRequest.getTransaction().getSingleUse().hasReadOnly());
      assertEquals(
          stale, executeRequest.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());
      ExecuteSqlRequest executeRequest2 = executeSqlRequests.get(2);
      assertEquals(QueryMode.NORMAL, executeRequest2.getQueryMode());
      assertTrue(executeRequest2.getTransaction().hasSingleUse());
      assertTrue(executeRequest2.getTransaction().getSingleUse().hasReadOnly());
      assertEquals(
          stale, executeRequest2.getTransaction().getSingleUse().getReadOnly().hasMaxStaleness());

      mockSpanner.clearRequests();
    }
  }

  @Test
  public void testCreateUser() throws Exception {
    String sql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("id")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("email")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("name")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("2373a81d-772c-4221-adf0-06965bc02c2c")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(StructType.getDefaultInstance())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2373a81d-772c-4221-adf0-06965bc02c2c")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String output = runTest("testCreateUser", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{\n"
            + "  id: '2373a81d-772c-4221-adf0-06965bc02c2c',\n"
            + "  email: 'alice@prisma.io',\n"
            + "  name: 'Alice'\n"
            + "}\n",
        output);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Ignore("Column order is random")
  @Test
  public void testNestedWrite() throws Exception {
    String insertPostSql =
        "INSERT INTO \"public\".\"Post\" (\"id\",\"title\",\"authorId\",\"published\") VALUES ($1,$2,$3,$4), ($5,$6,$7,$8)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertPostSql),
            ResultSet.newBuilder()
                .setMetadata(
                    createParameterTypesMetadata(
                        ImmutableList.of(
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.BOOL,
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.STRING,
                            TypeCode.BOOL,
                            TypeCode.STRING)))
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertPostSql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to("Join the Prisma Slack on https://slack.prisma.io")
                .bind("p3")
                .to(false)
                .bind("p4")
                .to("1")
                .bind("p5")
                .to("2")
                .bind("p6")
                .to("Follow @prisma on Twitter")
                .bind("p7")
                .to(false)
                .bind("p8")
                .to("1")
                .build(),
            2L));

    String insertUserSql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\") VALUES ($1,$2) RETURNING \"public\".\"User\".\"id\"";
    ResultSetMetadata insertUserMetadata =
        createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING)).toBuilder()
            .setRowType(createMetadata(ImmutableList.of(TypeCode.STRING)).getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertUserSql),
            ResultSet.newBuilder()
                .setMetadata(insertUserMetadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertUserSql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to("alice@prisma.io")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(insertUserMetadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .build()));

    String selectUserSql =
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" FROM \"public\".\"User\" WHERE \"public\".\"User\".\"id\" = $1 LIMIT $2 OFFSET $3";
    ResultSetMetadata selectMetadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.INT64, TypeCode.INT64))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectUserSql),
            ResultSet.newBuilder().setMetadata(selectMetadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectUserSql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(selectMetadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .build())
                .build()));

    String output = runTest("testNestedWrite", getHost(), pgServer.getLocalPort());

    assertEquals("{ id: '1', email: 'alice@prisma.io', name: null }\n", output);

    List<ExecuteSqlRequest> executeInsertUserSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertUserSql))
            .collect(Collectors.toList());
    assertEquals(2, executeInsertUserSqlRequests.size());
    ExecuteSqlRequest planInsertUserRequest = executeInsertUserSqlRequests.get(0);
    assertTrue(planInsertUserRequest.getTransaction().hasBegin());
    assertTrue(planInsertUserRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planInsertUserRequest.getQueryMode());
    ExecuteSqlRequest executeInsertUserRequest = executeInsertUserSqlRequests.get(1);
    assertTrue(executeInsertUserRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeInsertUserRequest.getQueryMode());

    List<ExecuteSqlRequest> executeInsertPostsSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertPostSql))
            .collect(Collectors.toList());
    assertEquals(3, executeInsertPostsSqlRequests.size());
    ExecuteSqlRequest planInsertPostRequest = executeInsertPostsSqlRequests.get(0);
    assertTrue(planInsertPostRequest.getTransaction().hasId());
    assertEquals(QueryMode.PLAN, planInsertPostRequest.getQueryMode());
    ExecuteSqlRequest executeInsertPost1Request = executeInsertPostsSqlRequests.get(1);
    assertTrue(executeInsertPost1Request.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeInsertPost1Request.getQueryMode());
    ExecuteSqlRequest executeInsertPost2Request = executeInsertPostsSqlRequests.get(2);
    assertTrue(executeInsertPost2Request.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeInsertPost2Request.getQueryMode());

    List<ExecuteSqlRequest> selectRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(selectUserSql))
            .collect(Collectors.toList());
    assertEquals(2, selectRequests.size());
    ExecuteSqlRequest planSelectRequest = selectRequests.get(0);
    assertTrue(planSelectRequest.getTransaction().hasId());
    assertEquals(QueryMode.PLAN, planSelectRequest.getQueryMode());
    ExecuteSqlRequest executeSelectRequest = selectRequests.get(1);
    assertTrue(executeSelectRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeSelectRequest.getQueryMode());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String sql =
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\" "
            + "FROM \"public\".\"User\" "
            + "WHERE (\"public\".\"User\".\"id\" = $1 AND 1=1) "
            + "LIMIT $2 OFFSET $3";
    ResultSetMetadata metadata =
        createMetadata(
            ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING),
            ImmutableList.of("id", "email", "name"));

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(
                    metadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.STRING, TypeCode.INT64, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("Peter").build())
                        .addValues(Value.newBuilder().setStringValue("peter@prisma.com").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql)
                .bind("p1")
                .to("2")
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.com").build())
                        .build())
                .build()));

    String output = runTest("testReadOnlyTransaction", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{ id: '1', email: 'Peter', name: 'peter@prisma.com' }\n"
            + "{ id: '2', email: 'Alice', name: 'alice@prisma.com' }\n",
        output);

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginTransactionRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginTransactionRequest.getOptions().hasReadOnly());
    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasId());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    assertTrue(executeRequest.getTransaction().hasId());
    ExecuteSqlRequest executeRequest2 = executeSqlRequests.get(2);
    assertEquals(QueryMode.NORMAL, executeRequest2.getQueryMode());
    assertTrue(executeRequest2.getTransaction().hasId());
  }

  @Test
  public void testCreateAllTypes() throws IOException, InterruptedException {
    String insertSql =
        "INSERT INTO \"public\".\"AllTypes\" (\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float4\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_interval\",\"col_date\",\"col_varchar\",\"col_jsonb\") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float4\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_interval\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float4\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_interval\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\"";
    ResultSet allTypesResultSet =
        createAllTypesResultSetWithoutNullsInArrays(/* intervalAsString= */ true);

    ResultSetMetadata insertMetadata =
        createParameterTypesMetadata(
                ImmutableList.of(
                    TypeCode.INT64,
                    TypeCode.BOOL,
                    TypeCode.BYTES,
                    TypeCode.FLOAT32,
                    TypeCode.FLOAT64,
                    TypeCode.INT64,
                    TypeCode.NUMERIC,
                    TypeCode.TIMESTAMP,
                    TypeCode.STRING,
                    TypeCode.DATE,
                    TypeCode.STRING,
                    TypeCode.JSON))
            .toBuilder()
            .setRowType(allTypesResultSet.getMetadata().getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(insertMetadata)
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test"))
                .bind("p4")
                .to(3.14f)
                .bind("p5")
                .to(3.14d)
                .bind("p6")
                .to(100)
                .bind("p7")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p8")
                .to(Timestamp.parseTimestamp("2022-02-16T13:18:02.123456000Z"))
                .bind("p9")
                .to("P1Y2M3DT4H5M6.789S")
                .bind("p10")
                .to(Date.parseDate("2022-03-29"))
                .bind("p11")
                .to("test")
                .bind("p12")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value\"}"))
                .build(),
            ResultSet.newBuilder()
                .setMetadata(insertMetadata)
                .addAllRows(allTypesResultSet.getRowsList())
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String output = runTest("testCreateAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: true,\n"
            + "  col_bytea: Uint8Array(4) [ 116, 101, 115, 116 ],\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 3.14,\n"
            + "  col_int: 100,\n"
            + "  col_numeric: 6.626,\n"
            + "  col_timestamptz: 2022-02-16T13:18:02.123Z,\n"
            + "  col_interval: 'P1Y2M3DT4H5M6.789S',\n"
            + "  col_date: 2022-03-29T00:00:00.000Z,\n"
            + "  col_varchar: 'test',\n"
            + "  col_jsonb: { key: 'value' },\n"
            + "  col_array_bigint: [ 1n, 1n, 2n ],\n"
            + "  col_array_bool: [ true, true, false ],\n"
            + "  col_array_bytea: [\n"
            + "    Uint8Array(6) [ 98, 121, 116, 101, 115, 49 ],\n"
            + "    Uint8Array(6) [ 98, 121, 116, 101, 115, 49 ],\n"
            + "    Uint8Array(6) [ 98, 121, 116, 101, 115, 50 ]\n"
            + "  ],\n"
            + "  col_array_float4: [ 3.14, 3.14, -99.99 ],\n"
            + "  col_array_float8: [ 3.14, 3.14, -99.99 ],\n"
            + "  col_array_int: [ -100, -100, -200 ],\n"
            + "  col_array_numeric: [ 6.626, 6.626, -3.14 ],\n"
            + "  col_array_timestamptz: [\n"
            + "    2022-02-16T16:18:02.123Z,\n"
            + "    2022-02-16T16:18:02.123Z,\n"
            + "    2000-01-01T00:00:00.000Z\n"
            + "  ],\n"
            + "  col_array_interval: [ 'P-100MT123456.789S', 'P-100MT123456.789S', 'P1Y' ],\n"
            + "  col_array_date: [\n"
            + "    2023-02-20T00:00:00.000Z,\n"
            + "    2023-02-20T00:00:00.000Z,\n"
            + "    2000-01-01T00:00:00.000Z\n"
            + "  ],\n"
            + "  col_array_varchar: [ 'string1', 'string1', 'string2' ],\n"
            + "  col_array_jsonb: [ { key: 'value1' }, { key: 'value1' }, { key: 'value2' } ]\n"
            + "}\n",
        output);
  }

  @Test
  public void testUpdateAllTypes() throws IOException, InterruptedException {
    ResultSetMetadata allTypesMetadata =
        createAllTypesResultSetMetadata("", /* intervalAsString= */ true);
    ListValue row =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setBoolValue(false).build())
            .addValues(
                Value.newBuilder()
                    .setStringValue(
                        Base64.getEncoder()
                            .encodeToString("updated".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .addValues(Value.newBuilder().setNumberValue(3.14f).build())
            .addValues(Value.newBuilder().setNumberValue(6.626d).build())
            .addValues(Value.newBuilder().setStringValue("-100").build())
            .addValues(Value.newBuilder().setStringValue("3.14").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13T05:40:02.123456000Z").build())
            .addValues(Value.newBuilder().setStringValue("P1Y2M3DT4H5M6.789S").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13").build())
            .addValues(Value.newBuilder().setStringValue("updated").build())
            .addValues(Value.newBuilder().setStringValue("{\"key\":\"updated\"}").build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();

    String updateSql =
        "UPDATE \"public\".\"AllTypes\" SET \"col_bool\" = $1, \"col_bytea\" = $2, \"col_float4\" = $3, \"col_float8\" = $4, \"col_int\" = $5, \"col_numeric\" = $6, \"col_timestamptz\" = $7, \"col_interval\" = $8, \"col_date\" = $9, \"col_varchar\" = $10, \"col_jsonb\" = $11 WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $12 AND 1=1) RETURNING \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float4\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_interval\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float4\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_interval\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\"";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(updateSql),
            ResultSet.newBuilder()
                .setMetadata(
                    allTypesMetadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.BOOL,
                                        TypeCode.BYTES,
                                        TypeCode.FLOAT32,
                                        TypeCode.FLOAT64,
                                        TypeCode.INT64,
                                        TypeCode.NUMERIC,
                                        TypeCode.TIMESTAMP,
                                        TypeCode.STRING,
                                        TypeCode.DATE,
                                        TypeCode.STRING,
                                        TypeCode.JSON,
                                        TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(updateSql)
                .bind("p1")
                .to(false)
                .bind("p2")
                .to(ByteArray.copyFrom("updated"))
                .bind("p3")
                .to(3.14f)
                .bind("p4")
                .to(6.626d)
                .bind("p5")
                .to(-100)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("3.14"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2023-03-13T05:40:02.123456000Z"))
                .bind("p8")
                .to("P1Y2M3DT4H5M6.789S")
                .bind("p9")
                .to(Date.parseDate("2023-03-13"))
                .bind("p10")
                .to("updated")
                .bind("p11")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"updated\"}"))
                .bind("p12")
                .to(1L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(allTypesMetadata)
                .addRows(row)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String output = runTest("testUpdateAllTypes", getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: Uint8Array(7) [\n"
            + "    117, 112, 100,\n"
            + "     97, 116, 101,\n"
            + "    100\n"
            + "  ],\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_interval: 'P1Y2M3DT4H5M6.789S',\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_interval: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    int index = -1;
    assertEquals(updateSql, requests.get(++index).getSql());
    assertEquals(QueryMode.PLAN, requests.get(index).getQueryMode());
    assertEquals(updateSql, requests.get(++index).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(index).getQueryMode());
  }

  @Test
  public void testUpsertAllTypes() throws IOException, InterruptedException {
    String upsertSql =
        "INSERT INTO \"public\".\"AllTypes\" (\"col_bigint\",\"col_bool\",\"col_bytea\",\"col_float4\",\"col_float8\",\"col_int\",\"col_numeric\",\"col_timestamptz\",\"col_interval\",\"col_date\",\"col_varchar\",\"col_jsonb\") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (\"col_bigint\") DO UPDATE SET \"col_bool\" = $13, \"col_bytea\" = $14, \"col_float4\" = $15, \"col_float8\" = $16, \"col_int\" = $17, \"col_numeric\" = $18, \"col_timestamptz\" = $19, \"col_interval\" = $20, \"col_date\" = $21, \"col_varchar\" = $22, \"col_jsonb\" = $23 WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $24 AND 1=1) RETURNING \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float4\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_interval\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float4\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_interval\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\"";
    ResultSetMetadata metadata =
        createMetadata(
            ImmutableList.of(
                TypeCode.INT64,
                TypeCode.BOOL,
                TypeCode.BYTES,
                TypeCode.FLOAT32,
                TypeCode.FLOAT64,
                TypeCode.INT64,
                TypeCode.NUMERIC,
                TypeCode.TIMESTAMP,
                TypeCode.STRING,
                TypeCode.DATE,
                TypeCode.STRING,
                TypeCode.JSON),
            true,
            ImmutableList.of(
                "col_bigint",
                "col_bool",
                "col_bytea",
                "col_float4",
                "col_float8",
                "col_int",
                "col_numeric",
                "col_timestamptz",
                "col_interval",
                "col_date",
                "col_varchar",
                "col_jsonb",
                "col_array_bigint",
                "col_array_bool",
                "col_array_bytea",
                "col_array_float4",
                "col_array_float8",
                "col_array_int",
                "col_array_numeric",
                "col_array_timestamptz",
                "col_array_interval",
                "col_array_date",
                "col_array_varchar",
                "col_array_jsonb"),
            ImmutableList.of(
                TypeCode.INT64,
                TypeCode.BOOL,
                TypeCode.BYTES,
                TypeCode.FLOAT32,
                TypeCode.FLOAT64,
                TypeCode.INT64,
                TypeCode.NUMERIC,
                TypeCode.TIMESTAMP,
                TypeCode.STRING,
                TypeCode.DATE,
                TypeCode.STRING,
                TypeCode.JSON,
                TypeCode.BOOL,
                TypeCode.BYTES,
                TypeCode.FLOAT32,
                TypeCode.FLOAT64,
                TypeCode.INT64,
                TypeCode.NUMERIC,
                TypeCode.TIMESTAMP,
                TypeCode.STRING,
                TypeCode.DATE,
                TypeCode.STRING,
                TypeCode.JSON,
                TypeCode.INT64));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(upsertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));

    ListValue row =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setBoolValue(false).build())
            .addValues(
                Value.newBuilder()
                    .setStringValue(
                        Base64.getEncoder()
                            .encodeToString("updated".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .addValues(Value.newBuilder().setNumberValue(3.14f).build())
            .addValues(Value.newBuilder().setNumberValue(6.626d).build())
            .addValues(Value.newBuilder().setStringValue("-100").build())
            .addValues(Value.newBuilder().setStringValue("3.14").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13T05:40:02.123456000Z").build())
            .addValues(Value.newBuilder().setStringValue("P1Y2M3DT4H5M6.789S").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13").build())
            .addValues(Value.newBuilder().setStringValue("updated").build())
            .addValues(Value.newBuilder().setStringValue("{\"key\":\"updated\"}").build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(upsertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(false)
                .bind("p3")
                .to(ByteArray.copyFrom("updated"))
                .bind("p4")
                .to(3.14f)
                .bind("p5")
                .to(6.626d)
                .bind("p6")
                .to(-100)
                .bind("p7")
                .to(com.google.cloud.spanner.Value.pgNumeric("3.14"))
                .bind("p8")
                .to(Timestamp.parseTimestamp("2023-03-13T05:40:02.123456000Z"))
                .bind("p9")
                .to("P1Y2M3DT4H5M6.789S")
                .bind("p10")
                .to(Date.parseDate("2023-03-13"))
                .bind("p11")
                .to("updated")
                .bind("p12")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"updated\"}"))
                .bind("p13")
                .to(false)
                .bind("p14")
                .to(ByteArray.copyFrom("updated"))
                .bind("p15")
                .to(3.14f)
                .bind("p16")
                .to(6.626d)
                .bind("p17")
                .to(-100)
                .bind("p18")
                .to(com.google.cloud.spanner.Value.pgNumeric("3.14"))
                .bind("p19")
                .to(Timestamp.parseTimestamp("2023-03-13T05:40:02.123456000Z"))
                .bind("p20")
                .to("P1Y2M3DT4H5M6.789S")
                .bind("p21")
                .to(Date.parseDate("2023-03-13"))
                .bind("p22")
                .to("updated")
                .bind("p23")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"updated\"}"))
                .bind("p24")
                .to(1L)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(row)
                .build()));

    String output = runTest("testUpsertAllTypes", getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: Uint8Array(7) [\n"
            + "    117, 112, 100,\n"
            + "     97, 116, 101,\n"
            + "    100\n"
            + "  ],\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_interval: 'P1Y2M3DT4H5M6.789S',\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_interval: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(upsertSql, requests.get(0).getSql());
    assertEquals(QueryMode.PLAN, requests.get(0).getQueryMode());
    assertEquals(upsertSql, requests.get(1).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(1).getQueryMode());
  }

  @Test
  public void testDeleteAllTypes() throws IOException, InterruptedException {
    String deleteSql =
        "DELETE FROM \"public\".\"AllTypes\" WHERE (\"public\".\"AllTypes\".\"col_bigint\" = $1 AND 1=1) RETURNING \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float4\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_interval\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float4\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_interval\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\"";

    ResultSetMetadata allTypesMetadata =
        createAllTypesResultSetMetadata("", /* intervalAsString= */ true);
    ListValue row =
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setBoolValue(false).build())
            .addValues(
                Value.newBuilder()
                    .setStringValue(
                        Base64.getEncoder()
                            .encodeToString("updated".getBytes(StandardCharsets.UTF_8)))
                    .build())
            .addValues(Value.newBuilder().setNumberValue(3.14f).build())
            .addValues(Value.newBuilder().setNumberValue(6.626d).build())
            .addValues(Value.newBuilder().setStringValue("-100").build())
            .addValues(Value.newBuilder().setStringValue("3.14").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13T05:40:02.123456000Z").build())
            .addValues(Value.newBuilder().setStringValue("P1Y2M3DT4H5M6.789S").build())
            .addValues(Value.newBuilder().setStringValue("2023-03-13").build())
            .addValues(Value.newBuilder().setStringValue("updated").build())
            .addValues(Value.newBuilder().setStringValue("{\"key\":\"updated\"}").build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(deleteSql),
            ResultSet.newBuilder()
                .setMetadata(
                    allTypesMetadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(ImmutableList.of(TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .setStats(ResultSetStats.getDefaultInstance())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(deleteSql).bind("p1").to(1L).build(),
            ResultSet.newBuilder()
                .setMetadata(allTypesMetadata)
                .addRows(row)
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .build()));

    String output = runTest("testDeleteAllTypes", getHost(), pgServer.getLocalPort());
    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: false,\n"
            + "  col_bytea: Uint8Array(7) [\n"
            + "    117, 112, 100,\n"
            + "     97, 116, 101,\n"
            + "    100\n"
            + "  ],\n"
            + "  col_float4: 3.14,\n"
            + "  col_float8: 6.626,\n"
            + "  col_int: -100,\n"
            + "  col_numeric: 3.14,\n"
            + "  col_timestamptz: 2023-03-13T05:40:02.123Z,\n"
            + "  col_interval: 'P1Y2M3DT4H5M6.789S',\n"
            + "  col_date: 2023-03-13T00:00:00.000Z,\n"
            + "  col_varchar: 'updated',\n"
            + "  col_jsonb: { key: 'updated' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float4: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_interval: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    int index = -1;
    assertEquals(deleteSql, requests.get(++index).getSql());
    assertEquals(QueryMode.PLAN, requests.get(index).getQueryMode());
    assertEquals(deleteSql, requests.get(++index).getSql());
    assertEquals(QueryMode.NORMAL, requests.get(index).getQueryMode());
  }

  @Ignore("Skip this, as the SQL generated by createMany contains the columns in random order")
  @Test
  public void testCreateManyAllTypes() throws IOException, InterruptedException {
    String insertSql =
        "INSERT INTO \"public\".\"AllTypes\" (\"col_numeric\",\"col_bigint\",\"col_date\",\"col_float8\",\"col_varchar\",\"col_bool\",\"col_int\",\"col_bytea\",\"col_jsonb\",\"col_timestamptz\") "
            + "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10), ($11,$12,$13,$14,$15,$16,$17,$18,$19,$20)";
    ResultSetMetadata insertMetadata =
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
                TypeCode.JSON,
                TypeCode.INT64,
                TypeCode.BOOL,
                TypeCode.BYTES,
                TypeCode.FLOAT64,
                TypeCode.INT64,
                TypeCode.NUMERIC,
                TypeCode.TIMESTAMP,
                TypeCode.DATE,
                TypeCode.STRING,
                TypeCode.JSON));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(insertMetadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("test1"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100)
                .bind("p6")
                .to(com.google.cloud.spanner.Value.pgNumeric("6.626"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-02-16T12:18:02.123456000Z"))
                .bind("p8")
                .to(Date.parseDate("2022-03-29"))
                .bind("p9")
                .to("test1")
                .bind("p10")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value1\"}"))
                .bind("p11")
                .to(2L)
                .bind("p12")
                .to(false)
                .bind("p13")
                .to(ByteArray.copyFrom("test2"))
                .bind("p14")
                .to(-3.14d)
                .bind("p15")
                .to(-100)
                .bind("p16")
                .to(com.google.cloud.spanner.Value.pgNumeric("-6.626"))
                .bind("p17")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02.123456000Z"))
                .bind("p18")
                .to(Date.parseDate("2022-03-30"))
                .bind("p19")
                .to("test2")
                .bind("p20")
                .to(com.google.cloud.spanner.Value.pgJsonb("{\"key\":\"value2\"}"))
                .build(),
            2L));

    String selectSql =
        "SELECT \"public\".\"AllTypes\".\"col_bigint\", \"public\".\"AllTypes\".\"col_bool\", \"public\".\"AllTypes\".\"col_bytea\", \"public\".\"AllTypes\".\"col_float8\", \"public\".\"AllTypes\".\"col_int\", \"public\".\"AllTypes\".\"col_numeric\", \"public\".\"AllTypes\".\"col_timestamptz\", \"public\".\"AllTypes\".\"col_date\", \"public\".\"AllTypes\".\"col_varchar\", \"public\".\"AllTypes\".\"col_jsonb\", \"public\".\"AllTypes\".\"col_array_bigint\", \"public\".\"AllTypes\".\"col_array_bool\", \"public\".\"AllTypes\".\"col_array_bytea\", \"public\".\"AllTypes\".\"col_array_float8\", \"public\".\"AllTypes\".\"col_array_int\", \"public\".\"AllTypes\".\"col_array_numeric\", \"public\".\"AllTypes\".\"col_array_timestamptz\", \"public\".\"AllTypes\".\"col_array_date\", \"public\".\"AllTypes\".\"col_array_varchar\", \"public\".\"AllTypes\".\"col_array_jsonb\" FROM \"public\".\"AllTypes\" WHERE \"public\".\"AllTypes\".\"col_bigint\" = $1 LIMIT $2 OFFSET $3";
    ResultSetMetadata allTypesMetadata = createAllTypesResultSetMetadata("");
    ResultSetMetadata allArrayTypesMetadata = createAllArrayTypesResultSetMetadata("");
    ResultSetMetadata selectMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addAllFields(allTypesMetadata.getRowType().getFieldsList())
                    .addAllFields(allArrayTypesMetadata.getRowType().getFieldsList())
                    .build())
            .build();
    ListValue row =
        createAllTypesResultSet("").getRows(0).toBuilder()
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(selectSql),
            ResultSet.newBuilder()
                .setMetadata(
                    selectMetadata.toBuilder()
                        .setUndeclaredParameters(
                            createParameterTypesMetadata(
                                    ImmutableList.of(
                                        TypeCode.INT64, TypeCode.INT64, TypeCode.INT64))
                                .getUndeclaredParameters())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(selectSql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(1L)
                .bind("p3")
                .to(0L)
                .build(),
            ResultSet.newBuilder().setMetadata(selectMetadata).addRows(row).build()));

    String output = runTest("testCreateManyAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "{\n"
            + "  col_bigint: 1n,\n"
            + "  col_bool: true,\n"
            + "  col_bytea: <Buffer 74 65 73 74>,\n"
            + "  col_float8: 3.14,\n"
            + "  col_int: 100,\n"
            + "  col_numeric: 6.626,\n"
            + "  col_timestamptz: 2022-02-16T13:18:02.123Z,\n"
            + "  col_date: 2022-03-29T00:00:00.000Z,\n"
            + "  col_varchar: 'test',\n"
            + "  col_jsonb: { key: 'value' },\n"
            + "  col_array_bigint: [],\n"
            + "  col_array_bool: [],\n"
            + "  col_array_bytea: [],\n"
            + "  col_array_float8: [],\n"
            + "  col_array_int: [],\n"
            + "  col_array_numeric: [],\n"
            + "  col_array_timestamptz: [],\n"
            + "  col_array_date: [],\n"
            + "  col_array_varchar: [],\n"
            + "  col_array_jsonb: []\n"
            + "}\n",
        output);
  }

  @Test
  public void testCreateMultipleUsersWithoutTransaction() throws Exception {
    String insertSql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("2373a81d-772c-4221-adf0-06965bc02c2c")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2373a81d-772c-4221-adf0-06965bc02c2c")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("e673150f-17ff-451a-8bc7-641fe77f1226")
                .bind("p2")
                .to("peter@prisma.io")
                .bind("p3")
                .to("Peter")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("e673150f-17ff-451a-8bc7-641fe77f1226")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("peter@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Peter").build())
                        .build())
                .build()));

    String output =
        runTest("testCreateMultipleUsersWithoutTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("Created two users\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    ExecuteSqlRequest execute2Request = executeSqlRequests.get(2);
    assertTrue(execute2Request.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.NORMAL, execute2Request.getQueryMode());

    // All transactions are committed.
    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testCreateMultipleUsersInTransaction() throws Exception {
    String insertSql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("2373a81d-772c-4221-adf0-06965bc02c2c")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("2373a81d-772c-4221-adf0-06965bc02c2c")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("e673150f-17ff-451a-8bc7-641fe77f1226")
                .bind("p2")
                .to("peter@prisma.io")
                .bind("p3")
                .to("Peter")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(
                            Value.newBuilder()
                                .setStringValue("e673150f-17ff-451a-8bc7-641fe77f1226")
                                .build())
                        .addValues(Value.newBuilder().setStringValue("peter@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Peter").build())
                        .build())
                .build()));

    String output =
        runTest("testCreateMultipleUsersInTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("Created two users\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    ExecuteSqlRequest execute2Request = executeSqlRequests.get(2);
    assertTrue(execute2Request.getTransaction().hasId());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.NORMAL, execute2Request.getQueryMode());

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testUnhandledErrorInTransaction() throws Exception {
    String insertSql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to("peter@prisma.io")
                .bind("p3")
                .to("Peter")
                .build(),
            Status.ALREADY_EXISTS
                .withDescription("Row with id 1 already exists")
                .asRuntimeException()));

    String output = runTest("testUnhandledErrorInTransaction", getHost(), pgServer.getLocalPort());

    assertTrue(output, output.contains("Transaction failed:"));
    assertTrue(output, output.contains("Row with id 1 already exists"));

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());
    ExecuteSqlRequest executeRequest2 = executeSqlRequests.get(2);
    assertTrue(executeRequest2.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeRequest2.getQueryMode());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testHandledErrorInTransaction() throws Exception {
    String insertSql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("1")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            Status.ALREADY_EXISTS
                .withDescription("Row with id 1 already exists")
                .asRuntimeException()));

    String output = runTest("testHandledErrorInTransaction", getHost(), pgServer.getLocalPort());

    assertTrue(output, output.contains("Transaction failed:"));
    assertTrue(output, output.contains("Insert statement failed:"));
    assertTrue(output, output.contains("Row with id 1 already exists"));
    assertTrue(
        output,
        output.contains(
            "current transaction is aborted, commands ignored until end of transaction block"));
    assertFalse(output, output.contains("Created user with id 2"));

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(insertSql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest planRequest = executeSqlRequests.get(0);
    assertTrue(planRequest.getTransaction().hasBegin());
    assertTrue(planRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(QueryMode.PLAN, planRequest.getQueryMode());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(QueryMode.NORMAL, executeRequest.getQueryMode());

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testUnsupportedTransactionIsolationLevel() throws Exception {
    String output =
        runTest("testUnsupportedTransactionIsolationLevel", getHost(), pgServer.getLocalPort());

    assertTrue(
        output,
        output.contains(
            "Error querying the database: ERROR: Unknown value for TRANSACTION: ISOLATION LEVEL READ COMMITTED"));
  }

  @Test
  public void testTransactionIsolationLevel() throws Exception {
    String insertSql =
        "INSERT INTO \"public\".\"User\" (\"id\",\"email\",\"name\") VALUES ($1,$2,$3) RETURNING \"public\".\"User\".\"id\", \"public\".\"User\".\"email\", \"public\".\"User\".\"name\"";
    ResultSetMetadata metadata =
        createParameterTypesMetadata(
                ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
            .toBuilder()
            .setRowType(
                createMetadata(ImmutableList.of(TypeCode.STRING, TypeCode.STRING, TypeCode.STRING))
                    .getRowType())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(insertSql),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(insertSql)
                .bind("p1")
                .to("2373a81d-772c-4221-adf0-06965bc02c2c")
                .bind("p2")
                .to("alice@prisma.io")
                .bind("p3")
                .to("Alice")
                .build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("alice@prisma.io").build())
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .build())
                .build()));

    String output = runTest("testTransactionIsolationLevel", getHost(), pgServer.getLocalPort());

    assertTrue(output, output.contains("Created one user using isolation level repeatable read"));
    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest describeRequest =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(
        IsolationLevel.REPEATABLE_READ,
        describeRequest.getTransaction().getBegin().getIsolationLevel());
  }

  @Test
  public void testMigrateStatus() throws Exception {
    setupPrismaMigrateQueries();

    String output = runTest("testMigrateStatus", getHost(), pgServer.getLocalPort());
    assertTrue(output, output.contains("Running npx prisma migrate status on"));
  }

  @Test
  public void testMigrateDeploy() throws Exception {
    setupPrismaMigrateQueries();
    addDdlResponseToSpannerAdmin();

    String tableSql =
        "with pg_class as (\n"
            + "  select\n"
            + "  '''\"' || t.table_schema || '\".\"' || t.table_name || '\"''' as oid,\n"
            + "  table_name as relname,\n"
            + "  case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "  0 as reltype,\n"
            + "  0 as reloftype,\n"
            + "  0 as relowner,\n"
            + "  1 as relam,\n"
            + "  0 as relfilenode,\n"
            + "  0 as reltablespace,\n"
            + "  0 as relpages,\n"
            + "  0.0::float8 as reltuples,\n"
            + "  0 as relallvisible,\n"
            + "  0 as reltoastrelid,\n"
            + "  false as relhasindex,\n"
            + "  false as relisshared,\n"
            + "  'p' as relpersistence,\n"
            + "  CASE table_type WHEN 'BASE TABLE' THEN 'r' WHEN 'VIEW' THEN 'v' ELSE '' END as relkind,\n"
            + "  count(*) as relnatts,\n"
            + "  0 as relchecks,\n"
            + "  false as relhasrules,\n"
            + "  false as relhastriggers,\n"
            + "  false as relhassubclass,\n"
            + "  false as relrowsecurity,\n"
            + "  false as relforcerowsecurity,\n"
            + "  true as relispopulated,\n"
            + "  'n' as relreplident,\n"
            + "  false as relispartition,\n"
            + "  0 as relrewrite,\n"
            + "  0 as relfrozenxid,\n"
            + "  0 as relminmxid,\n"
            + "  '{}'::bigint[] as relacl,\n"
            + "  '{}'::text[] as reloptions,\n"
            + "  0 as relpartbound\n"
            + "from information_schema.tables t\n"
            + "inner join information_schema.columns using (table_catalog, table_schema, table_name)\n"
            + "group by t.table_name, t.table_schema, t.table_type\n"
            + "union all\n"
            + "select\n"
            + "    '''\"' || i.table_schema || '\".\"' || i.table_name || '\".\"' || i.index_name || '\"''' as oid,\n"
            + "    i.index_name as relname,\n"
            + "    case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "    0 as reltype,\n"
            + "    0 as reloftype,\n"
            + "    0 as relowner,\n"
            + "    1 as relam,\n"
            + "    0 as relfilenode,\n"
            + "    0 as reltablespace,\n"
            + "    0 as relpages,\n"
            + "    0.0::float8 as reltuples,\n"
            + "    0 as relallvisible,\n"
            + "    0 as reltoastrelid,\n"
            + "    false as relhasindex,\n"
            + "    false as relisshared,\n"
            + "    'p' as relpersistence,\n"
            + "    'i' as relkind,\n"
            + "    count(*) as relnatts,\n"
            + "    0 as relchecks,\n"
            + "    false as relhasrules,\n"
            + "    false as relhastriggers,\n"
            + "    false as relhassubclass,\n"
            + "    false as relrowsecurity,\n"
            + "    false as relforcerowsecurity,\n"
            + "    true as relispopulated,\n"
            + "    'n' as relreplident,\n"
            + "    false as relispartition,\n"
            + "    0 as relrewrite,\n"
            + "    0 as relfrozenxid,\n"
            + "    0 as relminmxid,\n"
            + "    '{}'::bigint[] as relacl,\n"
            + "    '{}'::text[] as reloptions,\n"
            + "    0 as relpartbound\n"
            + "from information_schema.indexes i\n"
            + "inner join information_schema.index_columns using (table_catalog, table_schema, table_name, index_name)\n"
            + "group by i.index_name, i.table_name, i.table_schema\n"
            + "),\n"
            + "pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")\n"
            + "\n"
            + "                SELECT replace(tbl.relname, 'prisma_migrations', '_prisma_migrations') AS table_name, namespace.nspname AS table_namespace\n"
            + "                FROM pg_class AS tbl\n"
            + "                INNER JOIN pg_namespace AS namespace ON namespace.oid = tbl.relnamespace\n"
            + "                WHERE tbl.relkind = 'r' AND namespace.nspname = ANY ( $1 )\n"
            + "            ";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("table_name")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("table_namespace")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(
                                Type.newBuilder()
                                    .setCode(TypeCode.ARRAY)
                                    .setArrayElementType(
                                        Type.newBuilder().setCode(TypeCode.STRING).build())
                                    .build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(tableSql), ResultSet.newBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(tableSql)
                .bind("p1")
                .toStringArray(ImmutableList.of("public"))
                .build(),
            ResultSet.newBuilder().setMetadata(metadata).build()));

    String output = runTest("testMigrateDeploy", getHost(), pgServer.getLocalPort());

    assertTrue(output, output.contains("Running npx prisma migrate deploy"));
    assertEquals(1, mockDatabaseAdmin.getRequests().size());
    assertEquals(UpdateDatabaseDdlRequest.class, mockDatabaseAdmin.getRequests().get(0).getClass());
    UpdateDatabaseDdlRequest request =
        (UpdateDatabaseDdlRequest) mockDatabaseAdmin.getRequests().get(0);
    assertEquals(1, request.getStatementsCount());
    assertEquals(
        "CREATE TABLE prisma_migrations (\n"
            + "    id                      VARCHAR(36) PRIMARY KEY NOT NULL,\n"
            + "    checksum                VARCHAR(64) NOT NULL,\n"
            + "    finished_at             TIMESTAMPTZ,\n"
            + "    migration_name          VARCHAR(255) NOT NULL,\n"
            + "    logs                    TEXT,\n"
            + "    rolled_back_at          TIMESTAMPTZ,\n"
            + "    started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),\n"
            + "    applied_steps_count     INTEGER NOT NULL DEFAULT 0\n"
            + ")",
        request.getStatements(0));
  }

  private void setupPrismaMigrateQueries() {
    String versionSql =
        "with pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")\n"
            + "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1), '14.1', '140001'::integer as numeric_version;";
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("exists")
                            .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("c1")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setName("numeric_version")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .build())
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(versionSql), ResultSet.newBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(versionSql).bind("p1").to("public").build(),
            ResultSet.newBuilder()
                .setMetadata(metadata)
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setBoolValue(true).build())
                        .addValues(Value.newBuilder().setStringValue("14.1").build())
                        .addValues(Value.newBuilder().setStringValue("14001").build())
                        .build())
                .build()));

    String migrationsSql =
        "SELECT \"id\", \"checksum\", \"finished_at\", \"migration_name\", \"logs\", \"rolled_back_at\", \"started_at\", \"applied_steps_count\" FROM prisma_migrations ORDER BY \"started_at\" ASC";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(migrationsSql),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("id")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("checksum")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("finished_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("migration_name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("logs")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("rolled_back_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("started_at")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("applied_steps_count")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("foo").build())
                        .addValues(
                            Value.newBuilder().setStringValue("2025-03-22T10:00:00Z").build())
                        .addValues(Value.newBuilder().setStringValue("v1").build())
                        .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .addValues(
                            Value.newBuilder().setStringValue("2025-03-22T10:00:00Z").build())
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .build())
                .build()));
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("prisma-tests", testName, host, port, "db");
  }

  static ResultSet createAllTypesResultSetWithoutNullsInArrays() {
    return createAllTypesResultSetWithoutNullsInArrays(false);
  }

  static ResultSet createAllTypesResultSetWithoutNullsInArrays(boolean intervalAsString) {
    return createAllTypesResultSet("1", "", false, false, intervalAsString);
  }
}
