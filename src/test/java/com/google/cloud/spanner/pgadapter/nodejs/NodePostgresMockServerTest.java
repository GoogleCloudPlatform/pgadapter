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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.CopyInMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(NodeJSTest.class)
@RunWith(Parameterized.class)
public class NodePostgresMockServerTest extends AbstractMockServerTest {
  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("node-postgres");
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

    assertEquals("SELECT 1 returned: 1\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInsert() throws Exception {
    // The node-postgres driver by default does not use prepared statements. However, it also sends
    // all query parameters as untyped strings. As Cloud Spanner does not support this for all data
    // types, PGAdapter will in that case always do an extra round-trip to describe the parameters
    // of the statement, and then use the types for the parameters that are returned.
    // The result of the describe statement call is cached for that connection, so executing the
    // same statement once more will not cause another describe-statement round-trip.
    String sql = "INSERT INTO users(name) VALUES($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));

    String output = runTest("testInsert", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertExecutedTwice() throws Exception {
    String sql = "INSERT INTO users(name) VALUES($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("bar").build(), 2L));

    String output = runTest("testInsertTwice", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\nInserted 2 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasId());

    executeRequest = executeSqlRequests.get(2);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAutoCommit() throws IOException, InterruptedException {
    String sql = "INSERT INTO users(name) VALUES($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));

    String output = runTest("testInsertAutoCommit", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    // TODO: Enable when node-postgres 8.9 has been released.
    //    assertTrue(executeRequest.getTransaction().hasBegin());
    //    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    //    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAllTypes() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO AllTypes "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
    StatementResult updateResult =
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("some random string"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100)
                .bind("p6")
                .to(Value.pgNumeric("234.54235"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-07-22T20:15:42.011+02:00"))
                .bind("p8")
                .to(Date.parseDate("2022-07-22"))
                .bind("p9")
                .to("some-random-string")
                .bind("p10")
                .to("{\"my_key\":\"my-value\"}")
                .build(),
            1L);
    mockSpanner.putStatementResult(updateResult);

    String output = runTest("testInsertAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(10, executeRequest.getParamTypesCount());
    // TODO: Enable once node-postgres 8.9 is released.
    //    assertTrue(executeRequest.getTransaction().hasBegin());
    //    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    //    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAllTypesNull() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO AllTypes "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to((Value) null)
                .bind("p2")
                .to((Value) null)
                .bind("p3")
                .to((Value) null)
                .bind("p4")
                .to((Value) null)
                .bind("p5")
                .to((Value) null)
                .bind("p6")
                .to((Value) null)
                .bind("p7")
                .to((Value) null)
                .bind("p8")
                .to((Value) null)
                .bind("p9")
                .to((Value) null)
                .bind("p10")
                .to((Value) null)
                .build(),
            1L));

    String output = runTest("testInsertAllTypesAllNull", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(0);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(0, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAllTypesPreparedStatement() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO AllTypes "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
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
    StatementResult updateResult =
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("some random string"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100)
                .bind("p6")
                .to(Value.pgNumeric("234.54235"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-07-22T20:15:42.011+02:00"))
                .bind("p8")
                .to(Date.parseDate("2022-07-22"))
                .bind("p9")
                .to("some-random-string")
                .bind("p10")
                .to("{\"my_key\":\"my-value\"}")
                .build(),
            1L);
    mockSpanner.putStatementResult(updateResult);
    // The statement inserting null values will use the types that are returned by the automatically
    // described statement from the first insert.
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(2L)
                .bind("p2")
                .to((Boolean) null)
                .bind("p3")
                .to((ByteArray) null)
                .bind("p4")
                .to((Double) null)
                .bind("p5")
                .to((Long) null)
                .bind("p6")
                .to(Value.pgNumeric(null))
                .bind("p7")
                .to((Timestamp) null)
                .bind("p8")
                .to((Date) null)
                .bind("p9")
                .to((String) null)
                .bind("p10")
                .to((String) null)
                .build(),
            1L));

    String output =
        runTest("testInsertAllTypesPreparedStatement", getHost(), pgServer.getLocalPort());

    assertEquals("Inserted 1 row(s)\nInserted 1 row(s)\n", output);

    // node-postgres will only send one parse message when using prepared statements. It never uses
    // DescribeStatement. It will send a new DescribePortal for each time the prepared statement is
    // executed.
    List<ParseMessage> parseMessages = getWireMessagesOfType(ParseMessage.class);
    assertEquals(1, parseMessages.size());
    assertEquals("insert-all-types", parseMessages.get(0).getName());
    List<DescribeMessage> describeMessages = getWireMessagesOfType(DescribeMessage.class);
    assertEquals(2, describeMessages.size());
    assertEquals("", describeMessages.get(0).getName());
    assertEquals(PreparedType.Portal, describeMessages.get(0).getType());
    assertEquals("", describeMessages.get(1).getName());
    assertEquals(PreparedType.Portal, describeMessages.get(1).getType());

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(10, executeRequest.getParamTypesCount());
    // TODO: Enable once node-postgres 8.9 is released.
    //    assertTrue(executeRequest.getTransaction().hasBegin());
    //    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    //    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testSelectAllTypes() throws IOException, InterruptedException {
    String sql =
        "SELECT col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
            + "FROM AllTypes";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesResultSet("")));

    String output = runTest("testSelectAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "Selected {"
            + "\"col_bigint\":\"1\","
            + "\"col_bool\":true,"
            + "\"col_bytea\":{\"type\":\"Buffer\",\"data\":[116,101,115,116]},"
            + "\"col_float8\":3.14,"
            + "\"col_int\":\"100\","
            + "\"col_numeric\":\"6.626\","
            + "\"col_timestamptz\":\"2022-02-16T13:18:02.123Z\","
            + "\"col_date\":\"2022-03-29\","
            + "\"col_varchar\":\"test\","
            + "\"col_jsonb\":{\"key\":\"value\"}"
            + "}\n",
        output);
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testSelectAllTypesNull() throws IOException, InterruptedException {
    String sql =
        "SELECT col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
            + "FROM AllTypes";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createAllTypesNullResultSet("")));

    String output = runTest("testSelectAllTypes", getHost(), pgServer.getLocalPort());

    assertEquals(
        "Selected {"
            + "\"col_bigint\":null,"
            + "\"col_bool\":null,"
            + "\"col_bytea\":null,"
            + "\"col_float8\":null,"
            + "\"col_int\":null,"
            + "\"col_numeric\":null,"
            + "\"col_timestamptz\":null,"
            + "\"col_date\":null,"
            + "\"col_varchar\":null,"
            + "\"col_jsonb\":null"
            + "}\n",
        output);
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testErrorInReadWriteTransaction() throws IOException, InterruptedException {
    String sql = "INSERT INTO users(name) VALUES($1)";
    String describeParamsSql = "select $1 from (select name=$1 from users) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeParamsSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.newBuilder(sql).bind("p1").to("foo").build(),
            Status.ALREADY_EXISTS
                .withDescription("Row with \"name\" 'foo' already exists")
                .asRuntimeException()));

    String output = runTest("testErrorInReadWriteTransaction", getHost(), pgServer.getLocalPort());

    assertEquals(
        "Insert error: error: com.google.api.gax.rpc.AlreadyExistsException: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Row with \"name\" 'foo' already exists\n"
            + "Second insert failed with error: error: current transaction is aborted, commands ignored until end of transaction block\n"
            + "SELECT 1 returned: 1\n",
        output);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testReadOnlyTransaction() throws Exception {
    String output = runTest("testReadOnlyTransaction", getHost(), pgServer.getLocalPort());

    assertEquals("executed read-only transaction\n", output);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(
                request ->
                    request.getSql().equals("SELECT 1") || request.getSql().equals("SELECT 2"))
            .collect(Collectors.toList());
    assertEquals(2, requests.size());
    assertTrue(requests.get(0).getTransaction().hasId());
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    BeginTransactionRequest beginRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginRequest.getOptions().hasReadOnly());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testReadOnlyTransactionWithError() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of("SELECT * FROM foo"), Status.INVALID_ARGUMENT.asRuntimeException()));

    String output = runTest("testReadOnlyTransactionWithError", getHost(), pgServer.getLocalPort());

    assertEquals(
        "current transaction is aborted, commands ignored until end of transaction block\n"
            + "[ { C: '2' } ]\n",
        output);
  }

  @Test
  public void testCopyTo() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("select * from AllTypes"), createAllTypesResultSet("")));

    String output = runTest("testCopyTo", getHost(), pgServer.getLocalPort());

    assertEquals(
        "1\tt\t\\\\x74657374\t3.14\t100\t6.626\t2022-02-16 13:18:02.123456+00\t2022-03-29\ttest\t{\"key\": \"value\"}\n",
        output);
  }

  @Test
  public void testCopyFrom() throws Exception {
    CopyInMockServerTest.setupCopyInformationSchemaResults(mockSpanner, "public", "alltypes", true);

    String output = runTest("testCopyFrom", getHost(), pgServer.getLocalPort());

    assertEquals("Finished copy operation\n", output);
  }

  @Test
  public void testDmlBatch() throws Exception {
    String sql = "INSERT INTO users(name) VALUES($1)";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            ResultSet.newBuilder()
                .setMetadata(createParameterTypesMetadata(ImmutableList.of(TypeCode.STRING)))
                .setStats(ResultSetStats.newBuilder().build())
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("bar").build(), 1L));

    String output = runTest("testDmlBatch", getHost(), pgServer.getLocalPort());

    assertEquals("executed dml batch\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(sql, describeRequest.getSql());
    assertTrue(describeRequest.hasTransaction());
    assertTrue(describeRequest.getTransaction().hasBegin());
    assertTrue(describeRequest.getTransaction().getBegin().hasReadWrite());

    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchDmlRequests.size());
    ExecuteBatchDmlRequest request = batchDmlRequests.get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(2, request.getStatementsCount());
    String[] expectedValues = new String[] {"foo", "bar"};
    for (int i = 0; i < request.getStatementsCount(); i++) {
      assertEquals(sql, request.getStatements(i).getSql());
      assertEquals(1, request.getStatements(i).getParamTypesCount());
      assertEquals(
          expectedValues[i],
          request.getStatements(i).getParams().getFieldsMap().get("p1").getStringValue());
    }
    // We get two commits, because PGAdapter auto-describes the DML statement in a separate
    // transaction if the auto-describe happens during a DML batch.
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testDdlBatch() throws Exception {
    addDdlResponseToSpannerAdmin();

    String output = runTest("testDdlBatch", getHost(), pgServer.getLocalPort());

    assertEquals("executed ddl batch\n", output);
    assertEquals(1, mockDatabaseAdmin.getRequests().size());
    assertEquals(UpdateDatabaseDdlRequest.class, mockDatabaseAdmin.getRequests().get(0).getClass());
    UpdateDatabaseDdlRequest request =
        (UpdateDatabaseDdlRequest) mockDatabaseAdmin.getRequests().get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(
        "create table my_table1 (id bigint primary key, value varchar)", request.getStatements(0));
    assertEquals(
        "create table my_table2 (id bigint primary key, value varchar)", request.getStatements(1));
  }

  static String runTest(String testName, String host, int port)
      throws IOException, InterruptedException {
    return NodeJSTest.runTest("node-postgres", testName, host, port, "db");
  }
}
