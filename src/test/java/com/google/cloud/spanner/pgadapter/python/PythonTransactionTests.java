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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.BatchCreateSessionsRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(PythonTest.class)
public class PythonTransactionTests extends PythonTestSetup {

  private static ResultSet createResultSet(int id, String name) {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .setName("Id")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("Name")
                            .build())
                    .build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue(String.valueOf(id)).build())
            .addValues(Value.newBuilder().setStringValue(name).build())
            .build());
    return resultSetBuilder.build();
  }

  @Test
  public void testSimpleStatementsUsingTransactions() throws IOException, InterruptedException {
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table where some_col = some_val";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    String sql4 = "Select * from some_table3";
    String sql5 = "insert into some_table3(col1, col2) values(value1, value2)";
    String sql6 = "Select * from some_table3 where some_col = some_val";

    statements.add("query");
    statements.add(sql4);

    statements.add("update");
    statements.add(sql5);

    statements.add("query");
    statements.add(sql6);

    statements.add("transaction");
    statements.add("rollback");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql4), createResultSet(3, "1234")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql5), 2));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql6), createResultSet(4, "6789")));

    String expectedOutput =
        "(1, 'abcd')\n" + "1\n" + "(2, 'pqrs')\n" + "(3, '1234')\n" + "2\n" + "(4, '6789')\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);

    assertTrue(requests.get(0).getSql().equals(sql1));
    assertTrue(requests.get(1).getSql().equals(sql2));
    assertTrue(requests.get(2).getSql().equals(sql3));
    assertTrue(requests.get(3).getSql().equals(sql4));
    assertTrue(requests.get(4).getSql().equals(sql5));
    assertTrue(requests.get(5).getSql().equals(sql6));

    ByteString transactionIdForSql2 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql2))
            .findAny()
            .get()
            .getTransaction()
            .getId();
    ByteString transactionIdForSql3 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql3))
            .findAny()
            .get()
            .getTransaction()
            .getId();

    assertTrue(transactionIdForSql2.equals(transactionIdForSql3));

    ByteString transactionIdForSql5 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql5))
            .findAny()
            .get()
            .getTransaction()
            .getId();
    ByteString transactionIdForSql6 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql6))
            .findAny()
            .get()
            .getTransaction()
            .getId();

    assertTrue(transactionIdForSql5.equals(transactionIdForSql6));
  }

  @Test
  public void testAutocommitInTransactions() throws IOException, InterruptedException {
    // tests autocommit settings using connection.autocommit variable
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    // no commit request will be sent after this query because autocommit is false
    statements.add("query");
    statements.add(sql1);

    // no commit request will be sent after this update because autocommit is false
    statements.add("update");
    statements.add(sql2);

    // no commit request will be sent after this update because autocommit is false
    statements.add("update");
    statements.add(sql4);

    // 1 commit request will be sent
    statements.add("transaction");
    statements.add("commit");

    statements.add("transaction");
    statements.add("set autocommit True");

    // no commit request after this query although autocommit is true because it is a query sql
    statements.add("query");
    statements.add(sql3);

    // 1 commit request will be sent after this update because autocommit is true
    statements.add("update");
    statements.add(sql2);

    // 1 commit request will be sent after this update because autocommit is true
    statements.add("update");
    statements.add(sql4);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n" + "1\n" + "2\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();

    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(9, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(0))).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(1))).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(2))).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(4))).getSql().equals(sql3));
    assertTrue(((ExecuteSqlRequest) (requests.get(4))).getTransaction().hasSingleUse());
    assertTrue(
        ((ExecuteSqlRequest) (requests.get(4))).getTransaction().getSingleUse().hasReadOnly());

    assertTrue(requests.get(5).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(5))).getSql().equals(sql2));

    assertTrue(requests.get(6).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(7).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(7))).getSql().equals(sql4));

    assertTrue(requests.get(8).getClass().equals(CommitRequest.class));
  }

  @Test
  public void testAutocommitSessionInTransactions() throws IOException, InterruptedException {
    // tests autocommit settings using set_session() function
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    // no commit request will be sent after this query because autocommit is false
    statements.add("query");
    statements.add(sql1);

    // no commit request will be sent after this update because autocommit is false
    statements.add("update");
    statements.add(sql2);

    // no commit request will be sent after this update because autocommit is false
    statements.add("update");
    statements.add(sql4);

    // 1 commit request will be sent
    statements.add("transaction");
    statements.add("commit");

    statements.add("transaction");
    statements.add("set session autocommit True");

    // no commit request after this query although autocommit is true because it is a query sql
    statements.add("query");
    statements.add(sql3);

    // 1 commit request will be sent after this update because autocommit is true
    statements.add("update");
    statements.add(sql2);

    // 1 commit request will be sent after this update because autocommit is true
    statements.add("update");
    statements.add(sql4);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n" + "1\n" + "2\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();

    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(9, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(0))).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(1))).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(2))).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(4))).getSql().equals(sql3));
    assertTrue(((ExecuteSqlRequest) (requests.get(4))).getTransaction().hasSingleUse());
    assertTrue(
        ((ExecuteSqlRequest) (requests.get(4))).getTransaction().getSingleUse().hasReadOnly());

    assertTrue(requests.get(5).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(5))).getSql().equals(sql2));

    assertTrue(requests.get(6).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(7).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(7))).getSql().equals(sql4));

    assertTrue(requests.get(8).getClass().equals(CommitRequest.class));
  }

  @Test
  public void testDeferrableInTransactions() throws IOException, InterruptedException {
    List<String> statements = new ArrayList<>();
    String sql = "Select * from some_table";

    // It will throw an error because deferrable is not supported
    statements.add("transaction");
    statements.add("set deferrable True");

    // statement won't be executed because the above setting will throw error
    statements.add("query");
    statements.add(sql);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(1, "abcd")));

    String expectedOutput = "the 'deferrable' setting is only available from PostgreSQL 9.1\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    assertEquals(expectedOutput, actualOutput);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testDeferrableSessionInTransactions() throws IOException, InterruptedException {
    List<String> statements = new ArrayList<>();
    String sql = "Select * from some_table";

    // It will throw an error because deferrable is not supported
    statements.add("transaction");
    statements.add("set session deferrable True");

    // statement won't be executed because the above setting will throw error
    statements.add("query");
    statements.add(sql);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(1, "abcd")));

    String expectedOutput = "the 'deferrable' setting is only available from PostgreSQL 9.1\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    assertEquals(expectedOutput, actualOutput);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
  }

  @Test
  public void testReadOnlyInTransactions() throws IOException, InterruptedException {
    // tests readonly settings using connection.readonly variable
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    // query will be executed as expected
    statements.add("query");
    statements.add(sql1);

    // update will be executed as expected
    statements.add("update");
    statements.add(sql2);

    // update will be executed as expected
    statements.add("update");
    statements.add(sql4);

    // 1 commit request will be sent
    statements.add("transaction");
    statements.add("commit");

    statements.add("transaction");
    statements.add("set readonly True");

    // query will be executed as expected
    statements.add("query");
    statements.add(sql1);

    // query will be executed as expected
    statements.add("query");
    statements.add(sql3);

    // commit request will not be sent to the Cloud Spanner
    // because Cloud Spanner does not require read-only transactions
    // to be committed or rolled back
    statements.add("transaction");
    statements.add("commit");

    // update will throw an error because the readonly is activated
    statements.add("update");
    statements.add(sql2);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput =
        "(1, 'abcd')\n"
            + "1\n"
            + "2\n"
            + "(1, 'abcd')\n"
            + "(2, 'pqrs')\n"
            + "FAILED_PRECONDITION: Update statements are not allowed for read-only transactions\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(5, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();

    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(7, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(0))).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(1))).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(2))).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(BeginTransactionRequest.class));
    assertTrue(((BeginTransactionRequest) (requests.get(4))).getOptions().hasReadOnly());

    assertTrue(requests.get(5).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(5))).getSql().equals(sql1));
    ByteString transactionIdForRequest5 =
        ((ExecuteSqlRequest) (requests.get(5))).getTransaction().getId();

    assertTrue(requests.get(6).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(6))).getSql().equals(sql3));
    ByteString transactionIdForRequest6 =
        ((ExecuteSqlRequest) (requests.get(6))).getTransaction().getId();

    assertTrue(transactionIdForRequest6.equals(transactionIdForRequest5));
  }

  @Test
  public void testReadOnlySessionInTransactions() throws IOException, InterruptedException {
    // tests readonly settings using set_session() function
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    // query will be executed as expected
    statements.add("query");
    statements.add(sql1);

    // update will be executed as expected
    statements.add("update");
    statements.add(sql2);

    // update will be executed as expected
    statements.add("update");
    statements.add(sql4);

    // 1 commit request will be sent
    statements.add("transaction");
    statements.add("commit");

    statements.add("transaction");
    statements.add("set session readonly True");

    // query will be executed as expected
    statements.add("query");
    statements.add(sql1);

    // query will be executed as expected
    statements.add("query");
    statements.add(sql3);

    // commit request will not be sent to the Cloud Spanner
    // because Cloud Spanner does not require read-only transactions
    // to be committed or rolled back
    statements.add("transaction");
    statements.add("commit");

    // update will throw an error because the readonly is activated
    statements.add("update");
    statements.add(sql2);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput =
        "(1, 'abcd')\n"
            + "1\n"
            + "2\n"
            + "(1, 'abcd')\n"
            + "(2, 'pqrs')\n"
            + "FAILED_PRECONDITION: Update statements are not allowed for read-only transactions\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(5, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();

    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(7, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(0))).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(1))).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(2))).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    // Read Only Transaction is Started
    assertTrue(requests.get(4).getClass().equals(BeginTransactionRequest.class));
    assertTrue(((BeginTransactionRequest) (requests.get(4))).getOptions().hasReadOnly());

    assertTrue(requests.get(5).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(5))).getSql().equals(sql1));
    ByteString transactionIdForRequest5 =
        ((ExecuteSqlRequest) (requests.get(5))).getTransaction().getId();

    assertTrue(requests.get(6).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(6))).getSql().equals(sql3));
    ByteString transactionIdForRequest6 =
        ((ExecuteSqlRequest) (requests.get(6))).getTransaction().getId();

    assertTrue(transactionIdForRequest6.equals(transactionIdForRequest5));
  }

  // Isolation Levels
  // ISOLATION_LEVEL_AUTOCOMMIT -> 0
  // ISOLATION_LEVEL_READ_COMMITTED -> 1
  // ISOLATION_LEVEL_REPEATABLE_READ -> 2
  // ISOLATION_LEVEL_SERIALIZABLE -> 3
  // ISOLATION_LEVEL_READ_UNCOMMITTED -> 4
  @Test
  public void testUnsupportedIsolationLevelsInTransactions()
      throws IOException, InterruptedException {
    // tests isolation_level settings for unsupported isolation levels using
    // connection.isolation_level variable
    List<String> unsupportedIsolationLevels = Arrays.asList("1", "4");

    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput =
        "(1, 'abcd')\n"
            + "1\n"
            + "INVALID_ARGUMENT: Unknown value for TRANSACTION: ISOLATION LEVEL READ COMMITTED\n";

    for (String unsupportedIsolationLevel : unsupportedIsolationLevels) {
      List<String> statements = new ArrayList<>();

      // query will be executed as expected
      statements.add("query");
      statements.add(sql1);

      // update will be executed as expected
      statements.add("update");
      statements.add(sql2);

      // 1 commit request will be sent
      statements.add("transaction");
      statements.add("commit");

      statements.add("transaction");
      statements.add("set isolation_level " + unsupportedIsolationLevel);

      // query won't be executed because the previous setting would've thrown error
      statements.add("query");
      statements.add(sql3);

      String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
      assertEquals(expectedOutput, actualOutput);

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

      List<AbstractMessage> requests = mockSpanner.getRequests();

      requests =
          requests.stream()
              .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
              .collect(Collectors.toList());

      assertEquals(3, requests.size());

      assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
      assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

      assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
      assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

      assertTrue(requests.get(2).getClass().equals(CommitRequest.class));

      mockSpanner.clearRequests();
    }

    assertEquals(
        2,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("READ COMMITTED"))
            .count());
  }

  @Test
  public void testUnsupportedIsolationLevelsSessionInTransactions()
      throws IOException, InterruptedException {
    // tests isolation_level settings for unsupported isolation levels using set_session function
    List<String> unsupportedIsolationLevels = Arrays.asList("1", "4");

    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput =
        "(1, 'abcd')\n"
            + "1\n"
            + "INVALID_ARGUMENT: Unknown value for TRANSACTION: ISOLATION LEVEL READ COMMITTED\n";

    for (String unsupportedIsolationLevel : unsupportedIsolationLevels) {
      List<String> statements = new ArrayList<>();

      // query will be executed as expected
      statements.add("query");
      statements.add(sql1);

      // update will be executed as expected
      statements.add("update");
      statements.add(sql2);

      // 1 commit request will be sent
      statements.add("transaction");
      statements.add("commit");

      statements.add("transaction");
      statements.add("set session isolation_level " + unsupportedIsolationLevel);

      // query won't be executed because the previous setting would've thrown error
      statements.add("query");
      statements.add(sql3);

      String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
      assertEquals(expectedOutput, actualOutput);

      assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
      assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

      List<AbstractMessage> requests = mockSpanner.getRequests();
      requests =
          requests.stream()
              .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
              .collect(Collectors.toList());

      assertEquals(3, requests.size());

      assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
      assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

      assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
      assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

      assertTrue(requests.get(2).getClass().equals(CommitRequest.class));

      mockSpanner.clearRequests();
    }

    assertEquals(
        2,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("READ COMMITTED"))
            .count());
  }

  @Test
  public void testSupportedIsolationLevelInTransactions() throws IOException, InterruptedException {
    // tests supported isolation_level settings using connection.isolation_level variable
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("update");
    statements.add(sql4);

    statements.add("transaction");
    statements.add("commit");

    // This will work fine because SERIALIZABLE is supported by the Cloud Spanner
    statements.add("transaction");
    statements.add("set isolation_level 3");

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();
    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(6, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(2)).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(4)).getSql().equals(sql3));

    assertTrue(requests.get(5).getClass().equals(CommitRequest.class));

    assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("SERIALIZABLE"))
            .count());
  }

  @Test
  public void testSupportedIsolationLevelSessionInTransactions()
      throws IOException, InterruptedException {
    // tests supported isolation_level settings using set_session function
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("update");
    statements.add(sql4);

    statements.add("transaction");
    statements.add("commit");

    // This will work fine because SERIALIZABLE is supported by the Cloud Spanner
    statements.add("transaction");
    statements.add("set session isolation_level 3");

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();
    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(6, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(2)).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(4)).getSql().equals(sql3));

    assertTrue(requests.get(5).getClass().equals(CommitRequest.class));

    assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("SERIALIZABLE"))
            .count());
  }

  // The default version returned by PG Adapter is 1.0.
  // Psycopg2 doesn't support Repeatable Read with the versions of Postgres lower than 9.1
  // So, if the version of the Postgres (or PG Adapter in or case) is lower than 9.1,
  // it converts Repeatable Read to Serializable.
  // Hence, instead of sending SET TRANSACTION ISOLATION LEVEL REPEATABLE READ,
  // it sends SET TRANSACTION ISOLATION LEVEL SERIALIZABLE.
  // This is the reason why setting isolation_level to REPEATABLE_READ in the versions lower than
  // 9.1
  // will not lead to any error, even though we don't support REPEATABLE_READ
  @Test
  public void testRepeatableReadIsolationLevelWithLowerVersions()
      throws IOException, InterruptedException {
    // tests repeatable read isolation_level settings with default version 1.0 using
    // connection.isolation_level variable
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("update");
    statements.add(sql4);

    statements.add("transaction");
    statements.add("commit");

    // This will not cause error because the server version is 1.0,
    // so the psycopg2 will send SERIALIZABLE instead of REPEATABLE READ as the isolation level
    statements.add("transaction");
    statements.add("set isolation_level 2");

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();
    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(6, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(2)).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(4)).getSql().equals(sql3));

    assertTrue(requests.get(5).getClass().equals(CommitRequest.class));

    // PG Adapter should've received a call to set isolation level to SERIALIZABLE not REPEATABLE
    // READ
    // because the default version is 1.0 which is lower than 9.1
    // So, Psycopg2 will convert REPEATABLE_READ to SERIALIZABLE
    assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("SERIALIZABLE"))
            .count());
  }

  @Test
  public void testRepeatableReadIsolationLevelSessionWithLowerVersions()
      throws IOException, InterruptedException {
    // tests repeatable read with default version 1.0 isolation_level settings using set_session
    // function
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("update");
    statements.add(sql4);

    statements.add("transaction");
    statements.add("commit");

    // This will not cause error because the server version is 1.0,
    // so the psycopg2 will send SERIALIZABLE instead of REPEATABLE READ as the isolation level
    statements.add("transaction");
    statements.add("set session isolation_level 2");

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();
    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(6, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(2)).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(4)).getSql().equals(sql3));

    assertTrue(requests.get(5).getClass().equals(CommitRequest.class));

    // PG Adapter should've received a call to set isolation level to SERIALIZABLE not REPEATABLE
    // READ
    // because the default version is 1.0 which is lower than 9.1
    // So, Psycopg2 will convert REPEATABLE_READ to SERIALIZABLE
    assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("SERIALIZABLE"))
            .count());
  }

  private void restartServerWithDifferentVersion(String version) throws Exception {
    stopMockSpannerAndPgAdapterServers();
    doStartMockSpannerAndPgAdapterServers("d", Arrays.asList("-v", version));
    assertEquals(version, pgServer.getOptions().getServerVersion());
  }

  @Test
  public void testRepeatableReadIsolationLevelWithHigherVersions() throws Exception {
    // tests repeatable read isolation_level settings with version 9.1 using
    // connection.isolation_level variable

    restartServerWithDifferentVersion("9.1");

    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("update");
    statements.add(sql4);

    statements.add("transaction");
    statements.add("commit");

    // This will cause error because the server version is 9.1 now,
    // so the psycopg2 will send REPEATABLE READ as the isolation level
    statements.add("transaction");
    statements.add("set isolation_level 2");

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput =
        "(1, 'abcd')\n"
            + "1\n"
            + "2\n"
            + "INVALID_ARGUMENT: Unknown statement: BEGIN ISOLATION LEVEL REPEATABLE READ\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();
    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(4, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(2)).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    // PG Adapter should've received a call to set the isolation level to REPEATABLE READ not
    // SERIALIZABLE
    // because we've set the version to 9.1
    // So, Psycopg2 will not convert REPEATABLE_READ to SERIALIZABLE
    assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("REPEATABLE READ"))
            .count());

    restartServerWithDifferentVersion("1.0");
  }

  @Test
  public void testRepeatableReadIsolationLevelSessionWithHigherVersions() throws Exception {
    // tests repeatable read isolation_level settings with version 9.1 using set_session function

    restartServerWithDifferentVersion("9.1");

    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    statements.add("query");
    statements.add(sql1);

    statements.add("update");
    statements.add(sql2);

    statements.add("update");
    statements.add(sql4);

    statements.add("transaction");
    statements.add("commit");

    // This will cause error because the server version is 9.1 now,
    // so the psycopg2 will send REPEATABLE READ as the isolation level
    statements.add("transaction");
    statements.add("set session isolation_level 2");

    statements.add("query");
    statements.add(sql3);

    statements.add("transaction");
    statements.add("commit");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput =
        "(1, 'abcd')\n"
            + "1\n"
            + "2\n"
            + "INVALID_ARGUMENT: Unknown statement: BEGIN ISOLATION LEVEL REPEATABLE READ\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();
    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(4, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(0)).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(1)).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) requests.get(2)).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    // PG Adapter should've received a call to set the isolation level to REPEATABLE READ not
    // SERIALIZABLE
    // because we've set the version to 9.1
    // So, Psycopg2 will not convert REPEATABLE_READ to SERIALIZABLE
    assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("REPEATABLE READ"))
            .count());

    restartServerWithDifferentVersion("1.0");
  }

  @Test
  public void testIsolationLevelAutocommitInTransactions()
      throws IOException, InterruptedException {
    // tests isolation level autocommit settings using set_isolation_level() function
    List<String> statements = new ArrayList<>();
    String sql1 = "Select * from some_table";
    String sql2 = "insert into some_table(col1, col2) values(value1, value2)";
    String sql3 = "Select * from some_table3";
    String sql4 = "insert into some_table3(col1, col2) values(value1, value2)";

    // no commit request will be sent after this query because autocommit is false
    statements.add("query");
    statements.add(sql1);

    // no commit request will be sent after this update because autocommit is false
    statements.add("update");
    statements.add(sql2);

    // no commit request will be sent after this update because autocommit is false
    statements.add("update");
    statements.add(sql4);

    // 1 commit request will be sent
    statements.add("transaction");
    statements.add("commit");

    statements.add("transaction");
    statements.add("set isolation_level 0");

    // no commit request after this query although autocommit is true because it is a query sql
    statements.add("query");
    statements.add(sql3);

    // 1 commit request will be sent after this update because autocommit is true
    statements.add("update");
    statements.add(sql2);

    // 1 commit request will be sent after this update because autocommit is true
    statements.add("update");
    statements.add(sql4);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql1), createResultSet(1, "abcd")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 1));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSet(2, "pqrs")));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql4), 2));

    String expectedOutput = "(1, 'abcd')\n" + "1\n" + "2\n" + "(2, 'pqrs')\n" + "1\n" + "2\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<AbstractMessage> requests = mockSpanner.getRequests();

    requests =
        requests.stream()
            .filter(request -> !request.getClass().equals(BatchCreateSessionsRequest.class))
            .collect(Collectors.toList());

    assertEquals(9, requests.size());

    assertTrue(requests.get(0).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(0))).getSql().equals(sql1));

    assertTrue(requests.get(1).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(1))).getSql().equals(sql2));

    assertTrue(requests.get(2).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(2))).getSql().equals(sql4));

    assertTrue(requests.get(3).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(4).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(4))).getSql().equals(sql3));
    assertTrue(((ExecuteSqlRequest) (requests.get(4))).getTransaction().hasSingleUse());
    assertTrue(
        ((ExecuteSqlRequest) (requests.get(4))).getTransaction().getSingleUse().hasReadOnly());

    assertTrue(requests.get(5).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(5))).getSql().equals(sql2));

    assertTrue(requests.get(6).getClass().equals(CommitRequest.class));

    assertTrue(requests.get(7).getClass().equals(ExecuteSqlRequest.class));
    assertTrue(((ExecuteSqlRequest) (requests.get(7))).getSql().equals(sql4));

    assertTrue(requests.get(8).getClass().equals(CommitRequest.class));
  }

  @Ignore("To be Removed when the changes in the PR #1949 in the Java Client Library are live")
  @Test
  public void testSetAllPropertiesUsingSetSession() throws Exception {
    List<String> statements = new ArrayList<>();

    String sql = "insert into some_table values(value1, cvalue2)";

    statements.add("transaction");
    statements.add("set session isolation_level 2 readonly False autocommit True");

    statements.add("update");
    statements.add(sql);

    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 10));

    String expectedOutput = "10\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    statements.set(1, "set session isolation_level 3 readonly True autocommit False");

    mockSpanner.clearRequests();

    expectedOutput =
        "FAILED_PRECONDITION: Update statements are not allowed for read-only transactions\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    assertEquals(expectedOutput, actualOutput);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    statements.set(
        1, "set session isolation_level 2 readonly True autocommit False deferrable True");

    mockSpanner.clearRequests();

    // TODO
    expectedOutput = "Some Error\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    assertEquals(expectedOutput, actualOutput);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    statements.set(
        1, "set session isolation_level 1 readonly True autocommit False deferrable False");

    mockSpanner.clearRequests();

    // TODO
    expectedOutput = "Some Error\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    assertEquals(expectedOutput, actualOutput);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    restartServerWithDifferentVersion("9.1");

    statements.set(
        1, "set session isolation_level 2 readonly True autocommit False deferrable False");

    mockSpanner.clearRequests();

    // TODO
    expectedOutput = "Some Error\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    assertEquals(expectedOutput, actualOutput);

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    restartServerWithDifferentVersion("1.0");
  }
}
