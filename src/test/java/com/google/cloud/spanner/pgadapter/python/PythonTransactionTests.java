package com.google.cloud.spanner.pgadapter.python;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
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
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
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
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : requests) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(true, sql.contains(sql1));
    Assert.assertEquals(true, sql.contains(sql2));
    Assert.assertEquals(true, sql.contains(sql3));
    Assert.assertEquals(true, sql.contains(sql4));
    Assert.assertEquals(true, sql.contains(sql5));
    Assert.assertEquals(true, sql.contains(sql6));

    String transactionIdForSql2 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql2))
            .findAny()
            .get()
            .getTransaction()
            .getId()
            .toStringUtf8();
    String transactionIdForSql3 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql3))
            .findAny()
            .get()
            .getTransaction()
            .getId()
            .toStringUtf8();

    Assert.assertEquals(true, transactionIdForSql2.equals(transactionIdForSql3));

    String transactionIdForSql5 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql5))
            .findAny()
            .get()
            .getTransaction()
            .getId()
            .toStringUtf8();
    String transactionIdForSql6 =
        requests.stream()
            .filter(req -> req.getSql().equals(sql6))
            .findAny()
            .get()
            .getTransaction()
            .getId()
            .toStringUtf8();

    Assert.assertEquals(true, transactionIdForSql5.equals(transactionIdForSql6));
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
    for (AbstractMessage request : mockSpanner.getRequests())
      System.out.println(request.getClass());
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(2, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(2, Collections.frequency(sql, sql4));
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
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(2, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(2, Collections.frequency(sql, sql4));
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

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
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

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
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
    statements.add(sql3);

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
            + "(2, 'pqrs')\n"
            + "FAILED_PRECONDITION: Update statements are not allowed for read-only transactions\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));
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
    statements.add(sql3);

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
            + "(2, 'pqrs')\n"
            + "FAILED_PRECONDITION: Update statements are not allowed for read-only transactions\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));
  }

  // Isolation Levels
  // ISOLATION_LEVEL_AUTOCOMMIT -> 0
  // ISOLATION_LEVEL_READ_COMMITTED -> 1
  // ISOLATION_LEVEL_REPEATABLE_READ -> 2
  // ISOLATION_LEVEL_SERIALIZABLE -> 3
  // ISOLATION_LEVEL_READ_UNCOMMITTED -> 4
  @Test
  public void testIsolationLevelErrorInTransactions() throws IOException, InterruptedException {
    // tests isolation_level settings using connection.isolation_level variable
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
    statements.add("set isolation_level 1");

    // query won't be executed because the previous setting would've thrown error
    statements.add("query");
    statements.add(sql3);

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
            + "INVALID_ARGUMENT: Unknown value for TRANSACTION: ISOLATION LEVEL READ COMMITTED\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(0, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    mockSpanner.clearRequests();
    statements.set(9, "set isolation_level 4");

    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    sql.clear();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(0, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    Assert.assertEquals(
        2,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("READ COMMITTED"))
            .count());
  }

  @Test
  public void testIsolationLevelSessionErrorInTransactions()
      throws IOException, InterruptedException {
    // tests isolation_level settings using set_session() function
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
    statements.add("set session isolation_level 1");

    // query won't be executed because the previous setting would've thrown error
    statements.add("query");
    statements.add(sql3);

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
            + "INVALID_ARGUMENT: Unknown value for TRANSACTION: ISOLATION LEVEL READ COMMITTED\n";
    String actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(0, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    mockSpanner.clearRequests();
    statements.set(9, "set session isolation_level 4");

    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    for (AbstractMessage request : mockSpanner.getRequests())
      System.out.println(request.getClass());

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    sql.clear();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(0, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    Assert.assertEquals(
        2,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("READ COMMITTED"))
            .count());
  }

  private void restartServerWithDifferentVersion(String version) throws Exception {
    stopMockSpannerAndPgAdapterServers();
    doStartMockSpannerAndPgAdapterServers("d", Arrays.asList("-v", version));
    Assert.assertEquals(version, pgServer.getOptions().getServerVersion());
  }

  @Test
  public void testIsolationLevelInTransactions() throws IOException, InterruptedException {
    // tests isolation_level settings using connection.isolation_level variable
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
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    mockSpanner.clearRequests();
    statements.set(9, "set isolation_level 3");

    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    sql.clear();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    Assert.assertEquals(
        2,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("SERIALIZABLE"))
            .count());
  }

  @Test
  public void testIsolationLevelSessionInTransactions() throws IOException, InterruptedException {
    // tests isolation_level settings using set_session() function
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
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    mockSpanner.clearRequests();
    statements.set(9, "set session isolation_level 3");

    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(4, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    sql.clear();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    Assert.assertEquals(
        2,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("SERIALIZABLE"))
            .count());
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
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(3, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(6, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(2, Collections.frequency(sql, sql2));
    Assert.assertEquals(1, Collections.frequency(sql, sql3));
    Assert.assertEquals(2, Collections.frequency(sql, sql4));
  }

  @Test
  public void testIsolationLevelWithDifferentVersion() throws Exception {
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

    // This will not cause error because the server version is 9.1 now,
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
    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    Assert.assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    List<String> sql = new ArrayList<>();

    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      sql.add(request.getSql());
    }

    Assert.assertEquals(1, Collections.frequency(sql, sql1));
    Assert.assertEquals(1, Collections.frequency(sql, sql2));
    Assert.assertEquals(0, Collections.frequency(sql, sql3));
    Assert.assertEquals(1, Collections.frequency(sql, sql4));

    Assert.assertEquals(
        1,
        getWireMessagesOfType(QueryMessage.class).stream()
            .filter(qm -> qm.toString().contains("REPEATABLE READ"))
            .count());

    restartServerWithDifferentVersion("1.0");
  }

  @Ignore("To be Removed when changes in Client Library are live")
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

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    statements.set(1, "set session isolation_level 3 readonly True autocommit False");

    mockSpanner.clearRequests();

    expectedOutput =
        "FAILED_PRECONDITION: Update statements are not allowed for read-only transactions\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    statements.set(
        1, "set session isolation_level 2 readonly True autocommit False deferrable True");

    mockSpanner.clearRequests();

    // TODO
    expectedOutput = "Some Error\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    statements.set(
        1, "set session isolation_level 1 readonly True autocommit False deferrable False");

    mockSpanner.clearRequests();

    // TODO
    expectedOutput = "Some Error\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    restartServerWithDifferentVersion("9.1");

    statements.set(
        1, "set session isolation_level 2 readonly True autocommit False deferrable False");

    mockSpanner.clearRequests();

    // TODO
    expectedOutput = "Some Error\n";
    actualOutput = executeTransactions(pgServer.getLocalPort(), statements);

    Assert.assertEquals(expectedOutput, actualOutput);

    Assert.assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    Assert.assertEquals(0, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));

    restartServerWithDifferentVersion("1.0");
  }
}
