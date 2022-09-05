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

package com.google.cloud.spanner.pgadapter;

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.TRANSACTION_ABORTED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.AbstractMessage;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BatchCreateSessionsRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.RollbackRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DdlTransactionModeAutocommitExplicitTest
    extends DdlTransactionModeAutocommitImplicitTest {
  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        "d", Collections.singleton("-ddl=AutocommitExplicitTransaction"));
  }

  @Test
  public void testMixedBatchWithExplicitTransaction() throws SQLException {
    String sql =
        "BEGIN;INSERT INTO FOO VALUES (1);CREATE TABLE foo (id bigint primary key);COMMIT;";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        assertTrue(statement.execute("show transaction isolation level"));
        assertNotNull(statement.getResultSet());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testDdlInExplicitTransaction() throws SQLException {
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Start an explicit transaction before executing batch
        assertFalse(statement.execute("BEGIN;SELECT 1"));
        assertEquals(0, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Execute batch
        assertTrue(statement.execute("SELECT 2;CREATE TABLE BAR (id bigint primary key);"));
        assertNotNull(statement.getResultSet());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Commit the explicit transaction.
        assertFalse(statement.execute("COMMIT"));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    assertTrue(requests.get(0).getTransaction().getBegin().hasReadWrite());
    // Verify that the explicit transaction is active while batch starts
    assertEquals(SELECT2.getSql(), requests.get(1).getSql());
    assertFalse(requests.get(1).getTransaction().hasBegin());
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testDdlInExplicitTransactionSwitchedToImplicit() throws SQLException {
    String sql =
        String.format(
            "BEGIN;%s;CREATE TABLE foo (id bigint primary key);%s;%s;COMMIT;",
            INSERT_STATEMENT, UPDATE_STATEMENT, INVALID_DML);
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // The invalid DML statement will cause the batch to fail. The statement is in an explicit
        // transaction block, but that block also contains a DDL statement. That statement auto
        // commits the explicit transaction, which means that we continue with an implicit
        // transaction after that. That transaction is therefore rolled back when the batch fails.
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertEquals("ERROR: Statement is invalid.", exception.getMessage());

        // The connection should now be in the aborted transaction state.
        SQLException abortedException =
            assertThrows(
                SQLException.class, () -> statement.execute("show transaction isolation level"));
        assertTrue(
            abortedException.getMessage(),
            abortedException.getMessage().contains(TRANSACTION_ABORTED_ERROR));
      }
    }

    // The first statement is executed as a single DML statement.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    assertTrue(requests.get(0).getTransaction().getBegin().hasReadWrite());
    // The transaction is automatically committed by the CREATE TABLE statement.
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));

    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchDmlRequests.size());
    assertEquals(2, batchDmlRequests.get(0).getStatementsCount());
    assertTrue(batchDmlRequests.get(0).getTransaction().hasBegin());
    assertTrue(batchDmlRequests.get(0).getTransaction().getBegin().hasReadWrite());
    // The transaction fails and is automatically rolled back.
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testSingleDdlStatementInExplicitTransaction() throws SQLException {
    String sql = "CREATE TABLE foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("begin;");
        assertFalse(statement.execute(sql));
        statement.execute("commit");
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testMultipleDdlStatementsInExplicitTransaction() throws SQLException {
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // PGAdapter will automatically convert this transaction into a DDL batch.
        statement.execute("begin;");
        assertFalse(statement.execute("CREATE TABLE foo (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE bar (id bigint primary key)"));
        statement.execute("commit");
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
  }

  @Test
  public void testMultipleDdlAndDmlStatementsInExplicitTransaction() throws SQLException {
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // PGAdapter will automatically convert this transaction into a DDL batch.
        statement.execute("begin;");
        assertFalse(statement.execute("CREATE TABLE foo (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE bar (id bigint primary key)"));
        // The following DML statement will auto-commit the DDL batch above.
        assertEquals(1, statement.executeUpdate(INSERT_STATEMENT.getSql()));
        statement.execute("commit;");
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    // The DDL statements should be batched together.
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
  }

  @Test
  public void testMultipleDdlStatementsInExplicitTransactionWithRollback() throws SQLException {
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // PGAdapter will automatically convert this transaction into a DDL batch.
        statement.execute("begin;");
        assertFalse(statement.execute("CREATE TABLE foo (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE bar (id bigint primary key)"));
        statement.execute("rollback;");
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    // There should be no DDL requests.
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testMultipleDdlAndDmlStatementsInExplicitTransactionWithRollback()
      throws SQLException {
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // PGAdapter will automatically convert this transaction into a DDL batch.
        statement.execute("begin;");
        assertFalse(statement.execute("CREATE TABLE foo (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE bar (id bigint primary key)"));
        // The following DML statement will auto-commit the DDL batch above. This means that the
        // rollback will have no effect on the DDL statements.
        assertEquals(1, statement.executeUpdate(INSERT_STATEMENT.getSql()));
        statement.execute("rollback;");
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    // The DDL statements should be batched together.
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
    // The DML statement should not be committed.
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInvalidDdlStatementInExplicitTransaction() throws SQLException {
    addDdlExceptionToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // PGAdapter will automatically convert this transaction into a DDL batch.
        statement.execute("begin;");
        // This statement would normally fail, but as it is batched, the error will be deferred to
        // the commit statement.
        assertFalse(statement.execute("CREATE TABLE foo (id bigint)"));
        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute("commit"));
        assertTrue(exception.getMessage(), exception.getMessage().contains("Statement is invalid"));
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    // NOTE: The DDL request is actually sent to Cloud Spanner, but it is not executed because it
    // returns an error. The mock Admin server does not register requests that fail directly, which
    // is why this count is zero.
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testInvalidDdlStatementHalfwayInExplicitTransaction() throws SQLException {
    // Add a success and a failure for DDL statements to the mock server. This means that the first
    // DDL statement/batch will succeed, while the second will fail.
    addDdlResponseToSpannerAdmin();
    addDdlExceptionToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // PGAdapter will automatically convert this transaction into a DDL batch.
        statement.execute("begin;");
        assertFalse(statement.execute("CREATE TABLE foo (id bigint primary key)"));
        // This auto-commits the above DDL statement. The statement itself is also automatically
        // committed when the next DDL batch starts.
        assertEquals(1, statement.executeUpdate(INSERT_STATEMENT.getSql()));

        // This starts a new DDL batch. This batch will fail, but the error is only surfaced when
        // the batch is committed, which is at the next non-DDL statement.
        assertFalse(statement.execute("CREATE TABLE bar (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE baz (id bigint)"));
        SQLException exception =
            assertThrows(SQLException.class, () -> statement.execute(INSERT_STATEMENT.getSql()));
        assertTrue(exception.getMessage(), exception.getMessage().contains("Statement is invalid"));
        // Trying to execute further statements should return an aborted transaction error.
        SQLException abortedException =
            assertThrows(SQLException.class, () -> statement.execute(SELECT1.getSql()));
        assertTrue(
            abortedException.getMessage(),
            abortedException.getMessage().contains(TRANSACTION_ABORTED_ERROR));

        // Committing the transaction is accepted, but is silently converted to a rollback.
        assertFalse(statement.execute("commit;"));
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    // NOTE: The DDL request is actually sent to Cloud Spanner, but it is not executed because it
    // returns an error. The mock Admin server does not register requests that fail directly, which
    // is why this count is one (the first batch of DDL statements did succeed).
    assertEquals(1, updateDatabaseDdlRequests.size());
    // The DML statement between the two DDL batches is automatically committed.
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testMultipleDdlBatchesInExplicitTransaction() throws SQLException {
    // We need two responses, as we will be sending two batches.
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("begin;");

        assertEquals(1, statement.executeUpdate(INSERT_STATEMENT.getSql()));

        // This will auto-commit the transaction and start a DDL batch.
        assertFalse(statement.execute("CREATE TABLE foo1 (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE bar1 (id bigint primary key)"));

        // The following DML statement will auto-commit the DDL batch above.
        assertEquals(1, statement.executeUpdate(INSERT_STATEMENT.getSql()));

        // This will start another DDL batch.
        assertFalse(statement.execute("CREATE TABLE foo2 (id bigint primary key)"));
        assertFalse(statement.execute("CREATE TABLE bar2 (id bigint primary key)"));

        // This will once again auto-commit the DDL batch.
        assertEquals(1, statement.executeUpdate(INSERT_STATEMENT.getSql()));

        // Only the last DML statement will be rolled back.
        statement.execute("rollback;");
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    // There should be two separate DDL batches.
    assertEquals(2, updateDatabaseDdlRequests.size());

    assertEquals(2, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE foo1 (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));
    assertEquals(
        "CREATE TABLE bar1 (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(1));

    assertEquals(2, updateDatabaseDdlRequests.get(1).getStatementsCount());
    assertEquals(
        "CREATE TABLE foo2 (id bigint primary key)",
        updateDatabaseDdlRequests.get(1).getStatements(0));
    assertEquals(
        "CREATE TABLE bar2 (id bigint primary key)",
        updateDatabaseDdlRequests.get(1).getStatements(1));

    // Get all requests (except BatchCreateSession).
    List<AbstractMessage> requests =
        mockSpanner.getRequests().stream()
            .filter(message -> !(message instanceof BatchCreateSessionsRequest))
            .collect(Collectors.toList());
    // The order of requests should be:
    // 1. DML (with begin)
    // 2. Commit
    // 3. DML (with begin)
    // 4. Commit
    // 5. DML (with begin)
    // 6. Rollback
    assertEquals(6, requests.size());
    assertEquals(ExecuteSqlRequest.class, requests.get(0).getClass());
    assertEquals(CommitRequest.class, requests.get(1).getClass());
    assertEquals(ExecuteSqlRequest.class, requests.get(2).getClass());
    assertEquals(CommitRequest.class, requests.get(3).getClass());
    assertEquals(ExecuteSqlRequest.class, requests.get(4).getClass());
    assertEquals(RollbackRequest.class, requests.get(5).getClass());
  }
}
