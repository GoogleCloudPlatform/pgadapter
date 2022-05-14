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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
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
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DdlTransactionModeAutocommitImplicitTest extends DdlTransactionModeBatchTest {
  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        "d", Collections.singleton("-ddl=AutocommitImplicitTransaction"));
  }

  @Test
  public void testMixedBatchDdlFirst() throws SQLException {
    String sql =
        "CREATE TABLE foo (id bigint primary key); INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2;";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testMixedBatchDmlFirst() throws SQLException {
    String sql =
        "INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2; CREATE TABLE foo (id bigint primary key);";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(1, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testMixedBatchDqlFirst() throws SQLException {
    String sql = "SELECT 1; CREATE TABLE foo (id bigint primary key);";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(sql));
        assertNotNull(statement.getResultSet());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertTrue(
        mockSpanner
            .getRequestsOfType(BeginTransactionRequest.class)
            .get(0)
            .getOptions()
            .hasReadOnly());
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testMixedBatchDqlFirstDmlAfter() throws SQLException {
    String sql = "SELECT 1; CREATE TABLE foo (id bigint primary key); INSERT INTO FOO VALUES (1);";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(sql));
        assertNotNull(statement.getResultSet());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // We get 2 commit requests, because the first implicit transaction is automatically committed
    // by the DDL statement. The DML statement is also automatically committed at the end of the
    // batch.
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testMixedBatchWithExplicitTransaction() throws SQLException {
    String sql =
        "BEGIN; INSERT INTO FOO VALUES (1); CREATE TABLE foo (id bigint primary key); COMMIT;";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are only allowed outside explicit transactions."));

        exception =
            assertThrows(
                SQLException.class, () -> statement.execute("show transaction isolation level"));
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains(IntermediateStatement.TRANSACTION_ABORTED_ERROR));
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testMixedBatch() throws SQLException {
    String sql =
        "CREATE TABLE foo (id bigint primary key); INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2;";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());

        // getMoreResults() returns false as the next result is an update count.
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());

        // getMoreResults() returns false as the next result is an update count.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));

    // Verify that the DML statements were batched together by PgAdapter.
    List<ExecuteBatchDmlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testErrorHandlingOfDdl() throws SQLException {
    addDdlExceptionToSpannerAdmin();
    String sql = String.format("%s; %s; %s", INSERT_STATEMENT, INVALID_DDL, UPDATE_STATEMENT);
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception.getMessage().contains("INVALID_ARGUMENT: Statement is invalid."));
      }
    }

    // Verify that the execution is stopped after the first error.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(0).getSql());

    // The first insert statement is committed when the implicit transaction encounters a DDL
    // statement. This deviates from PostgreSQL, as PostgreSQL would have included the DDL statement
    // in the implicit transaction, and then rolled back the transaction when the DDL statement
    // failed.
    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testSelectAndDdlInBatch() throws SQLException {
    String sql = "SELECT 1; SELECT 2; CREATE TABLE foo (id bigint primary key);";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(sql));

        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));

    // The first SELECT statement should start a read-only implicit transaction.
    assertEquals(1, mockSpanner.getRequestsOfType(BeginTransactionRequest.class).size());
    BeginTransactionRequest beginRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginRequest.getOptions().hasReadOnly());

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasId());
    assertEquals(SELECT2.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasId());
  }

  @Test
  public void testDdlAndSelectInBatch() throws SQLException {
    String sql = "CREATE TABLE foo (id bigint primary key); SELECT 1; SELECT 2;";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result was either an update count or there
        // was no result. Statement#getUpdateCount() returns 0 if there was no result.
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    // The first statement after the DDL statement should start an implicit transaction.
    // That transaction will be a read-only transaction, and the begin of those are not inlined
    // with the first statement.
    assertTrue(requests.get(0).getTransaction().hasId());
    assertEquals(SELECT2.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasId());
    assertEquals(1, mockSpanner.getRequestsOfType(BeginTransactionRequest.class).size());
    BeginTransactionRequest beginRequest =
        mockSpanner.getRequestsOfType(BeginTransactionRequest.class).get(0);
    assertTrue(beginRequest.getOptions().hasReadOnly());
  }

  @Test
  public void testDdlInExplicitTransaction() throws SQLException {
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Start an explicit transaction before executing batch
        assertFalse(statement.execute("BEGIN; SELECT 1"));
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
        SQLException exception =
            assertThrows(
                SQLException.class,
                () -> statement.execute("SELECT 2; CREATE TABLE BAR (id bigint primary key);"));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are only allowed outside explicit transactions."));
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(2, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasBegin());
    // Verify that the explicit transaction is active while batch starts
    assertEquals(SELECT2.getSql(), requests.get(1).getSql());
    assertFalse(requests.get(1).getTransaction().hasBegin());
    assertTrue(requests.get(1).getTransaction().hasId());
  }
}
