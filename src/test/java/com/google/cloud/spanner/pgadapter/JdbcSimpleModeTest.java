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
import static org.junit.Assert.assertTrue;

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests using the PG JDBC driver in simple mode. In simple mode the JDBC driver behaves very much
 * the same as psql. The JDBC driver will by default use extended mode, and it is very uncommon to
 * configure the JDBC driver to use simple mode. The intention behind these tests is therefore not
 * to verify that PgAdapter works with the JDBC driver, but rather that PgAdapter works correctly
 * with clients that use simple mode (such as psql).
 */
@RunWith(JUnit4.class)
public class JdbcSimpleModeTest extends AbstractMockServerTest {

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use simple mode instead
   * of extended mode for queries and DML statements.
   */
  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferQueryMode=simple", pgServer.getLocalPort());
  }

  @Test
  public void testQuery() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result is an update count or no result.
        assertFalse(
            statement.execute(String.format("%s; %s;", INSERT_STATEMENT, UPDATE_STATEMENT)));

        // Note that we have sent two DML statements to the database in one string. These should be
        // treated as separate statements, and there should therefore be two results coming back
        // from the server. That is; The first update count should be 1 (the INSERT), and the second
        // should be 2 (the UPDATE).
        assertEquals(1, statement.getUpdateCount());

        // The following is a prime example of how not to design an API, but this is how JDBC works.
        // getMoreResults() returns true if the next result is a ResultSet. However, if the next
        // result is an update count, it returns false, and we have to check getUpdateCount() to
        // verify whether there were any more results.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // Verify that the DML statements were batched together by PgAdapter.
    List<ExecuteBatchDmlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, requests.size());
    ExecuteBatchDmlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT_STATEMENT.getSql(), request.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), request.getStatements(1).getSql());
  }

  @Test
  public void testJdbcBatch() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        statement.addBatch(INSERT_STATEMENT.getSql());
        statement.addBatch(UPDATE_STATEMENT.getSql());
        int[] updateCounts = statement.executeBatch();

        assertEquals(2, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(2, updateCounts[1]);
      }
    }

    // The PostgreSQL JDBC driver will send the DML statements as separated statements to PG when
    // executing a batch using simple mode. This means that Spanner will receive two separate DML
    // requests.
    assertTrue(mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).isEmpty());
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);

    // The server will also receive a 'SELECT 1' statement from PgAdapter, as PgAdapter calls
    // connection.isAlive() before using it.
    assertEquals(3, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(1).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(2).getSql());
  }

  @Test
  public void testTwoQueries() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns true if the result is a result set.
        assertTrue(statement.execute("SELECT 1; SELECT 2;"));

        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() returns true if the next result is a ResultSet.
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

    // The server will also receive a 'SELECT 1' statement from PgAdapter, as PgAdapter calls
    // connection.isAlive() before using it.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertEquals(SELECT1.getSql(), requests.get(1).getSql());
    assertEquals(SELECT2.getSql(), requests.get(2).getSql());
  }

  @Test
  public void testQueryAndDml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns true if the result is a result set.
        assertTrue(statement.execute(String.format("%s; %s;", SELECT2, UPDATE_STATEMENT)));

        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }

        // getMoreResults() returns false as the next result is an update count.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // The server will also receive a 'SELECT 1' statement from PgAdapter, as PgAdapter calls
    // connection.isAlive() before using it.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertEquals(SELECT2.getSql(), requests.get(1).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(2).getSql());
  }

  @Test
  public void testDmlAndQuery() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // execute returns false if the result is an update count or no result.
        assertFalse(statement.execute(String.format("%s; %s;", UPDATE_STATEMENT, SELECT2)));
        assertEquals(2, statement.getUpdateCount());

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

    // The server will also receive a 'SELECT 1' statement from PgAdapter, as PgAdapter calls
    // connection.isAlive() before using it.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(1).getSql());
    assertEquals(SELECT2.getSql(), requests.get(2).getSql());
  }

  @Test
  public void testBeginAndDml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // batch a BEGIN [TRANSACTION] statement together with an update statement.
        assertFalse(statement.execute(String.format("BEGIN; %s;", UPDATE_STATEMENT)));
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Execute an insert statement as part of the transaction.
        assertFalse(statement.execute(INSERT_STATEMENT.getSql()));
        assertEquals(1, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        statement.execute("COMMIT");
      }
    }

    // The server will also receive a 'SELECT 1' statement from PgAdapter, as PgAdapter calls
    // connection.isAlive() before using it. This should happen before the transaction is started.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(3, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasSingleUse());

    // The first statement in the transaction will include the BeginTransaction option.
    assertEquals(UPDATE_STATEMENT.getSql(), requests.get(1).getSql());
    assertTrue(requests.get(1).getTransaction().hasBegin());
    assertEquals(INSERT_STATEMENT.getSql(), requests.get(2).getSql());
    assertTrue(requests.get(2).getTransaction().hasId());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testCompleteTransactionAsBatch() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Send a complete transaction as a single batch.
        assertFalse(
            statement.execute(
                String.format("BEGIN; %s; %s; COMMIT", INSERT_STATEMENT, UPDATE_STATEMENT)));
        // The BEGIN statement should not return anything.
        assertEquals(0, statement.getUpdateCount());
        // INSERT
        assertFalse(statement.getMoreResults());
        assertEquals(1, statement.getUpdateCount());
        // UPDATE
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());
        // COMMIT
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // The server will also receive a 'SELECT 1' statement from PgAdapter, as PgAdapter calls
    // connection.isAlive() before using it. This should happen before the transaction is started.
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    assertEquals(SELECT1.getSql(), requests.get(0).getSql());
    assertTrue(requests.get(0).getTransaction().hasSingleUse());

    // As the two DML statements are adjacent in the batch, they can be combined into a BatchDML
    // request. This is the first statement in the transaction and will include the BeginTransaction
    // option.
    List<ExecuteBatchDmlRequest> batchDmlRequests =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class);
    assertEquals(1, batchDmlRequests.size());
    ExecuteBatchDmlRequest batchDmlRequest = batchDmlRequests.get(0);
    assertTrue(batchDmlRequest.getTransaction().hasBegin());
    assertEquals(INSERT_STATEMENT.getSql(), batchDmlRequest.getStatements(0).getSql());
    assertEquals(UPDATE_STATEMENT.getSql(), batchDmlRequest.getStatements(1).getSql());

    List<CommitRequest> commitRequests = mockSpanner.getRequestsOfType(CommitRequest.class);
    assertEquals(1, commitRequests.size());
  }

  @Test
  public void testTwoDdlStatements() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // execute returns false if the result is an update count or no result.
        assertFalse(
            statement.execute(String.format("%s; %s;", "CREATE TABLE FOO", "CREATE TABLE BAR")));
        assertEquals(0, statement.getUpdateCount());

        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());

        // getMoreResults() should now return false. We should also check getUpdateCount() as that
        // method should return -1 to indicate that there is also no update count available.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    // Verify that the two DDL statements are sent as one batch.
    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(r -> r instanceof UpdateDatabaseDdlRequest)
            .map(r -> (UpdateDatabaseDdlRequest) r)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals("CREATE TABLE FOO", request.getStatements(0));
    assertEquals("CREATE TABLE BAR", request.getStatements(1));
  }
}
