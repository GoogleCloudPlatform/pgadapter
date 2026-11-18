// Copyright 2026 Google LLC
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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IsolationLevelMockServerTest extends AbstractMockServerTest {
  private static final String INSERT = "insert into foo values (1)";
  private static final String REPEATABLE_READ_DATABASE = "repeatable_read_db";

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter without a default database.
    doStartMockSpannerAndPgAdapterServers(null, builder -> {});

    mockSpanner.putStatementResults(StatementResult.update(Statement.of(INSERT), 1L));
  }

  @After
  public void removeExecutionTimes() {
    mockSpanner.removeAllExecutionTimes();
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements.
   */
  private String createUrl(String database) {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s?preferQueryMode=simple",
        pgServer.getLocalPort(), database);
  }

  /**
   * Connects to a database with default isolation level repeatable read. The default isolation
   * level is cached per database, so this uses a separate database.
   */
  private static Connection connectToRepeatableReadDatabase(String url) throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.detectDialectResult(Dialect.POSTGRESQL, IsolationLevel.REPEATABLE_READ));
    try {
      return DriverManager.getConnection(url);
    } finally {
      mockSpanner.putStatementResult(StatementResult.detectDialectResult(Dialect.POSTGRESQL));
      mockSpanner.clearRequests();
    }
  }

  private static String show(java.sql.Statement stmt, String sql) throws SQLException {
    try (ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      String value = rs.getString(1);
      assertFalse(rs.next());
      return value;
    }
  }

  @Test
  public void testUnspecifiedIsolationLevel() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
  }

  @Test
  public void testDatabaseDefaultIsolationLevel() throws SQLException {
    try (Connection connection =
            connectToRepeatableReadDatabase(createUrl(REPEATABLE_READ_DATABASE));
        java.sql.Statement stmt = connection.createStatement()) {
      assertEquals("repeatable read", show(stmt, "show default_transaction_isolation"));
      assertEquals("repeatable read", show(stmt, "show transaction_isolation"));
      assertEquals("repeatable read", show(stmt, "show transaction isolation level"));
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());

      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
  }

  @Test
  public void testDatabaseDefaultIsolationLevelInAutocommit() throws SQLException {
    try (Connection connection =
            connectToRepeatableReadDatabase(createUrl(REPEATABLE_READ_DATABASE));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute(INSERT);
    }

    assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
  }

  @Test
  public void testDefaultIsolationLevelInUrlOverridesDatabaseDefault() throws SQLException {
    String url =
        createUrl(REPEATABLE_READ_DATABASE)
            + "&options=-c%20default_transaction_isolation=serializable";
    try (Connection connection = connectToRepeatableReadDatabase(url);
        java.sql.Statement stmt = connection.createStatement()) {
      assertEquals("serializable", show(stmt, "show default_transaction_isolation"));

      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.SERIALIZABLE);
  }

  @Test
  public void testSetDefaultIsolationLevelOverridesDatabaseDefault() throws SQLException {
    try (Connection connection =
            connectToRepeatableReadDatabase(createUrl(REPEATABLE_READ_DATABASE));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("set default_transaction_isolation = 'serializable'");
      assertEquals("serializable", show(stmt, "show default_transaction_isolation"));
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.SERIALIZABLE);

      mockSpanner.clearRequests();
      stmt.execute("set default_transaction_isolation to default");
      assertEquals("repeatable read", show(stmt, "show default_transaction_isolation"));
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
    }
  }

  @Test
  public void testBeginIsolationLevelOverridesDatabaseDefault() throws SQLException {
    try (Connection connection =
            connectToRepeatableReadDatabase(createUrl(REPEATABLE_READ_DATABASE));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("begin isolation level serializable");
      assertEquals("serializable", show(stmt, "show transaction_isolation"));
      assertEquals("repeatable read", show(stmt, "show default_transaction_isolation"));
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertEquals("repeatable read", show(stmt, "show transaction_isolation"));
    }

    assertIsolationLevel(IsolationLevel.SERIALIZABLE);
  }

  @Test
  public void testCheckSetRestoreIsolationLevelWithDatabaseDefault() throws SQLException {
    // Uses the default extended query mode of the PG JDBC driver, like Spring and HikariCP.
    String url =
        String.format(
            "jdbc:postgresql://localhost:%d/%s", pgServer.getLocalPort(), REPEATABLE_READ_DATABASE);
    try (Connection connection = connectToRepeatableReadDatabase(url)) {
      int previousIsolationLevel = connection.getTransactionIsolation();
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, previousIsolationLevel);

      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      connection.setAutoCommit(false);
      try (java.sql.Statement stmt = connection.createStatement()) {
        stmt.executeUpdate(INSERT);
      }
      connection.commit();
      assertIsolationLevel(IsolationLevel.SERIALIZABLE);

      connection.setAutoCommit(true);
      connection.setTransactionIsolation(previousIsolationLevel);
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
    }
  }

  @Test
  public void testDefaultIsolationLevelInUrl() throws SQLException {
    String url =
        createUrl("my_db") + "&options=-c%20default_transaction_isolation='repeatable read'";
    try (Connection connection = DriverManager.getConnection(url);
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
  }

  @Test
  public void testSetDefaultIsolationLevel() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("set default_transaction_isolation='repeatable read'");

      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
  }

  @Test
  public void testSetSessionCharacteristics() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("set session characteristics as transaction isolation level repeatable read");

      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
  }

  @Test
  public void testSetTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("begin");
      stmt.execute("set transaction isolation level repeatable read");
      stmt.execute(INSERT);
      stmt.execute("commit");
    }

    assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
  }

  @Test
  public void testShowDefaultIsolationLevel() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute("set default_transaction_isolation='repeatable read'");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("repeatable read", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  private void assertIsolationLevel(IsolationLevel expected) {
    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    ExecuteSqlRequest request = requests.get(0);
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(expected, request.getTransaction().getBegin().getIsolationLevel());
  }
}
