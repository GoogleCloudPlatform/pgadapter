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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class IsolationLevelMockServerTest extends AbstractMockServerTest {
  private static final String INSERT = "insert into foo values (1)";

  @Parameter public String preferQueryMode;

  @Parameters(name = "preferQueryMode = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"simple"}, new Object[] {"extended"});
  }

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

  private String createUrl(String database) {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s?preferQueryMode=%s",
        pgServer.getLocalPort(), database, preferQueryMode);
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
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      for (String sql :
          new String[] {
            "show default_transaction_isolation",
            "show transaction isolation level",
            "show transaction_isolation"
          }) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("serializable", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute("set default_transaction_isolation='repeatable read'");
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
      for (String sql :
          new String[] {
            "show default_transaction_isolation",
            "show transaction isolation level",
            "show transaction_isolation"
          }) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testShowTransactionIsolationInTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("begin isolation level repeatable read");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
      for (String sql :
          new String[] {"show transaction isolation level", "show transaction_isolation"}) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute("commit");
      for (String sql :
          new String[] {"show transaction isolation level", "show transaction_isolation"}) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("serializable", rs.getString(1));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testResetDefaultIsolationLevel() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("set default_transaction_isolation='repeatable read'");
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
      mockSpanner.clearRequests();

      stmt.execute("reset default_transaction_isolation");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
    }
  }

  @Test
  public void testSetDefaultIsolationLevelToDefaultWithStartupOption() throws SQLException {
    String url =
        createUrl("my_db") + "&options=-c%20default_transaction_isolation='repeatable read'";
    try (Connection connection = DriverManager.getConnection(url);
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("set default_transaction_isolation = 'serializable'");
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.SERIALIZABLE);
      mockSpanner.clearRequests();

      stmt.execute("set default_transaction_isolation = default");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("repeatable read", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
      mockSpanner.clearRequests();

      // Verify that setting default_transaction_read_only does not clobber
      // default_transaction_isolation.
      stmt.execute("set default_transaction_read_only = off");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("repeatable read", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
    }
  }

  @Test
  public void testSetAndResetDefaultIsolationLevelInTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      // Setting default_transaction_isolation before the first statement in a transaction must not
      // clobber the active transaction's explicit isolation level.
      stmt.execute("begin isolation level repeatable read");
      stmt.execute("set default_transaction_isolation = 'serializable'");
      stmt.execute("set session characteristics as transaction isolation level serializable");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
      for (String sql :
          new String[] {"show transaction isolation level", "show transaction_isolation"}) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
      mockSpanner.clearRequests();

      // Subsequent transaction uses the committed default_transaction_isolation (SERIALIZABLE),
      // and RESET ALL / RESET default_transaction_isolation after a statement in an active
      // transaction must succeed and take effect for the next transaction.
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("set session characteristics as transaction isolation level repeatable read");
      stmt.execute("reset default_transaction_isolation");
      stmt.execute("reset all");
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.SERIALIZABLE);
      mockSpanner.clearRequests();

      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
    }
  }

  @Test
  public void testRollbackDefaultIsolationLevel() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      stmt.execute("begin");
      stmt.execute("set default_transaction_isolation = 'repeatable read'");
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("repeatable read", rs.getString(1));
        assertFalse(rs.next());
      }
      try (ResultSet rs = stmt.executeQuery("show transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute("rollback");

      for (String sql :
          new String[] {
            "show default_transaction_isolation",
            "show transaction isolation level",
            "show transaction_isolation"
          }) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("serializable", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
    }
  }

  @Test
  public void testDatabaseDefaultIsolationLevelRepeatableRead() throws SQLException {
    setDefaultTransactionIsolation("repeatable read");
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
      for (String sql :
          new String[] {
            "show default_transaction_isolation",
            "show transaction isolation level",
            "show transaction_isolation"
          }) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }

      // A plain BEGIN without an explicit isolation level should keep showing repeatable read
      // while sending ISOLATION_LEVEL_UNSPECIFIED on the wire.
      stmt.execute("begin");
      for (String sql :
          new String[] {"show transaction isolation level", "show transaction_isolation"}) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
      mockSpanner.clearRequests();

      // Overriding the session default to serializable and then resetting it should restore
      // repeatable read in SHOW and ISOLATION_LEVEL_UNSPECIFIED on the wire.
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.SERIALIZABLE);
      mockSpanner.clearRequests();

      // BEGIN ISOLATION LEVEL DEFAULT uses the database default (repeatable read / UNSPECIFIED)
      // even when the session default_transaction_isolation is serializable.
      stmt.execute("begin isolation level default");
      for (String sql :
          new String[] {"show transaction isolation level", "show transaction_isolation"}) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
      mockSpanner.clearRequests();

      stmt.execute("reset default_transaction_isolation");
      for (String sql :
          new String[] {
            "show default_transaction_isolation",
            "show transaction isolation level",
            "show transaction_isolation"
          }) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals("repeatable read", rs.getString(1));
          assertFalse(rs.next());
        }
      }
      stmt.execute("begin");
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED);
      mockSpanner.clearRequests();
    }

    // Simulate an out-of-band update of the database default back to serializable.
    setDefaultTransactionIsolation("serializable");
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      try (ResultSet rs = stmt.executeQuery("show default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testSetAndResetTransactionIsolationAndEdgeCases() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my_db"));
        java.sql.Statement stmt = connection.createStatement()) {
      // Unsupported isolation levels on transaction_isolation and reset/default must fail.
      assertThrows(
          SQLException.class, () -> stmt.execute("set transaction_isolation = 'read committed'"));
      assertThrows(
          SQLException.class, () -> stmt.execute("set transaction_isolation = 'read uncommitted'"));
      assertThrows(SQLException.class, () -> stmt.execute("set transaction_isolation to default"));
      assertThrows(SQLException.class, () -> stmt.execute("reset transaction_isolation"));

      // SET transaction_isolation = 'repeatable read' inside a transaction sets the current
      // transaction's isolation level without changing default_transaction_isolation.
      stmt.execute("begin");
      stmt.execute("set transaction_isolation = 'repeatable read'");
      try (ResultSet rs = stmt.executeQuery("show variable transaction isolation level")) {
        assertTrue(rs.next());
        assertEquals("repeatable read", rs.getString(1));
        assertFalse(rs.next());
      }
      try (ResultSet rs = stmt.executeQuery("show variable default_transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("serializable", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
      mockSpanner.clearRequests();

      // After COMMIT, transaction_isolation returns to default_transaction_isolation.
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());

      // RESET ALL before the first statement in an explicit transaction preserves the active
      // transaction's explicit isolation level on both SHOW and Spanner BeginTransaction.
      stmt.execute("set default_transaction_isolation = 'serializable'");
      stmt.execute("begin isolation level repeatable read");
      stmt.execute("reset all");
      try (ResultSet rs = stmt.executeQuery("show transaction_isolation")) {
        assertTrue(rs.next());
        assertEquals("repeatable read", rs.getString(1));
        assertFalse(rs.next());
      }
      stmt.execute(INSERT);
      stmt.execute("commit");
      assertIsolationLevel(IsolationLevel.REPEATABLE_READ);
      mockSpanner.clearRequests();

      // Preparing/describing a SET statement without executing it must not mutate SessionState.
      if ("extended".equals(preferQueryMode)) {
        try (PreparedStatement ps =
            connection.prepareStatement("set default_transaction_isolation = 'repeatable read'")) {
          ps.getParameterMetaData();
        }
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      }

      // Multi-whitespace in SET SESSION CHARACTERISTICS with READ ONLY delegates cleanly.
      stmt.execute(
          "set session characteristics as transaction isolation level repeatable  read, read  only");
      assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
      try (ResultSet rs = stmt.executeQuery("show spanner.readonly")) {
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
      }

      stmt.execute("discard all");
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, connection.getTransactionIsolation());
      try (ResultSet rs = stmt.executeQuery("show spanner.readonly")) {
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
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
