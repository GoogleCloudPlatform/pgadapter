package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcSimpleModeMockServerTest extends AbstractMockServerTest {
  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
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
  public void testMixedBatch() throws SQLException {
    String sql =
        "CREATE TABLE testtable (id integer PRIMARY KEY, data integer); INSERT INTO testtable (id, data) VALUES (1, 2); INSERT INTO testtable (id, data) VALUES (3, 4);";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertEquals(
            String.format(
                "ERROR: FAILED_PRECONDITION: Executing updates is not allowed for DDL batches. \"%s\"",
                sql),
            exception.getMessage());
      }
    }
  }

  @Test
  public void testSelectInDdlBatch() throws SQLException {
    String sql =
        "CREATE TABLE test (id integer PRIMARY KEY); SELECT * FROM test; SELECT * FROM test;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertEquals(
            String.format(
                "ERROR: FAILED_PRECONDITION: Executing queries is not allowed for DDL batches. \"%s\"",
                sql),
            exception.getMessage());
      }
    }
  }

  @Test
  public void testTransactionStatementsInBatch() throws SQLException {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO users (id, age, name) VALUES (99, 99, 'test'); COMMIT;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertEquals(
            String.format(
                "ERROR: FAILED_PRECONDITION: This connection has an active batch and cannot begin a transaction \"%s\"",
                sql),
            exception.getMessage());
      }
    }
  }

  @Test
  public void testSelectAtStartOfBatch() throws SQLException {
    String sql =
        "SELECT * FROM users; INSERT INTO users (id, age, name) VALUES (99, 99, 'person'); SELECT * FROM users;";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertEquals(
            String.format(
                "ERROR: INVALID_ARGUMENT: Statement type is not supported for batching \"%s\"",
                sql),
            exception.getMessage());
      }
    }
  }
}
