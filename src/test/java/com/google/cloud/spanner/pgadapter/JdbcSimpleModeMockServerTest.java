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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ExecuteSqlRequest.QueryMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the native PG JDBC driver in simple query mode. This is similar to the protocol that is
 * used by psql, and for example allows batches to be given as semicolon-separated strings.
 */
@RunWith(JUnit4.class)
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
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    // The statement is sent only once to the mock server in simple query mode.
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(QueryMode.NORMAL, request.getQueryMode());
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testMixedBatch() throws SQLException {
    // TODO: Change this test case once we support mixed batches.
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
    // TODO: Change this test case once we support mixed batches.
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
    // TODO: Change this test case once we support mixed batches.
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
    // TODO: Change this test case once we support mixed batches.
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
