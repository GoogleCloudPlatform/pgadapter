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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.RandomResultSetGenerator;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;

@RunWith(JUnit4.class)
public class EmulatedPgFdwMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter without a default database.
    doStartMockSpannerAndPgAdapterServers(null, ImmutableList.of());
  }

  @After
  public void removeExecutionTimes() {
    mockSpanner.removeAllExecutionTimes();
  }

  private String createUrl() {
    return createUrl("postgres_fdw");
  }

  private String createUrl(String applicationName) {
    return String.format(
        "jdbc:postgresql://localhost:%d/d?preferQueryMode=simple&%s=%s&%s=090000",
        pgServer.getLocalPort(),
        PGProperty.APPLICATION_NAME.getName(),
        applicationName,
        PGProperty.ASSUME_MIN_SERVER_VERSION.getName());
  }

  @Test
  public void testTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(1, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertTrue(
        mockSpanner
            .getRequestsOfType(BeginTransactionRequest.class)
            .get(0)
            .getOptions()
            .hasReadOnly());
  }

  @Test
  public void testTransactionReadWrite() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createUrl("postgres_fdw readonly=false"))) {
      connection.setAutoCommit(false);
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(BeginTransactionRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertTrue(
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getTransaction().hasBegin());
    assertTrue(
        mockSpanner
            .getRequestsOfType(ExecuteSqlRequest.class)
            .get(0)
            .getTransaction()
            .getBegin()
            .hasReadWrite());
  }

  @Test
  public void testSelectRandom() throws SQLException {
    int numRows = new Random().nextInt(2000) + 1;
    int totalRows = 0;
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from random"),
            new RandomResultSetGenerator(numRows).generate()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.createStatement().execute("begin transaction");
      connection.createStatement().execute("declare c1 cursor for select * from random");
      while (true) {
        boolean foundRows = false;
        try (ResultSet resultSet = connection.createStatement().executeQuery("fetch 100 c1")) {
          while (resultSet.next()) {
            foundRows = true;
            totalRows++;
          }
        }
        if (!foundRows) {
          break;
        }
      }
      connection.createStatement().execute("close c1");
    }
    assertEquals(numRows, totalRows);
  }

  @Test
  public void testOnlyInTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .execute("declare c1 cursor for select * from random"));
      assertEquals(SQLState.NoActiveSqlTransaction.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testNotInAbortedTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);
      assertThrows(PSQLException.class, () -> connection.createStatement().execute("select foo"));
      PSQLException exception =
          assertThrows(
              PSQLException.class,
              () ->
                  connection
                      .createStatement()
                      .execute("declare c1 cursor for select * from random"));
      assertEquals(SQLState.InFailedSqlTransaction.toString(), exception.getSQLState());
    }
  }

  @Test
  public void testCommitAndRollbackClosesCursor() throws SQLException {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from random"), new RandomResultSetGenerator(10).generate()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      for (boolean commit : new boolean[] {true, false}) {
        connection.createStatement().execute("declare c1 cursor for select * from random");
        try (ResultSet resultSet = connection.createStatement().executeQuery("fetch 1 c1")) {
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
        }
        if (commit) {
          connection.commit();
        } else {
          connection.rollback();
        }
        assertThrows(
            PSQLException.class, () -> connection.createStatement().executeQuery("fetch 1 c1"));
      }
    }
  }
}
