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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
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
        "BEGIN; INSERT INTO FOO VALUES (1); CREATE TABLE foo (id bigint primary key); COMMIT;";
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
        assertTrue(statement.execute("SELECT 2; CREATE TABLE BAR (id bigint primary key);"));
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
            "BEGIN; %s; CREATE TABLE foo (id bigint primary key); %s; %s; COMMIT;",
            INSERT_STATEMENT, UPDATE_STATEMENT, INVALID_DML);
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // The invalid DML statement will cause the batch to fail. The statement is in an explicit
        // transaction block, but that block also contains a DDL statement. That statement auto
        // commits the explicit transaction, which means that we continue with an implicit
        // transaction after that. That transaction is therefore rolled back when the batch fails.
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(exception.getMessage(), exception.getMessage().contains(EXCEPTION.getMessage()));

        // The connection should not be in the aborted transaction state.
        assertTrue(statement.execute("show transaction isolation level"));
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
}
