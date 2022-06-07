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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.RollbackRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DdlTransactionModeBatchTest extends DdlTransactionModeNoneTest {

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers("d", Collections.singleton("-ddl=Batch"));
  }

  @Override
  @Test
  public void testDdlBatch() throws SQLException {
    addDdlResponseToSpannerAdmin();
    String sql =
        "CREATE TABLE foo (id bigint primary key); CREATE TABLE bar (id bigint primary key);";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }

  @Test
  public void testMixedBatchDdlFirst() throws SQLException {
    String sql =
        "CREATE TABLE foo (id bigint primary key); INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2;";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are not allowed in mixed batches or transactions."));
      }
    }
  }

  @Test
  public void testMixedBatchDmlFirst() throws SQLException {
    String sql =
        "INSERT INTO FOO VALUES (1); UPDATE FOO SET BAR=1 WHERE BAZ=2; CREATE TABLE foo (id bigint primary key);";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are not allowed in mixed batches or transactions."));
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }

  @Test
  public void testMixedBatchDqlFirst() throws SQLException {
    String sql = "SELECT 1; CREATE TABLE foo (id bigint primary key);";
    addDdlResponseToSpannerAdmin();
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are not allowed in mixed batches or transactions."));
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
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are not allowed in mixed batches or transactions."));
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
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
                .contains("DDL statements are not allowed in mixed batches or transactions."));

        exception =
            assertThrows(
                SQLException.class, () -> statement.execute("show transaction isolation level"));
        assertTrue(
            exception.getMessage(), exception.getMessage().contains(TRANSACTION_ABORTED_ERROR));
      }
    }

    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
  }
}
