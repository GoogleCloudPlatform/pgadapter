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

import static com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.TRANSACTION_ABORTED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.RollbackRequest;
import io.grpc.Status;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ErrorHandlingTest extends AbstractMockServerTest {
  private static final String INVALID_SELECT = "SELECT * FROM unknown_table";

  @Parameter public String preferQueryMode;

  @Parameters(name = "preferQueryMode = {0}")
  public static Object[] data() {
    return new Object[] {"extended", "simple"};
  }

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void setupErrorResults() {
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(INVALID_SELECT), Status.NOT_FOUND.asRuntimeException()));
  }

  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferQueryMode=%s",
        pgServer.getLocalPort(), preferQueryMode);
  }

  @Test
  public void testInvalidQueryNoTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLException exception =
          assertThrows(
              SQLException.class, () -> connection.createStatement().executeQuery(INVALID_SELECT));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));

      // The connection should be usable, as there was no transaction.
      assertTrue(connection.createStatement().execute("SELECT 1"));
    }
  }

  @Test
  public void testInvalidQueryInTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      SQLException exception =
          assertThrows(
              SQLException.class, () -> connection.createStatement().executeQuery(INVALID_SELECT));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));

      // The connection should be in the aborted state.
      exception =
          assertThrows(SQLException.class, () -> connection.createStatement().execute("SELECT 1"));
      assertTrue(
          exception.getMessage(), exception.getMessage().contains(TRANSACTION_ABORTED_ERROR));

      // Rolling back the transaction should bring the connection back to a usable state.
      connection.rollback();
      assertTrue(connection.createStatement().execute("SELECT 1"));
    }
  }

  @Test
  public void testCommitAbortedTransaction() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      connection.setAutoCommit(false);

      SQLException exception =
          assertThrows(
              SQLException.class, () -> connection.createStatement().executeQuery(INVALID_SELECT));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));

      // The connection should be in the aborted state.
      exception =
          assertThrows(SQLException.class, () -> connection.createStatement().execute("SELECT 1"));
      assertTrue(
          exception.getMessage(), exception.getMessage().contains(TRANSACTION_ABORTED_ERROR));

      // Committing the transaction will actually execute a rollback.
      connection.commit();
    }
    // Check that we only received a rollback and no commit.
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }
}
