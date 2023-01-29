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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLWarning;

@RunWith(JUnit4.class)
public class EmulatedPgbenchMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode. It also adds 'pgbench' as the application name, which will make PGAdapter automatically
   * recognize the connection as a pgbench connection.
   */
  private String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/db?preferQueryMode=simple&%s=pgbench&%s=090000",
        pgServer.getLocalPort(),
        PGProperty.APPLICATION_NAME.getName(),
        PGProperty.ASSUME_MIN_SERVER_VERSION.getName());
  }

  @Test
  public void testClientDetectionAndHint() throws Exception {
    WellKnownClient.PGBENCH.reset();
    // Verify that we get the notice response that indicates that the client was automatically
    // detected.
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLWarning warning = connection.getWarnings();
      assertNotNull(warning);
      PSQLWarning psqlWarning = (PSQLWarning) warning;
      assertNotNull(psqlWarning.getServerErrorMessage());
      assertNotNull(psqlWarning.getServerErrorMessage().getSQLState());
      assertArrayEquals(
          SQLState.Success.getBytes(),
          psqlWarning.getServerErrorMessage().getSQLState().getBytes(StandardCharsets.UTF_8));
      assertEquals(
          "Detected connection from pgbench", psqlWarning.getServerErrorMessage().getMessage());
      assertEquals(
          ClientAutoDetector.PGBENCH_USAGE_HINT + "\n",
          psqlWarning.getServerErrorMessage().getHint());

      assertNull(warning.getNextWarning());
    }

    // Verify that creating a second connection directly afterwards with pgbench does not repeat the
    // hint.
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      SQLWarning warning = connection.getWarnings();
      assertNull(warning);
    }
  }

  @Test
  public void testErrorHint() throws SQLException {
    // Verify that any error message includes the pgbench usage hint.
    String sql = "select * from foo where bar=1";
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(sql),
            Status.INVALID_ARGUMENT.withDescription("test error").asRuntimeException()));
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      PSQLException exception =
          assertThrows(PSQLException.class, () -> connection.createStatement().execute(sql));
      assertNotNull(exception.getServerErrorMessage());
      assertEquals(
          String.format("test error - Statement: '%s'", sql),
          exception.getServerErrorMessage().getMessage());
      assertEquals(
          ClientAutoDetector.PGBENCH_USAGE_HINT, exception.getServerErrorMessage().getHint());
    }
  }
}
