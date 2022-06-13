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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.DatabaseName;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.SessionName;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EmulatedPsqlMockServerTest extends AbstractMockServerTest {
  private static final String INSERT1 = "insert into foo values (1)";
  private static final String INSERT2 = "insert into foo values (2)";

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter without a default database.
    doStartMockSpannerAndPgAdapterServers(
        null, ImmutableList.of("-q", "-disable_auto_detect_client"));

    mockSpanner.putStatementResults(
        StatementResult.update(Statement.of(INSERT1), 1L),
        StatementResult.update(Statement.of(INSERT2), 1L));
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql.
   */
  private String createUrl(String database) {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s?preferQueryMode=simple",
        pgServer.getLocalPort(), database);
  }

  @Test
  public void testConnectToDifferentDatabases() throws SQLException {
    final ImmutableList<String> databases = ImmutableList.of("db1", "db2");
    for (String database : databases) {
      try (Connection connection = DriverManager.getConnection(createUrl(database))) {
        connection.createStatement().execute(INSERT1);
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(databases.size(), requests.size());
    for (int i = 0; i < requests.size(); i++) {
      assertEquals(databases.get(i), SessionName.parse(requests.get(i).getSession()).getDatabase());
    }
  }

  @Test
  public void testConnectToFullDatabasePath() throws Exception {
    String databaseName =
        "projects/full-path-test-project/instances/full-path-test-instance/databases/full-path-test-database";
    // Note that we need to URL encode the database name as it contains multiple forward slashes.
    try (Connection connection =
        DriverManager.getConnection(
            createUrl(URLEncoder.encode(databaseName, StandardCharsets.UTF_8.name())))) {
      connection.createStatement().execute(INSERT1);
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    SessionName sessionName = SessionName.parse(requests.get(0).getSession());
    DatabaseName gotDatabaseName =
        DatabaseName.of(
            sessionName.getProject(), sessionName.getInstance(), sessionName.getDatabase());
    assertEquals(DatabaseName.parse(databaseName), gotDatabaseName);
  }

  @Test
  public void testTwoInserts() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute(String.format("%s; %s", INSERT1, INSERT2));
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT1, request.getStatements(0).getSql());
    assertEquals(INSERT2, request.getStatements(1).getSql());
  }
}
