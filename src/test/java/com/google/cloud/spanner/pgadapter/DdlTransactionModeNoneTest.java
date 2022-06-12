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

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DdlTransactionModeNoneTest extends AbstractMockServerTest {
  @Parameter public String preferQueryMode;

  @Parameters(name = "preferQueryMode = {0}")
  public static List<Object[]> data() {
    String[] queryModes = {"extended", "simple"};
    List<Object[]> parameters = new ArrayList<>();
    for (String queryMode : queryModes) {
      parameters.add(new Object[] {queryMode});
    }
    return parameters;
  }

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers("d", Collections.singleton("-ddl=Single"));
  }

  protected String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferQueryMode=%s",
        pgServer.getLocalPort(), preferQueryMode);
  }

  @Test
  public void testSingleDdlStatement() throws SQLException {
    String sql = "CREATE TABLE foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        assertFalse(statement.execute(sql));
        assertEquals(0, statement.getUpdateCount());
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, updateDatabaseDdlRequests.size());
    assertEquals(1, updateDatabaseDdlRequests.get(0).getStatementsCount());
    assertEquals(
        "CREATE TABLE foo (id bigint primary key)",
        updateDatabaseDdlRequests.get(0).getStatements(0));
  }

  @Test
  public void testSingleDdlStatementInExplicitTransaction() throws SQLException {
    String sql = "CREATE TABLE foo (id bigint primary key)";
    addDdlResponseToSpannerAdmin();

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("begin");
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are only allowed outside explicit transactions."));
      }
    }

    List<UpdateDatabaseDdlRequest> updateDatabaseDdlRequests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(request -> request instanceof UpdateDatabaseDdlRequest)
            .map(UpdateDatabaseDdlRequest.class::cast)
            .collect(Collectors.toList());
    assertEquals(0, updateDatabaseDdlRequests.size());
  }

  @Test
  public void testDdlBatch() throws SQLException {
    String sql =
        "CREATE TABLE foo (id bigint primary key); CREATE TABLE bar (id bigint primary key);";
    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (Statement statement = connection.createStatement()) {
        SQLException exception = assertThrows(SQLException.class, () -> statement.execute(sql));
        assertTrue(
            exception.getMessage(),
            exception
                .getMessage()
                .contains("DDL statements are only allowed outside batches and transactions."));
      }
    }
  }
}
