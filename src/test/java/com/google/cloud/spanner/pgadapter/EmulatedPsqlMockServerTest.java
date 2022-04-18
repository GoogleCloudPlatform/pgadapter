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
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    doStartMockSpannerAndPgAdapterServers(ImmutableList.of("-q"));

    mockSpanner.putStatementResults(
        StatementResult.update(Statement.of(INSERT1), 1L),
        StatementResult.update(Statement.of(INSERT2), 1L));
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
  public void testTwoInserts() throws SQLException {
    String sql = "insert into foo values (1); insert into foo values (2);";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
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
