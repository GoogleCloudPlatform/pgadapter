// Copyright 2023 Google LLC
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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.SessionPoolOptions;
import com.google.spanner.v1.BatchCreateSessionsRequest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SessionPoolSettingsTest extends AbstractMockServerTest {
  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        null,
        builder ->
            builder
                .setSessionPoolOptions(
                    SessionPoolOptions.newBuilder()
                        .setMinSessions(20)
                        .setMaxSessions(100)
                        .setWaitForMinSessions(Duration.ofMillis(1000L))
                        .build())
                .setNumChannels(5));
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/my-db", pgServer.getLocalPort());
  }

  @Test
  public void testSessionPoolInitialization() throws SQLException {
    String sql = "SELECT 1";

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(1L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
    // Check that the session pool options are honored.
    assertEquals(5, mockSpanner.countRequestsOfType(BatchCreateSessionsRequest.class));
    for (BatchCreateSessionsRequest createSessionsRequest :
        mockSpanner.getRequestsOfType(BatchCreateSessionsRequest.class)) {
      assertEquals(4, createSessionsRequest.getSessionCount());
    }
  }
}
