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

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import io.opentelemetry.api.OpenTelemetry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OpenTelemetryMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    OptionsMetadata options =
        OptionsMetadata.newBuilder()
            .setProject("p")
            .setCredentials(NoCredentials.getInstance())
            .setEnableOpenTelemetry()
            .setOpenTelemetryTraceRatio(1.0)
            .build();
    OpenTelemetry openTelemetry = Server.setupOpenTelemetry(options);
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(), "d", configurator -> {}, openTelemetry);
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  @Test
  public void testSelectInAutoCommit() throws SQLException {
    String sql = "select * from random";
    RandomResultSetGenerator generator = new RandomResultSetGenerator(5, Dialect.POSTGRESQL);
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), generator.generate()));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        while (resultSet.next()) {
          assertEquals(20, resultSet.getMetaData().getColumnCount());
        }
      }
    }
  }
}
