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

import static com.google.cloud.spanner.pgadapter.ITJdbcMetadataTest.getDdlStatements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.trace.v1.TraceServiceClient;
import com.google.cloud.trace.v1.TraceServiceClient.ListTracesPagedResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.devtools.cloudtrace.v1.ListTracesRequest;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.semconv.SemanticAttributes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITOpenTelemetryTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();

  @BeforeClass
  public static void setup() throws IOException {
    IntegrationTest.skipOnEmulator("This test requires credentials");

    OptionsMetadata.Builder openTelemetryOptionsBuilder =
        OptionsMetadata.newBuilder()
            .setProject(testEnv.getProjectId())
            .setEnableOpenTelemetry()
            .setOpenTelemetryTraceRatio(1.0);
    if (testEnv.getCredentials() != null) {
      openTelemetryOptionsBuilder.setCredentials(
          GoogleCredentials.fromStream(Files.newInputStream(Paths.get(testEnv.getCredentials()))));
    }
    OpenTelemetry openTelemetry = Server.setupOpenTelemetry(openTelemetryOptionsBuilder.build());

    testEnv.setUp();
    Database database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(
        database.getId(), ImmutableList.of(), openTelemetry);
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private String getConnectionUrl() {
    return String.format("jdbc:postgresql://%s/", testEnv.getPGAdapterHostAndPort());
  }

  @Test
  public void testSimpleSelect() throws Exception {
    // Generate a unique select statement so it is easy to find in the traces.
    UUID uuid = UUID.randomUUID();
    String sql = "select '" + uuid + "'";
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        assertEquals(uuid.toString(), resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
    flushOpenTelemetry();
    try (TraceServiceClient client = TraceServiceClient.create()) {
      // It can take a few seconds before the trace is visible.
      boolean foundTrace = false;
      for (int attempts = 0; attempts < 20; attempts++) {
        ListTracesPagedResponse response =
            client.listTraces(
                ListTracesRequest.newBuilder()
                    .setProjectId(testEnv.getProjectId())
                    .setFilter(SemanticAttributes.DB_STATEMENT + ":\"" + sql + "\"")
                    .build());
        int size = Iterables.size(response.iterateAll());
        if (size != 0) {
          assertEquals(1, Iterables.size(response.iterateAll()));
          foundTrace = true;
          break;
        } else {
          Thread.sleep(500L);
        }
      }
      assertTrue(foundTrace);
    }
  }

  private void flushOpenTelemetry() {
    OpenTelemetrySdk openTelemetrySdk = (OpenTelemetrySdk) testEnv.getServer().getOpenTelemetry();
    openTelemetrySdk.getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);
  }
}
