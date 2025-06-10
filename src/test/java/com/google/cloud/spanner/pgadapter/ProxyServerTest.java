// Copyright 2024 Google LLC
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

import static com.google.cloud.spanner.pgadapter.ProxyServer.CONNECTION_HANDLERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.TestOptionsMetadataBuilder;
import com.google.common.base.Stopwatch;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProxyServerTest {

  @Test
  public void testProbeConnection() throws Exception {
    assertEquals(0, CONNECTION_HANDLERS.size());
    // This test verifies that doing a simple TCP aliveness probe to check whether PGAdapter is
    // running can be done without any errors.
    ProxyServer server = new ProxyServer(OptionsMetadata.newBuilder().setPort(0).build());
    server.startServer();
    server.awaitRunning();

    try (Socket ignore = new Socket("localhost", server.getLocalPort())) {
      // Do nothing, just verify that we can connect without any errors.
      Stopwatch stopwatch = Stopwatch.createStarted();
      //noinspection StatementWithEmptyBody
      while (stopwatch.elapsed(TimeUnit.SECONDS) < 3 && CONNECTION_HANDLERS.isEmpty()) {}
      assertEquals(1, CONNECTION_HANDLERS.size());
    }
    Stopwatch stopwatch = Stopwatch.createStarted();
    //noinspection StatementWithEmptyBody
    while (stopwatch.elapsed(TimeUnit.SECONDS) < 3 && !CONNECTION_HANDLERS.isEmpty()) {}
    assertEquals(0, CONNECTION_HANDLERS.size());
    server.stopServer();
    server.awaitTerminated();
  }

  @Test
  public void testFailedStartProxyServer() {
    ProxyServer server = new ProxyServer(OptionsMetadata.newBuilder().setPort(0).build());
    server.startServer();

    // Try to start another server on the same port.
    TestOptionsMetadataBuilder builder = new TestOptionsMetadataBuilder();
    ProxyServer server2 =
        new ProxyServer(
            builder
                .setStartupTimeout(Duration.ofMillis(10L))
                .setPort(server.getLocalPort())
                .build());
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, server2::startServer);
    assertTrue(
        exception.getMessage(),
        exception
            .getMessage()
            .contains(
                "Expected the service InnerService [FAILED] to be RUNNING, but the service has FAILED"));

    server.stopServer();
  }
}
