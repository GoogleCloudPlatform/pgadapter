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

package com.google.cloud.spanner.pgadapter.sample;

import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Strings;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Properties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Util class for managing the in-process PGAdapter instance used by this sample. */
class PGAdapter {
  private final GenericContainer<?> emulator;
  private final ProxyServer server;

  PGAdapter(boolean startEmulator) {
    this.emulator = startEmulator ? startEmulator() : null;
    this.server =
        startPGAdapter(
            startEmulator ? "localhost:" + this.emulator.getMappedPort(9010) : null, startEmulator);
  }

  /**
   * Starts PGAdapter in-process and returns a reference to the server. Use this reference to get
   * the port number that was dynamically assigned to PGAdapter, and to gracefully shut down the
   * server when your application shuts down.
   */
  private static ProxyServer startPGAdapter(String emulatorHost, boolean autoConfigEmulator) {
    // Start PGAdapter using the default credentials of the runtime environment on port a random
    // port.
    OptionsMetadata.Builder builder = OptionsMetadata.newBuilder().setPort(0);
    // autoConfigEmulator ensures that PGAdapter automatically sets up a connection that works with
    // the Emulator. That means:
    // 1. Use plain text instead of SSL.
    // 2. Do not use any credentials.
    // 3. Automatically create the Spanner instance and database that PGAdapter wants to connect to,
    //    so no prior setup is required.
    if (autoConfigEmulator || !Strings.isNullOrEmpty(System.getenv("SPANNER_EMULATOR_HOST"))) {
      builder.autoConfigureEmulator();
    }
    // Set a custom emulator host if the Emulator was started automatically by this application.
    // That means that the Emulator uses a random port number.
    Properties properties = new Properties();
    if (emulatorHost != null) {
      properties.put("endpoint", emulatorHost);
    }
    OptionsMetadata options = builder.build();
    ProxyServer server = new ProxyServer(options, OpenTelemetry.noop(), properties);
    server.startServer();
    server.awaitRunning();

    // Override the port that is set in the application.properties file with the one that was
    // automatically assigned to the in-memory PGAdapter instance.
    System.setProperty("pgadapter.port", String.valueOf(server.getLocalPort()));

    return server;
  }

  /** Starts a Docker container that contains the Cloud Spanner Emulator. */
  private static GenericContainer<?> startEmulator() {
    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator"));
    container.addExposedPort(9010);
    container.setWaitStrategy(Wait.forListeningPorts(9010));
    container.start();

    return container;
  }

  /** Gracefully shuts down PGAdapter. Call this method when the application is stopping. */
  synchronized void shutdown() {
    if (this.server != null) {
      this.server.stopServer();
      this.server.awaitTerminated();
    }
    if (this.emulator != null) {
      this.emulator.stop();
    }
    SpannerPool.closeSpannerPool();
  }
}
