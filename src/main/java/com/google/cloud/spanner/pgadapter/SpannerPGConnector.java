// Copyright 2025 Google LLC
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

import com.google.cloud.spanner.pgadapter.logging.DefaultLogConfiguration;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import io.opentelemetry.api.OpenTelemetry;

/**
 * A simplified connector executable that sets up PGAdapter and runs a command (like psql) against
 * it. * Usage: spgc <command> [args...] Example: spgc psql -d
 * "projects/my-project/instances/my-inst/databases/my-db"
 */
public class SpannerPGConnector {

  public static void main(String[] args) {
    // Scan for unsupported connection overrides
    for (String arg : args) {
      if (arg.equals("-h")
          || arg.startsWith("--host")
          || arg.equals("-p")
          || arg.startsWith("--port")) {
        System.err.println("Error: Explicit host/port flags (-h/-p) are not supported.");
        System.err.println("spanner-pg-connector manages the connection routing dynamically.");
        System.exit(1);
      }
    }

    if (args.length == 0) {
      System.out.println("Usage: spgc <command> [args...]");
      System.out.println("Example: spgc psql -d \"projects/p/instances/i/databases/d\"");
      System.exit(1);
    }

    try {
      DefaultLogConfiguration.disableLogging();
      OptionsMetadata.Builder builder =
          OptionsMetadata.newBuilder()
              .setPort(0)
              .setSslMode(SslMode.Disable)
              .disableUnixDomainSockets();

      String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
      if (projectId != null) {
        builder.setProject(projectId);
      }
      String instanceId = System.getenv("SPANNER_INSTANCE");
      if (instanceId != null) {
        builder.setInstance(instanceId);
      }

      if (System.getenv("SPANNER_EMULATOR_HOST") != null) {
        builder.autoConfigureEmulator();
      }

      OptionsMetadata options = builder.build();

      OpenTelemetry openTelemetry = Server.setupOpenTelemetry(options);
      ProxyServer proxyServer = new ProxyServer(options, openTelemetry);
      proxyServer.startServer();

      int exitCode = 0;
      try {
        exitCode = Server.runCommand(proxyServer, null, args);
      } finally {
        proxyServer.stopServer();
      }
      System.exit(exitCode);

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
