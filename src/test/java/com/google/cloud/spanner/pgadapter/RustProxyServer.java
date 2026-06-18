// Copyright 2026 Google LLC
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

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class RustProxyServer extends ProxyServer {
  private static final Logger logger = Logger.getLogger(RustProxyServer.class.getName());
  private final OptionsMetadata options;
  private final int spannerPort;
  private Process process;
  private int port;

  public RustProxyServer(OptionsMetadata options, int spannerPort) {
    super(options, io.opentelemetry.api.OpenTelemetry.noop());
    this.options = options;
    this.spannerPort = spannerPort;
  }

  @Override
  public void startServer() {
    try {
      java.net.ServerSocket serverSocket = new java.net.ServerSocket(0);
      this.port = serverSocket.getLocalPort();
      serverSocket.close();

      String binaryPath = "./src/rust/spanner-pgadapter/target/debug/spanner-pgadapter";
      List<String> command = new ArrayList<>();
      command.add(binaryPath);
      if (options.getDefaultInstanceId() != null) {
        command.add("-p");
        command.add(options.getDefaultInstanceId().getProject());
        command.add("-i");
        command.add(options.getDefaultInstanceId().getInstance());
      }
      if (options.getDefaultDatabaseId() != null) {
        command.add("-d");
        command.add(options.getDefaultDatabaseId().getDatabase());
      }
      command.add("-s");
      command.add(String.valueOf(port));
      command.add("-e");
      command.add(String.format("http://localhost:%d", spannerPort));

      logger.info("Starting Rust PGAdapter: " + String.join(" ", command));

      java.io.File logFile = new java.io.File("target/rust-pgadapter.log");
      process =
          new ProcessBuilder(command)
              .redirectErrorStream(true)
              .redirectOutput(ProcessBuilder.Redirect.to(logFile))
              .start();

      // Wait a tiny bit to make sure it started up
      Thread.sleep(1500);

    } catch (Exception e) {
      throw new RuntimeException("Failed to start Rust Proxy Server", e);
    }
  }

  @Override
  public void stopServer() {
    if (process != null) {
      process.destroyForcibly();
    }
  }

  @Override
  public int getLocalPort() {
    return this.port;
  }
}
