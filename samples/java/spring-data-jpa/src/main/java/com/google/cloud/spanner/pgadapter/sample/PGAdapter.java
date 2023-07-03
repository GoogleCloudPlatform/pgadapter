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

class PGAdapter {
  private final ProxyServer server;

  public PGAdapter() {
    this.server = startPGAdapter();
  }

  /**
   * Starts PGAdapter in-process and returns a reference to the server. Use this reference to get
   * the port number that was dynamically assigned to PGAdapter, and to gracefully shut down the
   * server when your application shuts down.
   */
  static ProxyServer startPGAdapter() {
    OptionsMetadata options =
        new OptionsMetadata(
            // Start PGAdapter using the default credentials in the environment on port 9432.
            new String[] {"-s", "9432"});
    ProxyServer server = new ProxyServer(options);
    server.startServer();
    server.awaitRunning();

    return server;
  }

  /** Gracefully shuts down PGAdapter. Call this method when the application is stopping. */
  void stopPGAdapter() {
    if (this.server != null) {
      this.server.stopServer();
      SpannerPool.closeSpannerPool();
    }
  }
}
