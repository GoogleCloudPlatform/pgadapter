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
import org.springframework.stereotype.Component;

@Component
public class PGAdapter {
  static {
    try {
      Class.forName(org.postgresql.Driver.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load JDBC driver: " + e.getMessage(), e);
    }
  }

  final SpannerProperties spannerProperties;

  final ProxyServer server;

  public PGAdapter(SpannerProperties spannerProperties) {
    this.spannerProperties = spannerProperties;
    this.server =
        startPGAdapter(
            spannerProperties.getProject(),
            spannerProperties.getInstance(),
            spannerProperties.getCredentials());
  }

  public String getConnectionUrl() {
    return String.format(
        "jdbc:postgresql://localhost/%s?"
            + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
            + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
        spannerProperties.getDatabase(), server.getLocalPort());
  }

  /**
   * Starts PGAdapter in-process and returns a reference to the server. Use this reference to
   * gracefully shut down the server when your application shuts down.
   *
   * @param project the Google Cloud project that PGAdapter should connect to
   * @param instance the Cloud Spanner instance that PGAdapter should connect to
   * @param credentials the full path of a credentials file that PGAdapter should use, or <code>null
   *     </code> if PGAdapter should use the application default credentials
   */
  static ProxyServer startPGAdapter(String project, String instance, String credentials) {
    OptionsMetadata options =
        new OptionsMetadata(
            // Check whether we should start PGAdapter with the default credentials or a specific
            // credentials file.
            Strings.isNullOrEmpty(credentials)
                ? new String[] {
                  "-p", project,
                  "-i", instance,
                  "-s", "0" // Start PGAdapter on any available port.
                }
                : new String[] {
                  "-p", project,
                  "-i", instance,
                  "-c", credentials,
                  "-s", "0" // Start PGAdapter on any available port.
                });
    ProxyServer server = new ProxyServer(options);
    server.startServer();

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
