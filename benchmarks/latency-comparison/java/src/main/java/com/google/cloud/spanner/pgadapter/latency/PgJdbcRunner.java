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

package com.google.cloud.spanner.pgadapter.latency;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.time.Duration;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PgJdbcRunner extends AbstractJdbcRunner {
  private ProxyServer proxyServer;

  private final boolean useUnixDomainSockets;

  PgJdbcRunner(DatabaseId databaseId, boolean useUnixDomainSockets) {
    super(databaseId);
    this.useUnixDomainSockets = useUnixDomainSockets;
  }

  @Override
  public List<Duration> execute(
      TransactionType transactionType, int numClients, int numOperations, int waitMillis) {
    // Silence the PGAdapter logging.
    Logger root = Logger.getLogger("");
    root.setLevel(Level.WARNING);
    for (Handler handler : root.getHandlers()) {
      handler.setLevel(Level.WARNING);
    }
    // Start PGAdapter in-process.
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(databaseId.getInstanceId().getProject())
            .setInstance(databaseId.getInstanceId().getInstance())
            .setPort(0)
            .useVirtualThreads()
            .useVirtualGrpcTransportThreads();
    if (!useUnixDomainSockets) {
      builder.disableUnixDomainSockets();
    }
    OptionsMetadata options = builder.build();
    proxyServer = new ProxyServer(options);
    try {
      proxyServer.startServer();
      return super.execute(transactionType, numClients, numOperations, waitMillis);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    } finally {
      proxyServer.stopServer();
    }
  }

  @Override
  protected String createUrl() {
    if (useUnixDomainSockets) {
      return String.format(
          "jdbc:postgresql://localhost/%s?"
              + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
          databaseId.getDatabase(), proxyServer.getLocalPort());
    }
    return String.format(
        "jdbc:postgresql://localhost:%d/%s", proxyServer.getLocalPort(), databaseId.getDatabase());
  }
}
