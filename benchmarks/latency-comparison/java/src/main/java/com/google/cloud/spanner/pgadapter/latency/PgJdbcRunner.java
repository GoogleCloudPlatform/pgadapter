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

public class PgJdbcRunner extends AbstractJdbcRunner {
  private ProxyServer proxyServer;

  PgJdbcRunner(DatabaseId databaseId) {
    super(databaseId);
  }

  @Override
  public List<Duration> execute(String sql, int numClients, int numOperations) {
    // Start PGAdapter in-process.
    OptionsMetadata options =
        new OptionsMetadata(
            new String[] {
              "-p", databaseId.getInstanceId().getProject(),
              "-i", databaseId.getInstanceId().getInstance()
            });
    proxyServer = new ProxyServer(options);
    try {
      proxyServer.startServer();
      return super.execute(sql, numClients, numOperations);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    } finally {
      proxyServer.stopServer();
    }
  }

  @Override
  protected String createUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s", proxyServer.getLocalPort(), databaseId.getDatabase());
  }
}
