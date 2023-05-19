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
import com.google.cloud.spanner.SessionPoolOptions.ReturnPosition;
import com.google.cloud.spanner.connection.SpannerPool;
import java.time.Duration;
import java.util.List;

public class JdbcRunner extends AbstractJdbcRunner {

  JdbcRunner(
      DatabaseId databaseId,
      Boolean useSharedSessions,
      Integer numChannels,
      ReturnPosition sessionReturnPosition) {
    super(databaseId, useSharedSessions, numChannels, sessionReturnPosition);
  }

  @Override
  public List<Duration> execute(String sql, int numClients, int numOperations) {
    try {
      return super.execute(sql, numClients, numOperations);
    } finally {
      SpannerPool.closeSpannerPool();
    }
  }

  @Override
  protected String createUrl() {
    String url = String.format("jdbc:cloudspanner:/%s", databaseId.getName());
    if (useSharedSessions != null) {
      url += ";useSharedSessions=" + useSharedSessions;
    }
    if (numChannels != null) {
      url += ";numChannels=" + numChannels;
    }
    if (sessionReturnPosition != null) {
      url += ";sessionReturnPosition=" + sessionReturnPosition;
    }
    return url;
  }
}
