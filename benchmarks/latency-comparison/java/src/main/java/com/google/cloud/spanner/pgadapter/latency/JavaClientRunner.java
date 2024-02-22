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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class JavaClientRunner extends AbstractRunner {
  private final DatabaseId databaseId;
  private long numNullValues;
  private long numNonNullValues;

  JavaClientRunner(DatabaseId databaseId) {
    this.databaseId = databaseId;
  }

  @Override
  public List<Duration> execute(String sql, int numClients, int numOperations, int waitMillis) {
    SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(databaseId.getInstanceId().getProject()).build();
    try (Spanner spanner = options.getService()) {
      DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);

      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        results.add(
            service.submit(() -> runBenchmark(databaseClient, sql, numOperations, waitMillis)));
      }
      return collectResults(service, results, numClients, numOperations);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }

  private List<Duration> runBenchmark(
      DatabaseClient databaseClient, String sql, int numOperations, int waitMillis) {
    List<Duration> results = new ArrayList<>(numOperations);
    // Execute one query to make sure everything has been warmed up.
    executeQuery(databaseClient, sql);

    for (int i = 0; i < numOperations; i++) {
      try {
        randomWait(waitMillis);
        results.add(executeQuery(databaseClient, sql));
        incOperations();
      } catch (InterruptedException interruptedException) {
        throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
      }
    }
    return results;
  }

  private Duration executeQuery(DatabaseClient client, String sql) {
    Stopwatch watch = Stopwatch.createStarted();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(sql)
                    .bind("p1")
                    .to(ThreadLocalRandom.current().nextInt(100000))
                    .build())) {
      while (resultSet.next()) {
        for (int i = 0; i < resultSet.getColumnCount(); i++) {
          if (resultSet.isNull(i)) {
            numNullValues++;
          } else {
            numNonNullValues++;
          }
        }
      }
    }
    return watch.elapsed();
  }
}
