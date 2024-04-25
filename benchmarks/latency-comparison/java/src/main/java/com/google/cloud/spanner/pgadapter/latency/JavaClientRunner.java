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

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.spanner.BenchmarkSessionPoolOptionsHelper;
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
  private final boolean useMultiplexedSessions;
  private final boolean useRandomChannels;
  private final boolean useVirtualThreads;
  private final boolean useLeaderAwareRouting;
  private long numNullValues;
  private long numNonNullValues;

  JavaClientRunner(
      DatabaseId databaseId,
      boolean useMultiplexedSessions,
      boolean useRandomChannels,
      boolean useVirtualThreads,
      boolean useLeaderAwareRouting) {
    this.databaseId = databaseId;
    this.useMultiplexedSessions = useMultiplexedSessions;
    this.useRandomChannels = useRandomChannels;
    this.useVirtualThreads = useVirtualThreads;
    this.useLeaderAwareRouting = useLeaderAwareRouting;
  }

  @Override
  public List<Duration> execute(
      TransactionType transactionType, int numClients, int numOperations, int waitMillis) {
    InstantiatingGrpcChannelProvider.Builder defaultChannelProviderBuilder =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint("staging-wrenchworks.sandbox.googleapis.com:443")
            // .setMaxInboundMessageSize(MAX_MESSAGE_SIZE)
            // .setMaxInboundMetadataSize(MAX_METADATA_SIZE)
            .setPoolSize(Math.min(4, (int) Math.ceil((double) numClients / 10)));

    SpannerOptions.Builder optionsBuilder =
        SpannerOptions.newBuilder()
            .setProjectId(databaseId.getInstanceId().getProject())
            // .setHost("https://staging-wrenchworks.sandbox.googleapis.com")
            .setChannelProvider(defaultChannelProviderBuilder.build())
            .setSessionPoolOption(
                BenchmarkSessionPoolOptionsHelper.getSessionPoolOptions(useMultiplexedSessions))
            .setUseRandomChannel(useRandomChannels)
            .disableDirectPath();
    if (!useLeaderAwareRouting) {
      optionsBuilder.disableLeaderAwareRouting();
    }
    try (Spanner spanner = optionsBuilder.build().getService()) {
      DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);

      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        results.add(
            service.submit(
                () -> runBenchmark(databaseClient, transactionType, numOperations, waitMillis)));
      }
      return collectResults(service, results, numClients, numOperations);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    }
  }

  private List<Duration> runBenchmark(
      DatabaseClient databaseClient,
      TransactionType transactionType,
      int numOperations,
      int waitMillis) {
    List<Duration> results = new ArrayList<>(numOperations);
    // Execute one query to make sure everything has been warmed up.
    executeTransaction(databaseClient, transactionType);

    for (int i = 0; i < numOperations; i++) {
      try {
        randomWait(waitMillis);
        results.add(executeTransaction(databaseClient, transactionType));
        incOperations();
      } catch (InterruptedException interruptedException) {
        throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
      }
    }
    return results;
  }

  private Duration executeTransaction(DatabaseClient client, TransactionType transactionType) {
    Stopwatch watch = Stopwatch.createStarted();
    switch (transactionType) {
      case READ_ONLY:
        executeReadOnlyTransaction(client, transactionType.getSql());
        break;
      case READ_WRITE:
        executeReadWriteTransaction(client, transactionType.getSql());
        break;
    }
    return watch.elapsed();
  }

  private void executeReadOnlyTransaction(DatabaseClient client, String sql) {
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
  }

  private void executeReadWriteTransaction(DatabaseClient client, String sql) {
    client
        .readWriteTransaction()
        .run(
            transaction ->
                transaction.executeUpdate(
                    Statement.newBuilder(sql)
                        .bind("p1")
                        .to(generateRandomString())
                        .bind("p2")
                        .to(ThreadLocalRandom.current().nextInt(100000))
                        .build()));
  }
}
