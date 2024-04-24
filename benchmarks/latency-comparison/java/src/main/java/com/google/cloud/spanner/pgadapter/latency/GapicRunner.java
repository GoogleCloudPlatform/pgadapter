// Copyright 2024 Google LLC
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
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.v1.SpannerClient;
import com.google.cloud.spanner.v1.SpannerSettings;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.BatchCreateSessionsRequest;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class GapicRunner extends AbstractRunner {
  private final DatabaseId databaseId;
  private final int numChannels;
  private final boolean useMultiplexedSessions;
  private long numNullValues;
  private long numNonNullValues;

  private List<SpannerClient> clients;
  private Map<SpannerClient, List<Session>> sessions;
  private Session multiplexedSession;

  GapicRunner(DatabaseId databaseId, int numChannels, boolean useMultiplexedSessions) {
    this.databaseId = databaseId;
    this.numChannels = numChannels;
    this.useMultiplexedSessions = useMultiplexedSessions;
  }

  @Override
  public List<Duration> execute(
      TransactionType transactionType, int numClients, int numOperations, int waitMillis) {

    try {
      this.clients = new ArrayList<>(numChannels);
      for (int i = 0; i < numChannels; i++) {
        this.clients.add(
            SpannerClient.create(
                SpannerSettings.newBuilder()
                    .setEndpoint("staging-wrenchworks.sandbox.googleapis.com:443")
                    .build()));
      }
      // Create the required session(s).
      createSessions(clients, numClients);
      int numSessionsPerChannel = numClients / clients.size();

      List<Future<List<Duration>>> results = new ArrayList<>(numClients);
      ExecutorService service = Executors.newFixedThreadPool(numClients);
      for (int client = 0; client < numClients; client++) {
        final int clientIndex = client % numChannels;
        final int sessionIndex = client % numSessionsPerChannel;
        results.add(
            service.submit(
                () ->
                    runBenchmark(
                        clients.get(clientIndex),
                        sessionIndex,
                        transactionType,
                        numOperations,
                        waitMillis)));
      }
      return collectResults(service, results, numClients, numOperations);
    } catch (Throwable t) {
      throw SpannerExceptionFactory.asSpannerException(t);
    } finally {
      if (this.clients != null) {
        for (SpannerClient client : this.clients) {
          client.close();
        }
      }
    }
  }

  private List<Duration> runBenchmark(
      SpannerClient spannerClient,
      int sessionIndex,
      TransactionType transactionType,
      int numOperations,
      int waitMillis) {
    List<Duration> results = new ArrayList<>(numOperations);
    // Execute one query to make sure everything has been warmed up.
    executeTransaction(spannerClient, sessionIndex, transactionType);

    for (int i = 0; i < numOperations; i++) {
      try {
        randomWait(waitMillis);
        results.add(executeTransaction(spannerClient, sessionIndex, transactionType));
        incOperations();
      } catch (InterruptedException interruptedException) {
        throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
      }
    }
    return results;
  }

  private void createSessions(List<SpannerClient> clients, int numSessions) {
    if (useMultiplexedSessions) {
      this.multiplexedSession =
          clients
              .get(0)
              .createSession(
                  CreateSessionRequest.newBuilder()
                      .setDatabase(databaseId.getName())
                      .setSession(Session.newBuilder().setMultiplexed(true).build())
                      .build());
    } else {
      this.sessions = new HashMap<>();
      int numSessionsPerChannel = numSessions / clients.size();
      for (SpannerClient client : clients) {
        this.sessions.put(
            client,
            client
                .batchCreateSessions(
                    BatchCreateSessionsRequest.newBuilder()
                        .setDatabase(databaseId.getName())
                        .setSessionCount(numSessionsPerChannel)
                        .build())
                .getSessionList());
      }
    }
  }

  private Duration executeTransaction(
      SpannerClient client, int sessionIndex, TransactionType transactionType) {
    Stopwatch watch = Stopwatch.createStarted();
    switch (transactionType) {
      case READ_ONLY:
        executeReadOnlyTransaction(client, sessionIndex, transactionType.getSql());
        break;
      case READ_WRITE:
        executeReadWriteTransaction(client, transactionType.getSql());
        break;
    }
    return watch.elapsed();
  }

  private void executeReadOnlyTransaction(SpannerClient client, int sessionIndex, String sql) {
    Session session =
        this.useMultiplexedSessions
            ? this.multiplexedSession
            : this.sessions.get(client).get(sessionIndex);
    com.google.spanner.v1.ResultSet resultSet =
        client.executeSql(
            ExecuteSqlRequest.newBuilder()
                .setSession(session.getName())
                .setSql(sql)
                .setParams(
                    Struct.newBuilder()
                        .putFields(
                            "p1",
                            Value.newBuilder()
                                .setStringValue(
                                    String.valueOf(ThreadLocalRandom.current().nextInt(100000)))
                                .build())
                        .build())
                .putParamTypes("p1", Type.newBuilder().setCode(TypeCode.INT64).build())
                .build());
    for (ListValue row : resultSet.getRowsList()) {
      for (int col = 0; col < resultSet.getMetadata().getRowType().getFieldsCount(); col++) {
        if (row.getValues(col).hasNullValue()) {
          numNullValues++;
        } else {
          numNonNullValues++;
        }
      }
    }
  }

  private void executeReadWriteTransaction(SpannerClient client, String sql) {
    throw SpannerExceptionFactory.newSpannerException(ErrorCode.UNIMPLEMENTED, "Not supported");
  }
}
