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

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.BenchmarkSessionPoolOptionsHelper;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.common.io.ByteStreams;
import com.google.spanner.v1.ResultSet;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.protobuf.ProtoUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class GrpcWarmup extends AbstractMockServerTest {

  public static void main(String[] args) {
    runWarmup(50000, true);
  }

  public static void runWarmup(int numIterations, boolean useMultiplexedSessions) {
    try {
      startStaticServer();
      GrpcWarmup grpcWarmup = new GrpcWarmup();
      // grpcWarmup.runWarmupFunction(numIterations);
      grpcWarmup.runWarmupQueries(numIterations, useMultiplexedSessions);
      stopServer();
    } catch (Exception exception) {
      throw SpannerExceptionFactory.asSpannerException(exception);
    }
  }

  private void runWarmupFunction(int numIterations) throws Exception {
    RandomResultSetGenerator generator = new RandomResultSetGenerator(1);
    for (int i = 0; i < numIterations; i++) {
      com.google.spanner.v1.ResultSet resultSet = generator.generate();
      Marshaller<ResultSet> marshaller =
          ProtoUtils.marshaller(com.google.spanner.v1.ResultSet.getDefaultInstance());
      try (InputStream inputStream = marshaller.stream(resultSet)) {
        byte[] bytes = ByteStreams.toByteArray(inputStream);

        com.google.spanner.v1.ResultSet resultSet1 =
            marshaller.parse(new ByteArrayInputStream(bytes));
        if (!resultSet.equals(resultSet1)) {
          throw new IllegalStateException();
        }
      }
    }
  }

  private void runWarmupQueries(int numIterations, boolean useMultiplexedSessions) {
    RandomResultSetGenerator generator = new RandomResultSetGenerator(1);
    try (Spanner spanner =
        SpannerOptions.newBuilder()
            .setHost("http://localhost:" + getPort())
            .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
            .setCredentials(NoCredentials.getInstance())
            .setSessionPoolOption(
                BenchmarkSessionPoolOptionsHelper.getSessionPoolOptions(
                    useMultiplexedSessions, false))
            .build()
            .getService()) {
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("p", "i", "d"));

      for (int i = 0; i < numIterations; i++) {
        mockSpanner.putStatementResult(
            StatementResult.query(SELECT_RANDOM_STATEMENT, generator.generate()));
        try (com.google.cloud.spanner.ResultSet resultSet =
            client.singleUse().executeQuery(SELECT1_STATEMENT)) {
          while (resultSet.next()) {}
        }
      }
    }
  }
}
