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

import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.cloud.spanner.connection.RandomResultSetGenerator;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.spanner.v1.ResultSet;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.protobuf.ProtoUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class GrpcWarmup extends AbstractMockServerTest {

  public static void runWarmup(int numIterations) {
    try {
      startStaticServer();
      runWarmupFunction(numIterations);
      stopServer();
    } catch (Exception exception) {
      throw SpannerExceptionFactory.asSpannerException(exception);
    }
  }

  private static void runWarmupFunction(int numIterations) throws Exception {
    RandomResultSetGenerator generator = new RandomResultSetGenerator(1);
    for (int i = 0; i < numIterations; i++) {
      com.google.spanner.v1.ResultSet resultSet = generator.generate();
      Marshaller<ResultSet> marshaller =
          ProtoUtils.marshaller(com.google.spanner.v1.ResultSet.getDefaultInstance());
      try (InputStream inputStream = marshaller.stream(resultSet)) {
        byte[] bytes = ByteStreams.toByteArray(inputStream);

        Stopwatch watch = Stopwatch.createStarted();
        com.google.spanner.v1.ResultSet resultSet1 =
            marshaller.parse(new ByteArrayInputStream(bytes));
        if (!resultSet.equals(resultSet1)) {
          throw new IllegalStateException();
        }
      }
    }
  }
}
