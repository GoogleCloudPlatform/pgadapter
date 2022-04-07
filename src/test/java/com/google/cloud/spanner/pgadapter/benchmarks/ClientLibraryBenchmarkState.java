// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.benchmarks;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockDatabaseAdminServiceImpl;
import com.google.cloud.spanner.MockOperationsServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import com.google.spanner.admin.database.v1.InstanceName;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.threeten.bp.Duration;

@State(Scope.Benchmark)
public class ClientLibraryBenchmarkState {
  protected static final Statement SELECT1 = Statement.of("SELECT 1");
  private static final ResultSetMetadata SELECT1_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("C")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT1_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("1").build())
                  .build())
          .setMetadata(SELECT1_METADATA)
          .build();

  protected volatile MockSpannerServiceImpl mockSpanner;
  protected volatile MockOperationsServiceImpl mockOperationsService;
  protected volatile MockDatabaseAdminServiceImpl mockDatabaseAdmin;
  private volatile Server spannerServer;

  private Spanner spanner;
  private DatabaseClient client;

  @Setup(Level.Trial)
  public void createClient() throws Exception {
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0D); // We don't want any unpredictable aborted transactions.
    mockSpanner.putStatementResult(StatementResult.query(SELECT1, SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.detectDialectResult(Dialect.POSTGRESQL));

    mockOperationsService = new MockOperationsServiceImpl();
    mockDatabaseAdmin = new MockDatabaseAdminServiceImpl(mockOperationsService);

    InetSocketAddress address = new InetSocketAddress("localhost", 0);
    spannerServer =
        NettyServerBuilder.forAddress(address)
            .addService(mockSpanner)
            .addService(mockDatabaseAdmin)
            .addService(mockOperationsService)
            .build()
            .start();
    // Create the test database on the mock server. This should be replaced by a simple feature in
    // the mock server to just add a database instead of having to simulate the creation of it.
    createDatabase();

    spanner =
        SpannerOptions.newBuilder()
            .setProjectId("spanner-pg-preview-internal")
            .setHost("http://localhost:" + spannerServer.getPort())
            .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
    client =
        spanner.getDatabaseClient(
            DatabaseId.of("spanner-pg-preview-internal", "europe-north1", "knut-test-db"));
  }

  @TearDown(Level.Trial)
  public void closeClient() throws InterruptedException {
    spanner.close();

    if (spannerServer != null) {
      spannerServer.shutdown();
      spannerServer.awaitTermination();
    }
  }

  public DatabaseClient getClient() {
    return client;
  }

  private void createDatabase() throws Exception {
    // TODO: Replace this entire method with a feature in the test framework to just manually add a
    // database instead of having to create it.
    DatabaseAdminSettings.Builder builder =
        DatabaseAdminSettings.newBuilder()
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setTransportChannelProvider(
                InstantiatingGrpcChannelProvider.newBuilder()
                    .setEndpoint(String.format("localhost:%d", spannerServer.getPort()))
                    .setCredentials(NoCredentials.getInstance())
                    .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                    .build());

    RetrySettings longRunningInitialRetrySettings =
        RetrySettings.newBuilder()
            .setInitialRpcTimeout(Duration.ofMillis(600L))
            .setMaxRpcTimeout(Duration.ofMillis(6000L))
            .setInitialRetryDelay(Duration.ofMillis(20L))
            .setMaxRetryDelay(Duration.ofMillis(45L))
            .setRetryDelayMultiplier(1.5)
            .setRpcTimeoutMultiplier(1.5)
            .setTotalTimeout(Duration.ofMinutes(48L))
            .build();
    builder
        .createDatabaseOperationSettings()
        .setInitialCallSettings(
            UnaryCallSettings
                .<CreateDatabaseRequest, OperationSnapshot>newUnaryCallSettingsBuilder()
                .setRetrySettings(longRunningInitialRetrySettings)
                .build());
    builder
        .createDatabaseOperationSettings()
        .setPollingAlgorithm(
            OperationTimedPollAlgorithm.create(
                RetrySettings.newBuilder()
                    .setInitialRpcTimeout(Duration.ofMillis(20L))
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setMaxRetryDelay(Duration.ofMillis(150L))
                    .setMaxRpcTimeout(Duration.ofMillis(150L))
                    .setMaxAttempts(10)
                    .setTotalTimeout(Duration.ofMillis(5000L))
                    .setRetryDelayMultiplier(1.3)
                    .setRpcTimeoutMultiplier(1.3)
                    .build()));

    DatabaseAdminClient client = DatabaseAdminClient.create(builder.build());
    client
        .createDatabaseAsync(
            InstanceName.newBuilder().setProject("p").setInstance("i").build(), "CREATE DATABASE d")
        .get();
    client.close();
  }
}
