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

package com.google.cloud.spanner.pgadapter;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockDatabaseAdminServiceImpl;
import com.google.cloud.spanner.MockOperationsServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import com.google.spanner.admin.database.v1.InstanceName;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.threeten.bp.Duration;

/**
 * Abstract base class for tests that verify that PgAdapter is receiving wire protocol requests
 * correctly, translates these correctly to Spanner RPC invocations, and correctly translates the
 * RPC invocations back to wire protocol responses. The test starts two in-process servers for this
 * purpose: 1. An in-process {@link MockSpannerServiceImpl}. The mock server implements the entire
 * gRPC API of Cloud Spanner, but does not contain an actual query engine or any other
 * implementation. Instead, all query and DML statements that the client will be executing must
 * first be registered as mock results on the server. This makes the mock server dialect agnostic
 * and usable for both normal Spanner requests and Spangres requests. Note that this also means that
 * the server does NOT verify that the SQL statement is correct and valid for the specific dialect.
 * It only verifies that the statement corresponds with one of the previously registered statements
 * on the server. 2. An in-process PgAdapter {@link ProxyServer} that connects to the
 * above-mentioned mock Spanner server. The in-process PgAdapter server listens on a random local
 * port, and tests can use the client of their choosing to connect to the {@link ProxyServer}. The
 * requests are translated by the proxy into RPC invocations on the mock Spanner server, and the
 * responses from the mock Spanner server are translated into wire protocol responses to the client.
 * The tests can then inspect the requests that the mock Spanner server received to verify that the
 * server received the requests that the test expected.
 */
abstract class AbstractMockServerTest {
  private static final Logger logger = Logger.getLogger(AbstractMockServerTest.class.getName());

  protected static final Statement SELECT1 = Statement.of("SELECT 1");
  protected static final Statement SELECT2 = Statement.of("SELECT 2");
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
  private static final com.google.spanner.v1.ResultSet SELECT2_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("2").build())
                  .build())
          .setMetadata(SELECT1_METADATA)
          .build();
  protected static final Statement UPDATE_STATEMENT =
      Statement.of("UPDATE FOO SET BAR=1 WHERE BAZ=2");
  protected static final int UPDATE_COUNT = 2;
  protected static final Statement INSERT_STATEMENT = Statement.of("INSERT INTO FOO VALUES (1)");
  protected static final int INSERT_COUNT = 1;

  protected static final ResultSetMetadata ALL_TYPES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("col_bigint")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_bool")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_bytea")
                          .setType(Type.newBuilder().setCode(TypeCode.BYTES).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_float8")
                          .setType(Type.newBuilder().setCode(TypeCode.FLOAT64).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_numeric")
                          .setType(
                              Type.newBuilder()
                                  .setCode(TypeCode.NUMERIC)
                                  .setTypeAnnotation(TypeAnnotationCode.PG_NUMERIC)
                                  .build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_timestamptz")
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build()))
                  .addFields(
                      Field.newBuilder()
                          .setName("col_varchar")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build()))
                  .build())
          .build();
  protected static final com.google.spanner.v1.ResultSet ALL_TYPES_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .setMetadata(ALL_TYPES_METADATA)
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("1").build())
                  .addValues(Value.newBuilder().setBoolValue(true).build())
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              Base64.getEncoder()
                                  .encodeToString("test".getBytes(StandardCharsets.UTF_8)))
                          .build())
                  .addValues(Value.newBuilder().setNumberValue(3.14d).build())
                  .addValues(Value.newBuilder().setStringValue("6.626").build())
                  .addValues(
                      Value.newBuilder().setStringValue("2022-02-16T13:18:02.123456789Z").build())
                  .addValues(Value.newBuilder().setStringValue("test").build())
                  .build())
          .build();
  protected static final com.google.spanner.v1.ResultSet ALL_TYPES_NULLS_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .setMetadata(ALL_TYPES_METADATA)
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                  .build())
          .build();

  protected static MockSpannerServiceImpl mockSpanner;
  protected static MockOperationsServiceImpl mockOperationsService;
  protected static MockDatabaseAdminServiceImpl mockDatabaseAdmin;
  private static Server spannerServer;
  protected static ProxyServer pgServer;

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0D); // We don't want any unpredictable aborted transactions.
    mockSpanner.putStatementResult(StatementResult.query(SELECT1, SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT2, SELECT2_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.update(UPDATE_STATEMENT, UPDATE_COUNT));
    mockSpanner.putStatementResult(StatementResult.update(INSERT_STATEMENT, INSERT_COUNT));
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.detectDialectResult(Dialect.POSTGRESQL));

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

    ImmutableList.Builder<String> argsListBuilder =
        ImmutableList.<String>builder()
            .add(
                "-p",
                "p",
                "-i",
                "i",
                "-d",
                "d",
                "-jdbc",
                "-c",
                "", // empty credentials file, as we are using a plain text connection.
                "-s",
                "0", // port 0 to let the OS pick an available port
                "-e",
                String.format("localhost:%d", spannerServer.getPort()),
                "-r",
                "usePlainText=true;");
    String[] args = argsListBuilder.build().toArray(new String[0]);
    pgServer = new ProxyServer(new OptionsMetadata(args));
    pgServer.startServer();
  }

  private static void createDatabase() throws Exception {
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

  @AfterClass
  public static void stopMockSpannerAndPgAdapterServers() throws Exception {
    pgServer.stopServer();
    while (pgServer.isAlive()) {
      // TODO: Remove once the pgServer.stopServer() is blocking until it has actually stopped (or
      // there is some other way to block until it has stopped).
      Thread.sleep(1L);
    }
    try {
      SpannerPool.closeSpannerPool();
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.FAILED_PRECONDITION
          && e.getMessage()
              .contains(
                  "There is/are 1 connection(s) still open. Close all connections before calling closeSpanner()")) {
        // Ignore this exception for now. It is caused by the fact that the PgAdapter proxy server
        // is not gracefully shutting down all connections when the proxy is stopped, and it also
        // does not wait until any connections that have been requested to close, actually have
        // closed.
        logger.warning(String.format("Ignoring %s as this is expected", e.getMessage()));
      } else {
        throw e;
      }
    }
    spannerServer.shutdown();
    spannerServer.awaitTermination();
  }

  /**
   * Closes all open Spanner instances in the pool. Use this to force the recreation of a Spanner
   * instance for a test case. This method will ignore any errors and retry if closing fails.
   */
  protected void closeSpannerPool() {
    SpannerException exception = null;
    for (int attempt = 0; attempt < 1000; attempt++) {
      try {
        SpannerPool.closeSpannerPool();
        return;
      } catch (SpannerException e) {
        exception = e;
      }
    }
    throw exception;
  }

  @Before
  public void clearRequests() {
    mockSpanner.clearRequests();
  }
}
