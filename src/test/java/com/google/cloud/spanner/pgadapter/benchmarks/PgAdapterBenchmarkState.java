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
import com.google.cloud.spanner.pgadapter.ProxyServer;
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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Objects;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.threeten.bp.Duration;

@State(Scope.Benchmark)
public class PgAdapterBenchmarkState {
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

  protected volatile MockSpannerServiceImpl mockSpanner;
  protected volatile MockOperationsServiceImpl mockOperationsService;
  protected volatile MockDatabaseAdminServiceImpl mockDatabaseAdmin;
  private volatile Server spannerServer;
  protected volatile ProxyServer pgServer;

  private HikariDataSource dataSource;

  @Setup(Level.Trial)
  public void startMockSpannerAndPgAdapterServers() throws Exception {
    System.setProperty(
        "java.util.logging.config.file",
        ClassLoader.getSystemResource("logging.properties").getPath());
    System.setProperty("useMockServer", "true");

    ImmutableList.Builder<String> argsListBuilder;
    if (Objects.equals("true", System.getProperty("useMockServer"))) {
      mockSpanner = new MockSpannerServiceImpl();
      mockSpanner.setAbortProbability(
          0.0D); // We don't want any unpredictable aborted transactions.
      mockSpanner.putStatementResult(StatementResult.query(SELECT1, SELECT1_RESULTSET));
      mockSpanner.putStatementResult(StatementResult.query(SELECT2, SELECT2_RESULTSET));
      mockSpanner.putStatementResult(StatementResult.update(UPDATE_STATEMENT, UPDATE_COUNT));
      mockSpanner.putStatementResult(StatementResult.update(INSERT_STATEMENT, INSERT_COUNT));
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

      argsListBuilder =
          ImmutableList.<String>builder()
              .add(
                  "-p",
                  "p",
                  "-i",
                  "i",
                  "-d",
                  "d",
                  "-c",
                  "", // empty credentials file, as we are using a plain text connection.
                  "-s",
                  "0", // port 0 to let the OS pick an available port
                  "-e",
                  String.format("localhost:%d", spannerServer.getPort()),
                  "-r",
                  "usePlainText=true;");
    } else {
      argsListBuilder =
          ImmutableList.<String>builder()
              .add(
                  "-p",
                  "spanner-pg-preview-internal",
                  "-i",
                  "europe-north1",
                  "-d",
                  "knut-test-db",
                  "-s",
                  "0");
    }
    String[] args = argsListBuilder.build().toArray(new String[0]);
    pgServer = new ProxyServer(new OptionsMetadata(args));
    pgServer.startServer();

    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(
        String.format(
            "jdbc:postgresql://localhost:%d/?preferQueryMode=simple", pgServer.getLocalPort()));
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    hikariConfig.setMaximumPoolSize(150);
    dataSource = new HikariDataSource(hikariConfig);
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
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

  @TearDown(Level.Trial)
  public void stopMockSpannerAndPgAdapterServers() throws Exception {
    dataSource.close();
    pgServer.stopServer();
    pgServer.awaitTerminated();
    while (true) {
      try {
        SpannerPool.closeSpannerPool();
        break;
      } catch (SpannerException e) {
        if (e.getErrorCode() == ErrorCode.FAILED_PRECONDITION
            && e.getMessage()
                .contains(
                    "There is/are 1 connection(s) still open. Close all connections before calling closeSpanner()")) {
        } else {
          throw e;
        }
      }
    }
    if (spannerServer != null) {
      spannerServer.shutdown();
      spannerServer.awaitTermination();
    }
  }
}
