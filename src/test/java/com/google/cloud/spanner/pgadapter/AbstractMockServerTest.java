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
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.MockDatabaseAdminServiceImpl;
import com.google.cloud.spanner.MockOperationsServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

abstract class AbstractMockServerTest {
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

    // TODO: Replace this with a feature in the test framework to just manually add a database.
    DatabaseAdminClient client =
        DatabaseAdminClient.create(
            DatabaseAdminSettings.newBuilder()
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(
                    InstantiatingGrpcChannelProvider.newBuilder()
                        .setEndpoint(String.format("localhost:%d", spannerServer.getPort()))
                        .setCredentials(NoCredentials.getInstance())
                        .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                        .build())
                .build());
    client.createDatabaseAsync(
        InstanceName.newBuilder().setProject("p").setInstance("i").build(), "CREATE DATABASE d");
    client.close();

    ImmutableList.Builder<String> argsListBuilder =
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
    String[] args = argsListBuilder.build().toArray(new String[0]);
    pgServer = new ProxyServer(new OptionsMetadata(args));
    pgServer.startServer();
  }

  @AfterClass
  public static void stopMockSpannerAndPgAdapterServers() throws Exception {
    pgServer.stopServer();
    SpannerPool.closeSpannerPool();
    spannerServer.shutdown();
    spannerServer.awaitTermination();
  }

  @Before
  public void clearRequests() {
    mockSpanner.clearRequests();
  }
}
