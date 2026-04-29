// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.benchmark;

import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for PGAdapter using the in-process mock Spanner server. This class is named without
 * the 'Test' suffix to prevent it from running automatically during standard unit test execution.
 *
 * <p>This benchmark is intentionally single-threaded. Its intended use is to measure CPU and memory
 * usage of PGAdapter in a controlled single-threaded environment. It is not intended to measure
 * throughput or scalability.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SingleThreadedMockServerBenchmark extends AbstractMockServerTest {

  /**
   * The single connection that is used for all benchmarks. Note that this makes this benchmark
   * single-threaded by design.
   */
  private Connection connection;

  private static com.google.cloud.spanner.MockSpannerServiceImpl
      createMockSpannerThatReturnsOneQueryPartition() {
    return new com.google.cloud.spanner.MockSpannerServiceImpl() {
      @Override
      public void partitionQuery(
          com.google.spanner.v1.PartitionQueryRequest request,
          io.grpc.stub.StreamObserver<com.google.spanner.v1.PartitionResponse> responseObserver) {
        responseObserver.onNext(
            com.google.spanner.v1.PartitionResponse.newBuilder()
                .addPartitions(
                    com.google.spanner.v1.Partition.newBuilder()
                        .setPartitionToken(com.google.protobuf.ByteString.EMPTY)
                        .build())
                .build());
        responseObserver.onCompleted();
      }
    };
  }

  private static com.google.spanner.v1.ResultSetMetadata createNonArrayTypesResultSetMetadata() {
    return com.google.spanner.v1.ResultSetMetadata.newBuilder()
        .setRowType(
            com.google.spanner.v1.StructType.newBuilder()
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_bigint")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.INT64)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_bool")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.BOOL)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_bytea")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.BYTES)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_float4")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.FLOAT32)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_float8")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.FLOAT64)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_int")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.INT64)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_numeric")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.NUMERIC)
                                .setTypeAnnotation(
                                    com.google.spanner.v1.TypeAnnotationCode.PG_NUMERIC)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_timestamptz")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.TIMESTAMP)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_interval")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.INTERVAL)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_date")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.DATE)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_varchar")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.STRING)
                                .build()))
                .addFields(
                    com.google.spanner.v1.StructType.Field.newBuilder()
                        .setName("col_jsonb")
                        .setType(
                            com.google.spanner.v1.Type.newBuilder()
                                .setCode(com.google.spanner.v1.TypeCode.JSON)
                                .setTypeAnnotation(
                                    com.google.spanner.v1.TypeAnnotationCode.PG_JSONB)
                                .build()))
                .build())
        .build();
  }

  private static String createLargeJson(String id) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"key\": \"").append(id).append("\", \"data\": \"");
    for (int i = 0; i < 1000; i++) {
      sb.append("abcdefghij");
    }
    sb.append("\"}");
    return sb.toString();
  }

  private static com.google.protobuf.ListValue createNonArrayRow(String id) {
    int rowId = Integer.parseInt(id);
    return com.google.protobuf.ListValue.newBuilder()
        .addValues(com.google.protobuf.Value.newBuilder().setStringValue(id).build())
        .addValues(com.google.protobuf.Value.newBuilder().setBoolValue(rowId % 2 == 0).build())
        .addValues(
            com.google.protobuf.Value.newBuilder()
                .setStringValue(
                    java.util.Base64.getEncoder()
                        .encodeToString(
                            ("test" + id).getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .build())
        .addValues(com.google.protobuf.Value.newBuilder().setNumberValue(3.14f + rowId).build())
        .addValues(com.google.protobuf.Value.newBuilder().setNumberValue(3.14d + rowId).build())
        .addValues(
            com.google.protobuf.Value.newBuilder()
                .setStringValue(String.valueOf(100 + rowId))
                .build())
        .addValues(
            com.google.protobuf.Value.newBuilder()
                .setStringValue(String.valueOf(6.626 + rowId))
                .build())
        .addValues(
            com.google.protobuf.Value.newBuilder()
                .setStringValue(String.format("2022-02-16T13:18:02.%09dZ", rowId))
                .build())
        .addValues(
            com.google.protobuf.Value.newBuilder()
                .setStringValue("P" + rowId + "Y2M3DT4H5M6.789S")
                .build())
        .addValues(
            com.google.protobuf.Value.newBuilder()
                .setStringValue(java.time.LocalDate.of(2022, 3, 1).plusDays(rowId % 28).toString())
                .build())
        .addValues(com.google.protobuf.Value.newBuilder().setStringValue("testÄ" + id).build())
        .addValues(
            com.google.protobuf.Value.newBuilder().setStringValue(createLargeJson(id)).build())
        .build();
  }

  private static com.google.spanner.v1.ResultSet createNonArrayTypesResultSet(String id) {
    return com.google.spanner.v1.ResultSet.newBuilder()
        .setMetadata(createNonArrayTypesResultSetMetadata())
        .addRows(createNonArrayRow(id))
        .build();
  }

  private static com.google.spanner.v1.ResultSet createLargeNonArrayTypesResultSet() {
    com.google.spanner.v1.ResultSet.Builder builder =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(createNonArrayTypesResultSetMetadata());
    for (int i = 0; i < 1000; i++) {
      builder.addRows(createNonArrayRow(String.valueOf(i)));
    }
    return builder.build();
  }

  @Setup(Level.Trial)
  public void setup() throws Exception {
    // Start the mock servers with debug mode disabled to avoid filling up memory/logs.
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        null,
        builder ->
            ((com.google.cloud.spanner.pgadapter.metadata.TestOptionsMetadataBuilder) builder)
                .disableDebugMode(),
        io.opentelemetry.api.OpenTelemetry.noop());

    // Create a shared connection for the benchmark.
    String url = String.format("jdbc:postgresql://localhost:%d/d", pgServer.getLocalPort());
    connection = DriverManager.getConnection(url);
    connection.createStatement().execute("set spanner.string_conversion_buffer_size=0");

    // Register partial result for parameterized query benchmark
    mockSpanner.putPartialStatementResult(
        com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult.query(
            com.google.cloud.spanner.Statement.of("SELECT * FROM all_types WHERE col_bigint = $1"),
            createNonArrayTypesResultSet("1")));

    // Register result for large result set benchmark
    mockSpanner.putStatementResult(
        com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult.query(
            com.google.cloud.spanner.Statement.of("SELECT * FROM large_table"),
            createLargeNonArrayTypesResultSet()));
    // Register results for read/write transaction benchmark
    mockSpanner.putPartialStatementResult(
        com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult.query(
            com.google.cloud.spanner.Statement.of("SELECT * FROM table1 WHERE id = $1"),
            createNonArrayTypesResultSet("1")));
    mockSpanner.putPartialStatementResult(
        com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult.update(
            com.google.cloud.spanner.Statement.of(
                "UPDATE table1 SET col_varchar = $1 WHERE id = $2"),
            1L));
    mockSpanner.putPartialStatementResult(
        com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult.query(
            com.google.cloud.spanner.Statement.of("SELECT * FROM table2 WHERE id = $1"),
            createNonArrayTypesResultSet("2")));
    mockSpanner.putPartialStatementResult(
        com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult.update(
            com.google.cloud.spanner.Statement.of(
                "UPDATE table2 SET col_varchar = $1 WHERE id = $2"),
            1L));
  }

  @TearDown(Level.Trial)
  public void teardown() throws Exception {
    if (connection != null) {
      connection.close();
    }
    stopMockSpannerAndPgAdapterServers();
  }

  @State(Scope.Benchmark)
  public static class ParameterState {
    private long value = 0;

    public long nextValue() {
      return ++value;
    }
  }

  @Benchmark
  public void testSelectOneRowWithParam(ParameterState state, Blackhole blackhole)
      throws SQLException {
    try (java.sql.PreparedStatement statement =
        connection.prepareStatement("SELECT * FROM all_types WHERE col_bigint = ?")) {
      statement.setLong(1, state.nextValue());
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          // Consume result set
          blackhole.consume(resultSet.getLong(1));
        }
      }
    }
  }

  @Benchmark
  public void testSelect1(Blackhole blackhole) throws SQLException {
    try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
      while (resultSet.next()) {
        // Consume result set
        blackhole.consume(resultSet.getLong(1));
      }
    }
  }

  @Benchmark
  public void testSelectFiveRows(Blackhole blackhole) throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery("SELECT * FROM TableWithFiveRows")) {
      while (resultSet.next()) {
        // Consume result set
        blackhole.consume(resultSet.getString(1));
      }
    }
  }

  @Benchmark
  public void testSelectLargeResultSet(Blackhole blackhole) throws SQLException {
    try (ResultSet resultSet =
        connection.createStatement().executeQuery("SELECT * FROM large_table")) {
      while (resultSet.next()) {
        // Consume result set
        blackhole.consume(resultSet.getString(1));
      }
    }
  }

  @Benchmark
  public void testReadWriteTransaction(Blackhole blackhole) throws SQLException {
    connection.setAutoCommit(false);
    try {
      try (java.sql.PreparedStatement statement =
          connection.prepareStatement("SELECT * FROM table1 WHERE id = ?")) {
        statement.setLong(1, 1L);
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            blackhole.consume(resultSet.getLong(1));
          }
        }
      }
      try (java.sql.PreparedStatement statement =
          connection.prepareStatement("UPDATE table1 SET col_varchar = ? WHERE id = ?")) {
        statement.setString(1, "value1");
        statement.setLong(2, 1L);
        statement.executeUpdate();
      }
      try (java.sql.PreparedStatement statement =
          connection.prepareStatement("SELECT * FROM table2 WHERE id = ?")) {
        statement.setLong(1, 2L);
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            blackhole.consume(resultSet.getLong(1));
          }
        }
      }
      try (java.sql.PreparedStatement statement =
          connection.prepareStatement("UPDATE table2 SET col_varchar = ? WHERE id = ?")) {
        statement.setString(1, "value2");
        statement.setLong(2, 2L);
        statement.executeUpdate();
      }
      connection.commit();
    } catch (SQLException e) {
      connection.rollback();
      throw e;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  /**
   * This method allows running the benchmark as a JUnit test from an IDE or command line. It is NOT
   * picked up by default by Surefire because the class name doesn't end in 'Test'.
   */
  @Test
  public void runBenchmarks() throws Exception {
    // Touch file to force recompilation and JMH generation
    Options opt =
        new OptionsBuilder()
            .include(SingleThreadedMockServerBenchmark.class.getName())
            .addProfiler(GCProfiler.class)
            .forks(1)
            .warmupIterations(2)
            .measurementIterations(5)
            .build();

    new Runner(opt).run();
  }
}
