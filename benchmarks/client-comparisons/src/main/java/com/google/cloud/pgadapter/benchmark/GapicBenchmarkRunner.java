package com.google.cloud.pgadapter.benchmark;

import static com.google.cloud.spanner.ThreadFactoryUtil.createVirtualOrDaemonThreadFactory;
import static org.junit.Assert.assertEquals;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.cloud.pgadapter.benchmark.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.benchmark.config.SpannerConfiguration;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.v1.SpannerClient;
import com.google.cloud.spanner.v1.SpannerSettings;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.TransactionOptions.ReadWrite;
import com.google.spanner.v1.TransactionSelector;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

class GapicBenchmarkRunner extends AbstractBenchmarkRunner {

  private final SpannerConfiguration spannerConfiguration;

  private final PGAdapterConfiguration pgAdapterConfiguration;

  private SpannerClient client;

  GapicBenchmarkRunner(
      String name,
      Statistics statistics,
      BenchmarkConfiguration benchmarkConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      SpannerConfiguration spannerConfiguration) {
    super(name, statistics, benchmarkConfiguration);
    this.spannerConfiguration = spannerConfiguration;
    this.pgAdapterConfiguration = pgAdapterConfiguration;
  }

  @Override
  public void run() {
    try (SpannerClient client = createClient()) {
      this.client = client;
      super.run();
      client.shutdown();
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  List<String> loadIdentifiers() {
    try {
      List<String> result = new ArrayList<>(benchmarkConfiguration.getRecordCount());
      Session session =
          client.createSession(
              CreateSessionRequest.newBuilder()
                  .setDatabase(spannerConfiguration.getDatabaseId().getName())
                  .build());
      Iterator<PartialResultSet> iterator =
          client
              .executeStreamingSqlCallable()
              .call(
                  ExecuteSqlRequest.newBuilder()
                      .setSql("select id from benchmark_all_types")
                      .setSession(session.getName())
                      .build())
              .iterator();
      PartialResultSet previous = null;
      while (iterator.hasNext()) {
        PartialResultSet resultSet = iterator.next();
        int firstIndex = (previous != null && previous.getChunkedValue()) ? 1 : 0;
        int lastIndex =
            resultSet.getChunkedValue()
                ? (resultSet.getValuesCount() - 1)
                : resultSet.getValuesCount();
        if (firstIndex == 1) {
          result.add(
              previous.getValues(previous.getValuesCount() - 1).getStringValue()
                  + resultSet.getValues(0).getStringValue());
        }
        for (int row = firstIndex; row < lastIndex; row++) {
          result.add(resultSet.getValues(row).getStringValue());
        }
        previous = resultSet;
      }
      client.deleteSession(session.getName());
      return result;
    } catch (Exception exception) {
      exception.printStackTrace();
      throw new RuntimeException(exception);
    }
  }

  @Override
  String getParameterName(int index) {
    return "$" + index;
  }

  @Override
  void runQuery(
      String sql,
      boolean autoCommit,
      int iterations,
      int numRows,
      ConcurrentLinkedQueue<Duration> durations) {
    try {
      Session session =
          client.createSession(
              CreateSessionRequest.newBuilder()
                  .setDatabase(spannerConfiguration.getDatabaseId().getName())
                  .build());
      for (int n = 0; n < iterations; n++) {
        if (!benchmarkConfiguration.getMaxRandomWait().isZero()) {
          long sleepDuration =
              ThreadLocalRandom.current()
                  .nextLong(benchmarkConfiguration.getMaxRandomWait().toMillis());
          Thread.sleep(sleepDuration);
        }
        String id = identifiers.get(ThreadLocalRandom.current().nextInt(identifiers.size()));
        Value paramValue;
        if (numRows == 1) {
          paramValue = Value.newBuilder().setStringValue(id).build();
        } else {
          paramValue =
              Value.newBuilder()
                  .setListValue(
                      ListValue.newBuilder()
                          .addAllValues(
                              Arrays.stream(getRandomIds(numRows))
                                  .map(
                                      element ->
                                          Value.newBuilder()
                                              .setStringValue(element.toString())
                                              .build())
                                  .collect(Collectors.toList()))
                          .build())
                  .build();
        }
        Stopwatch watch = Stopwatch.createStarted();
        ExecuteSqlRequest.Builder builder =
            ExecuteSqlRequest.newBuilder()
                .setSession(session.getName())
                .setSql(sql)
                .setSeqno(1L)
                .setParams(Struct.newBuilder().putFields("p1", paramValue).build());
        if (!autoCommit) {
          builder.setTransaction(
              TransactionSelector.newBuilder()
                  .setBegin(
                      TransactionOptions.newBuilder()
                          .setReadWrite(ReadWrite.newBuilder().build())
                          .build())
                  .build());
        }
        ResultSetMetadata metadata;
        if (spannerConfiguration.isUseStreamingSql()) {
          ServerStream<PartialResultSet> stream =
              client.executeStreamingSqlCallable().call(builder.build());
          metadata = consumeStream(stream, numRows);
        } else {
          ResultSet resultSet = client.executeSql(builder.build());
          consumeResultSet(resultSet, numRows);
          metadata = resultSet.getMetadata();
        }
        if (!autoCommit) {
          client.commit(
              CommitRequest.newBuilder()
                  .setSession(session.getName())
                  .setTransactionId(metadata.getTransaction().getId())
                  .build());
        }
        statistics.incOperations();
        durations.add(watch.elapsed());
      }
      client.deleteSession(session.getName());
    } catch (InterruptedException interruptedException) {
      throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private void consumeResultSet(ResultSet resultSet, int expectedNumRows) {
    assertEquals(expectedNumRows, resultSet.getRowsList().size());
    for (ListValue row : resultSet.getRowsList()) {
      int col = 0;
      for (Value value : row.getValuesList()) {
        assertEquals(value, row.getValues(col));
        col++;
      }
    }
  }

  private ResultSetMetadata consumeStream(
      ServerStream<PartialResultSet> stream, int expectedNumRows) {
    ResultSetMetadata metadata = ResultSetMetadata.getDefaultInstance();
    int expectedNumCells = -1;
    int numCells = 0;
    for (PartialResultSet partialResultSet : stream) {
      if (partialResultSet.hasMetadata()) {
        metadata = partialResultSet.getMetadata();
        expectedNumCells = metadata.getRowType().getFieldsCount() * expectedNumRows;
      }
      int col = 0;
      for (Value value : partialResultSet.getValuesList()) {
        assertEquals(value, partialResultSet.getValues(col));
        col++;
        numCells++;
      }
    }
    assertEquals(expectedNumCells, numCells);
    return metadata;
  }

  SpannerClient createClient() throws Exception {
    InstantiatingGrpcChannelProvider channelProvider =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint(SpannerSettings.getDefaultEndpoint())
            .setExecutor(
                Executors.newCachedThreadPool(
                    createVirtualOrDaemonThreadFactory(
                        "grpc-virtual-executor", spannerConfiguration.isUseVirtualThreads())))
            .setChannelPoolSettings(
                ChannelPoolSettings.builder()
                    .setInitialChannelCount(pgAdapterConfiguration.getNumChannels())
                    .setMaxChannelCount(pgAdapterConfiguration.getNumChannels())
                    .setPreemptiveRefreshEnabled(true)
                    .build())
            .build();
    return SpannerClient.create(
        SpannerSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(
                    GoogleCredentials.fromStream(
                        new FileInputStream(pgAdapterConfiguration.getCredentials()))))
            .setTransportChannelProvider(channelProvider)
            .build());
  }
}
