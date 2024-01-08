package com.google.cloud.pgadapter.benchmark;

import static org.junit.Assert.assertEquals;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.cloud.pgadapter.benchmark.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.benchmark.config.SpannerConfiguration;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SpannerBenchmarkRunner extends AbstractBenchmarkRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerBenchmarkRunner.class);

  private final SpannerConfiguration spannerConfiguration;

  private final PGAdapterConfiguration pgAdapterConfiguration;

  private DatabaseClient databaseClient;

  SpannerBenchmarkRunner(
      String name,
      Statistics statistics,
      SpannerConfiguration spannerConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      BenchmarkConfiguration benchmarkConfiguration) {
    super(name, statistics, benchmarkConfiguration);
    this.spannerConfiguration = spannerConfiguration;
    this.pgAdapterConfiguration = pgAdapterConfiguration;
  }

  @Override
  public void run() {
    try (Spanner spanner = createSpanner()) {
      databaseClient =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spannerConfiguration.getProject(),
                  spannerConfiguration.getInstance(),
                  spannerConfiguration.getDatabase()));
      Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(2000L));
      super.run();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  List<String> loadIdentifiers() {
    List<String> result = new ArrayList<>(benchmarkConfiguration.getRecordCount());
    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of("select id from benchmark_all_types"))) {
      while (resultSet.next()) {
        result.add(resultSet.getString(0));
      }
    }
    return result;
  }

  @Override
  String getParameterName(int index) {
    return "$" + index;
  }

  @Override
  void runQuery(
      String sql, boolean autoCommit, int iterations, ConcurrentLinkedQueue<Duration> durations) {
    try {
      for (int n = 0; n < iterations; n++) {
        if (!benchmarkConfiguration.getMaxRandomWait().isZero()) {
          long sleepDuration =
              ThreadLocalRandom.current()
                  .nextLong(benchmarkConfiguration.getMaxRandomWait().toMillis());
          Thread.sleep(sleepDuration);
        }
        String id = identifiers.get(ThreadLocalRandom.current().nextInt(identifiers.size()));
        Stopwatch watch = Stopwatch.createStarted();
        Statement statement = Statement.newBuilder(sql).bind("p1").to(id).build();
        if (autoCommit) {
          try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
            consumeResultSet(resultSet);
          }
        } else {
          databaseClient
              .readWriteTransaction()
              .run(
                  transaction -> {
                    try (ResultSet resultSet = transaction.executeQuery(statement)) {
                      consumeResultSet(resultSet);
                    }
                    return 0L;
                  });
        }
        statistics.incOperations();
        durations.add(watch.elapsed());
      }
    } catch (InterruptedException interruptedException) {
      throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
    }
  }

  private void consumeResultSet(ResultSet resultSet) {
    while (resultSet.next()) {
      for (int col = 0; col < resultSet.getColumnCount(); col++) {
        assertEquals(
            resultSet.getValue(col),
            resultSet.getValue(resultSet.getMetadata().getRowType().getFields(col).getName()));
      }
    }
  }

  private Spanner createSpanner() throws IOException {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setProjectId(spannerConfiguration.getProject())
            .setNumChannels(pgAdapterConfiguration.getNumChannels())
            .setUseVirtualThreads(spannerConfiguration.isUseVirtualThreads())
            .setSessionPoolOption(
                SessionPoolOptions.newBuilder()
                    .setMinSessions(
                        Math.max(
                            100,
                            benchmarkConfiguration.getParallelism().stream()
                                .max(Integer::compare)
                                .orElse(100)))
                    .setMaxSessions(
                        Math.max(
                            400,
                            benchmarkConfiguration.getParallelism().stream()
                                .max(Integer::compare)
                                .orElse(400)))
                    .setOptimizeSessionPoolFuture(
                        spannerConfiguration.isOptimizeSessionPoolFuture())
                    .setOptimizeUnbalancedCheck(spannerConfiguration.isOptimizeUnbalancedCheck())
                    .setRandomizePositionTransactionsPerSecondThreshold(
                        spannerConfiguration.getRandomizePositionTransactionsPerSecondThreshold())
                    .build());
    if (!Strings.isNullOrEmpty(pgAdapterConfiguration.getCredentials())) {
      builder.setCredentials(
          GoogleCredentials.fromStream(
              new FileInputStream(pgAdapterConfiguration.getCredentials())));
    }
    return builder.build().getService();
  }
}
