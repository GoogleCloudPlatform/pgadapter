package com.google.cloud.pgadapter.benchmark;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.cloud.pgadapter.benchmark.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.benchmark.config.SpannerConfiguration;
import com.google.cloud.pgadapter.benchmark.dataloader.DataLoadStatus;
import com.google.cloud.pgadapter.benchmark.dataloader.DataLoader;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BenchmarkApplication implements CommandLineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkApplication.class);

  private static final String NO_NAME = "(no name)";

  public static void main(String[] args) {
    try {
      SpringApplication.run(BenchmarkApplication.class, args);
    } catch (Throwable exception) {
      LOG.error("Failed to start application", exception);
      //noinspection CallToPrintStackTrace
      exception.printStackTrace();
    }
  }

  private final SpannerConfiguration spannerConfiguration;

  private final PGAdapterConfiguration pgAdapterConfiguration;

  private final BenchmarkConfiguration benchmarkConfiguration;

  public BenchmarkApplication(
      SpannerConfiguration spannerConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      BenchmarkConfiguration benchmarkConfiguration) {
    this.spannerConfiguration = spannerConfiguration;
    this.pgAdapterConfiguration = pgAdapterConfiguration;
    this.benchmarkConfiguration = benchmarkConfiguration;
  }

  @Override
  public void run(String... args) throws Exception {
    String name = args == null || args.length == 0 ? NO_NAME : args[0];
    Timestamp executedAt = Timestamp.from(Instant.now());
    System.out.println("Running benchmark with name " + name + ", executed at " + executedAt);
    ProxyServer server = pgAdapterConfiguration.isInProcess() ? startPGAdapter() : null;
    String pgAdapterConnectionUrl =
        server == null
            ? pgAdapterConfiguration.getConnectionUrl()
            : String.format(
                "jdbc:postgresql://localhost:%d/%s",
                server.getLocalPort(), spannerConfiguration.getDatabase());
    String spannerConnectionUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?numChannels=%d;minSessions=%d;maxSessions=%d;useVirtualThreads=%s;useStickySessions=%s;optimizeSessionPool=%s"
                + (pgAdapterConfiguration.getCredentials() == null
                    ? ""
                    : ";credentials=" + pgAdapterConfiguration.getCredentials()),
            spannerConfiguration.getProject(),
            spannerConfiguration.getInstance(),
            spannerConfiguration.getDatabase(),
            pgAdapterConfiguration.getNumChannels(),
            Math.max(
                100,
                benchmarkConfiguration.getParallelism().stream().max(Integer::compare).orElse(100)),
            Math.max(
                400,
                benchmarkConfiguration.getParallelism().stream().max(Integer::compare).orElse(400)),
            spannerConfiguration.isUseVirtualThreads(),
            spannerConfiguration.isUseStickySessionClient(),
            spannerConfiguration.isOptimizeUnbalancedCheck()
                && spannerConfiguration.isOptimizeSessionPoolFuture()
                && spannerConfiguration.getRandomizePositionTransactionsPerSecondThreshold() > 0L);
    try {
      System.out.println("Checking schema");
      SchemaService schemaService = new SchemaService(pgAdapterConnectionUrl);
      schemaService.createSchema();
      System.out.println("Checked schema, starting benchmark");

      if (benchmarkConfiguration.isLoadData()) {
        LOG.info("Starting data load");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DataLoadStatus status = new DataLoadStatus(benchmarkConfiguration);
        Future<Long> loadDataFuture =
            executor.submit(() -> loadData(status, pgAdapterConnectionUrl));
        executor.shutdown();
        Stopwatch watch = Stopwatch.createStarted();
        while (!loadDataFuture.isDone()) {
          //noinspection BusyWait
          Thread.sleep(1_000L);
          status.print(watch.elapsed());
        }
        System.out.printf("Finished loading %d rows\n", loadDataFuture.get());
      }

      if (benchmarkConfiguration.isRunPgadapterBenchmark()
          || benchmarkConfiguration.isRunJdbcBenchmark()
          || benchmarkConfiguration.isRunSpannerBenchmark()
          || benchmarkConfiguration.isRunGapicBenchmark()) {
        List<AbstractBenchmarkRunner> runners = new ArrayList<>();
        LOG.info("Starting benchmarks");
        Statistics statistics = new Statistics(benchmarkConfiguration);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        if (benchmarkConfiguration.isRunPgadapterBenchmark()) {
          JdbcBenchmarkRunner runner =
              new JdbcBenchmarkRunner(
                  "PGAdapter Benchmarks",
                  statistics,
                  pgAdapterConnectionUrl,
                  benchmarkConfiguration);
          executor.submit(runner);
          runners.add(runner);
        }
        if (benchmarkConfiguration.isRunJdbcBenchmark()) {
          JdbcBenchmarkRunner runner =
              new JdbcBenchmarkRunner(
                  "Spanner JDBC Driver Benchmarks",
                  statistics,
                  spannerConnectionUrl,
                  benchmarkConfiguration);
          executor.submit(runner);
          runners.add(runner);
        }
        if (benchmarkConfiguration.isRunSpannerBenchmark()) {
          SpannerBenchmarkRunner runner =
              new SpannerBenchmarkRunner(
                  "Spanner Java Client Library Benchmarks",
                  statistics,
                  spannerConfiguration,
                  pgAdapterConfiguration,
                  benchmarkConfiguration);
          executor.submit(runner);
          runners.add(runner);
        }
        if (benchmarkConfiguration.isRunGapicBenchmark()) {
          GapicBenchmarkRunner runner =
              new GapicBenchmarkRunner(
                  "Generated Client Benchmarks",
                  statistics,
                  benchmarkConfiguration,
                  pgAdapterConfiguration,
                  spannerConfiguration);
          executor.submit(runner);
          runners.add(runner);
        }

        executor.shutdown();

        Stopwatch watch = Stopwatch.createStarted();
        while (!executor.isTerminated()
            && watch.elapsed().compareTo(benchmarkConfiguration.getBenchmarkDuration()) <= 0) {
          //noinspection BusyWait
          Thread.sleep(1_000L);
          statistics.print(watch.elapsed());
        }
        executor.shutdownNow();
        if (!executor.awaitTermination(60L, TimeUnit.SECONDS)) {
          throw new TimeoutException("Timed out while waiting for benchmark runner to shut down");
        }

        for (AbstractBenchmarkRunner runner : runners) {
          System.out.println(runner.getName());
          for (BenchmarkResult result : runner.getResults()) {
            System.out.println(result);
          }
        }

        if (!Objects.equals(NO_NAME, name)) {
          try (Connection connection = DriverManager.getConnection(spannerConnectionUrl);
              PreparedStatement statement =
                  connection.prepareStatement(
                      "insert into benchmark_results (name, executed_at, parallelism, avg, p50, p90, p95, p99) values (?, ?, ?, ?, ?, ?, ?, ?)")) {
            for (AbstractBenchmarkRunner runner : runners) {
              for (BenchmarkResult result : runner.getResults()) {
                int param = 0;
                statement.setString(++param, name + " - " + runner.getName() + " - " + result.name);
                statement.setTimestamp(++param, executedAt);
                statement.setInt(++param, result.parallelism);
                statement.setBigDecimal(++param, toBigDecimalSeconds(result.avg));
                statement.setBigDecimal(++param, toBigDecimalSeconds(result.p50));
                statement.setBigDecimal(++param, toBigDecimalSeconds(result.p90));
                statement.setBigDecimal(++param, toBigDecimalSeconds(result.p95));
                statement.setBigDecimal(++param, toBigDecimalSeconds(result.p99));
                statement.addBatch();
              }
              statement.executeBatch();
            }
          }
        }
      }
    } finally {
      if (server != null) {
        server.stopServer();
        server.awaitTerminated();
      }
    }
  }

  static BigDecimal toBigDecimalSeconds(Duration duration) {
    return BigDecimal.valueOf(duration.getSeconds()).add(BigDecimal.valueOf(duration.getNano(), 9));
  }

  private long loadData(DataLoadStatus status, String connectionUrl) throws Exception {
    try (DataLoader loader = new DataLoader(status, connectionUrl, benchmarkConfiguration)) {
      return loader.loadData();
    }
  }

  private ProxyServer startPGAdapter() {
    int parallelism = Runtime.getRuntime().availableProcessors() * 3;
    int maxPoolSize = Math.max(256, parallelism * 3);
    if (!System.getProperties().containsKey("jdk.virtualThreadScheduler.parallelism")) {
      System.setProperty("jdk.virtualThreadScheduler.parallelism", String.valueOf(parallelism));
    }
    if (!System.getProperties().containsKey("jdk.virtualThreadScheduler.maxPoolSize")) {
      System.setProperty("jdk.virtualThreadScheduler.maxPoolSize", String.valueOf(maxPoolSize));
    }

    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(spannerConfiguration.getProject())
            .setInstance(spannerConfiguration.getInstance())
            .setDatabase(spannerConfiguration.getDatabase())
            .setNumChannels(pgAdapterConfiguration.getNumChannels())
            .setSessionPoolOptions(
                SessionPoolOptions.newBuilder()
                    .setTrackStackTraceOfSessionCheckout(false)
                    .setMinSessions(
                        benchmarkConfiguration.getParallelism().stream()
                            .max(Integer::compare)
                            .orElse(100))
                    .setMaxSessions(
                        benchmarkConfiguration.getParallelism().stream()
                            .max(Integer::compare)
                            .orElse(400))
                    .setOptimizeSessionPoolFuture(
                        spannerConfiguration.isOptimizeSessionPoolFuture())
                    .setOptimizeUnbalancedCheck(spannerConfiguration.isOptimizeUnbalancedCheck())
                    .setRandomizePositionTransactionsPerSecondThreshold(
                        spannerConfiguration.getRandomizePositionTransactionsPerSecondThreshold())
                    .build())
            .setDisableVirtualThreads(!spannerConfiguration.isUseVirtualThreads())
            .disableUnixDomainSockets();
    if (pgAdapterConfiguration.isEnableOpenTelemetry()) {
      builder
          .setEnableOpenTelemetry()
          .setOpenTelemetryTraceRatio(pgAdapterConfiguration.getOpenTelemetrySampleRate());
    }
    if (!Strings.isNullOrEmpty(pgAdapterConfiguration.getCredentials())) {
      builder.setCredentialsFile(pgAdapterConfiguration.getCredentials());
    }
    ProxyServer server = new ProxyServer(builder.build());
    server.startServer();
    server.awaitRunning();

    return server;
  }
}
