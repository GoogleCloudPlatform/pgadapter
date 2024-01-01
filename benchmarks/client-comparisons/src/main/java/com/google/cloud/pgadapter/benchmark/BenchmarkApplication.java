package com.google.cloud.pgadapter.benchmark;

import com.google.cloud.pgadapter.benchmark.config.BenchmarkConfiguration;
import com.google.cloud.pgadapter.benchmark.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.benchmark.config.SpannerConfiguration;
import com.google.cloud.pgadapter.benchmark.dataloader.DataLoadStatus;
import com.google.cloud.pgadapter.benchmark.dataloader.DataLoader;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
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
    ProxyServer server = pgAdapterConfiguration.isInProcess() ? startPGAdapter() : null;
    String pgAdapterConnectionUrl =
        server == null
            ? pgAdapterConfiguration.getConnectionUrl()
            : String.format(
                "jdbc:postgresql://localhost:%d/%s",
                server.getLocalPort(), spannerConfiguration.getDatabase());
    String spannerConnectionUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?numChannels=%d"
                + (pgAdapterConfiguration.getCredentials() == null
                    ? ""
                    : ";credentials=" + pgAdapterConfiguration.getCredentials()),
            spannerConfiguration.getProject(),
            spannerConfiguration.getInstance(),
            spannerConfiguration.getDatabase(),
            pgAdapterConfiguration.getNumChannels());
    try {
      SchemaService schemaService = new SchemaService(pgAdapterConnectionUrl);
      schemaService.createSchema();

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
          || benchmarkConfiguration.isRunSpannerBenchmark()) {
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
      }
    } finally {
      if (server != null) {
        server.stopServer();
        server.awaitTerminated();
      }
    }
  }

  private long loadData(DataLoadStatus status, String connectionUrl) throws Exception {
    try (DataLoader loader = new DataLoader(status, connectionUrl, benchmarkConfiguration)) {
      return loader.loadData();
    }
  }

  private ProxyServer startPGAdapter() {
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(spannerConfiguration.getProject())
            .setInstance(spannerConfiguration.getInstance())
            .setDatabase(spannerConfiguration.getDatabase())
            .setNumChannels(pgAdapterConfiguration.getNumChannels())
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
