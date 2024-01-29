package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.pgadapter.tpcc.dataloader.DataLoadStatus;
import com.google.cloud.pgadapter.tpcc.dataloader.DataLoader;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
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
      exception.printStackTrace();
    }
  }

  private final SpannerConfiguration spannerConfiguration;

  private final PGAdapterConfiguration pgAdapterConfiguration;

  private final TpccConfiguration tpccConfiguration;

  public BenchmarkApplication(
      SpannerConfiguration spannerConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      TpccConfiguration tpccConfiguration) {
    this.spannerConfiguration = spannerConfiguration;
    this.pgAdapterConfiguration = pgAdapterConfiguration;
    this.tpccConfiguration = tpccConfiguration;
  }

  @Override
  public void run(String... args) throws Exception {
    ProxyServer server = pgAdapterConfiguration.isInProcess() ? startPGAdapter() : null;
    String connectionUrl =
        server == null
            ? pgAdapterConfiguration.getConnectionUrl()
            : String.format(
                "jdbc:postgresql://localhost:%d/tpcc?preferQueryMode=simple",
                server.getLocalPort());
    try {
      if (tpccConfiguration.isLoadData()) {
        System.out.println("Checking schema");
        SchemaService schemaService = new SchemaService(connectionUrl);
        schemaService.createSchema();
        System.out.println("Checked schema, starting benchmark");

        LOG.info("Starting data load");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DataLoadStatus status = new DataLoadStatus(tpccConfiguration);
        Future<Long> loadDataFuture = executor.submit(() -> loadData(status, connectionUrl));
        executor.shutdown();
        Stopwatch watch = Stopwatch.createStarted();
        while (!loadDataFuture.isDone()) {
          //noinspection BusyWait
          Thread.sleep(1_000L);
          status.print(watch.elapsed());
        }
        System.out.printf("Finished loading %d rows\n", loadDataFuture.get());
      }

      if (tpccConfiguration.isRunBenchmark()) {
        LOG.info("Starting benchmark");
        Statistics statistics = new Statistics(tpccConfiguration);
        ExecutorService executor =
            Executors.newFixedThreadPool(tpccConfiguration.getBenchmarkThreads());
        for (int i = 0; i < tpccConfiguration.getBenchmarkThreads(); i++) {
          executor.submit(new BenchmarkRunner(statistics, connectionUrl, tpccConfiguration));
        }

        Stopwatch watch = Stopwatch.createStarted();
        while (watch.elapsed().compareTo(tpccConfiguration.getBenchmarkDuration()) <= 0) {
          //noinspection BusyWait
          Thread.sleep(1_000L);
          statistics.print(watch.elapsed());
        }
        executor.shutdownNow();
        if (!executor.awaitTermination(60L, TimeUnit.SECONDS)) {
          throw new TimeoutException("Timed out while waiting for benchmark runners to shut down");
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
    try (DataLoader loader = new DataLoader(status, connectionUrl, tpccConfiguration)) {
      return loader.loadData();
    }
  }

  private ProxyServer startPGAdapter() {
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(spannerConfiguration.getProject())
            .setInstance(spannerConfiguration.getInstance())
            .setDatabase(spannerConfiguration.getDatabase())
            // .setNumChannels(pgAdapterConfiguration.getNumChannels())
            .setSessionPoolOptions(
                SessionPoolOptions.newBuilder()
                    .setTrackStackTraceOfSessionCheckout(false)
                    // .setMinSessions(
                    //     benchmarkConfiguration.getParallelism().stream()
                    //         .max(Integer::compare)
                    //         .orElse(100))
                    .setMinSessions(100)
                    // .setMaxSessions(
                    //     benchmarkConfiguration.getParallelism().stream()
                    //         .max(Integer::compare)
                    //         .orElse(400))
                    .setMaxSessions(800)
                    // .setOptimizeSessionPoolFuture(
                    //     spannerConfiguration.isOptimizeSessionPoolFuture())
                    // .setOptimizeUnbalancedCheck(spannerConfiguration.isOptimizeUnbalancedCheck())
                    // .setRandomizePositionTransactionsPerSecondThreshold(
                    //
                    // spannerConfiguration.getRandomizePositionTransactionsPerSecondThreshold())
                    .build())
            // .setDisableVirtualThreads(!spannerConfiguration.isUseVirtualThreads())
            .disableUnixDomainSockets();
    if (pgAdapterConfiguration.isEnableOpenTelemetry()) {
      builder
          .setEnableOpenTelemetry()
          .setOpenTelemetryTraceRatio(pgAdapterConfiguration.getOpenTelemetrySampleRate());
    }
    if (pgAdapterConfiguration.isDisableInternalRetries()) {
      builder.setDisableInternalRetries();
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
