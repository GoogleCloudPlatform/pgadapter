// Copyright 2024 Google LLC
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
package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.pgadapter.tpcc.config.HibernateConfiguration;
import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.pgadapter.tpcc.dataloader.DataLoadStatus;
import com.google.cloud.pgadapter.tpcc.dataloader.DataLoader;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
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

  private final HibernateConfiguration hibernateConfiguration;

  public BenchmarkApplication(
      SpannerConfiguration spannerConfiguration,
      PGAdapterConfiguration pgAdapterConfiguration,
      TpccConfiguration tpccConfiguration,
      HibernateConfiguration hibernateConfiguration) {
    this.spannerConfiguration = spannerConfiguration;
    this.pgAdapterConfiguration = pgAdapterConfiguration;
    this.tpccConfiguration = tpccConfiguration;
    this.hibernateConfiguration = hibernateConfiguration;
  }

  @Override
  public void run(String... args) throws Exception {
    ProxyServer server = pgAdapterConfiguration.isInProcess() ? startPGAdapter() : null;
    String pgadapterConnectionUrl =
        server == null
            ? pgAdapterConfiguration.getConnectionUrl()
            : String.format("jdbc:postgresql://localhost:%d/tpcc", server.getLocalPort());
    String spannerConnectionUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?numChannels=%d;minSessions=%d;maxSessions=%d"
                + (pgAdapterConfiguration.getCredentials() == null
                    ? ""
                    : ";credentials=" + pgAdapterConfiguration.getCredentials()),
            spannerConfiguration.getProject(),
            spannerConfiguration.getInstance(),
            spannerConfiguration.getDatabase(),
            pgAdapterConfiguration.getNumChannels(),
            Math.max(
                pgAdapterConfiguration.getMinSessions(), tpccConfiguration.getBenchmarkThreads()),
            Math.max(
                pgAdapterConfiguration.getMaxSessions(), tpccConfiguration.getBenchmarkThreads()));
    StandardServiceRegistry registry = null;
    SessionFactory sessionFactory = null;
    try {
      if (tpccConfiguration.isLoadData()) {
        boolean isClientLibGSQLRunner =
            tpccConfiguration.getBenchmarkRunner().equals(TpccConfiguration.CLIENT_LIB_GSQL_RUNNER);
        Dialect dialect = isClientLibGSQLRunner ? Dialect.GOOGLE_STANDARD_SQL : Dialect.POSTGRESQL;
        String loadDataConnectionUrl =
            isClientLibGSQLRunner ? spannerConnectionUrl : pgadapterConnectionUrl;
        System.out.println("Checking schema");
        SchemaService schemaService = new SchemaService(loadDataConnectionUrl, dialect);
        schemaService.createSchema();
        System.out.println("Checked schema, starting benchmark");

        LOG.info("Starting data load");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DataLoadStatus status = new DataLoadStatus(tpccConfiguration);
        Future<Long> loadDataFuture =
            executor.submit(() -> loadData(status, loadDataConnectionUrl));
        executor.shutdown();
        Stopwatch watch = Stopwatch.createStarted();
        while (!loadDataFuture.isDone()) {
          //noinspection BusyWait
          Thread.sleep(1_000L);
          status.print(watch.elapsed());
        }
        System.out.printf("Finished loading %d rows\n", loadDataFuture.get());
      }

      if (!tpccConfiguration.isRunBenchmark()) {
        return;
      }
      if (TpccConfiguration.RUNNERS.contains(tpccConfiguration.getBenchmarkRunner())) {
        LOG.info("Starting benchmark");
        // Enable the OpenTelemetry metrics in the client library.
        OpenTelemetry openTelemetry = enableOpenTelemetryMetrics();
        Metrics metrics = new Metrics(openTelemetry, createMetricAttributes(spannerConfiguration));
        Statistics statistics = new Statistics(tpccConfiguration);
        ExecutorService executor =
            Executors.newFixedThreadPool(tpccConfiguration.getBenchmarkThreads());

        if (tpccConfiguration.getBenchmarkRunner().equals(TpccConfiguration.HIBERNATE_RUNNER)) {
          registry = buildHibernateConfiguration();
          sessionFactory = new MetadataSources(registry).buildMetadata().buildSessionFactory();
        }
        for (int i = 0; i < tpccConfiguration.getBenchmarkThreads(); i++) {
          if (tpccConfiguration
              .getBenchmarkRunner()
              .equals(TpccConfiguration.PGADAPTER_JDBC_RUNNER)) {
            // Run PGAdapter benchmark
            statistics.setRunnerName("PGAdapter benchmark");
            executor.submit(
                new JdbcBenchmarkRunner(
                    statistics,
                    pgadapterConnectionUrl,
                    tpccConfiguration,
                    pgAdapterConfiguration,
                    spannerConfiguration,
                    metrics));
          } else if (tpccConfiguration
              .getBenchmarkRunner()
              .equals(TpccConfiguration.SPANNER_JDBC_RUNNER)) {
            // Run Spanner JDBC benchmark
            statistics.setRunnerName("Spanner JDBC benchmark");
            executor.submit(
                new JdbcBenchmarkRunner(
                    statistics,
                    spannerConnectionUrl,
                    tpccConfiguration,
                    pgAdapterConfiguration,
                    spannerConfiguration,
                    metrics));
          } else if (tpccConfiguration
              .getBenchmarkRunner()
              .equals(TpccConfiguration.CLIENT_LIB_PG_RUNNER)) {
            // Run client library PG benchmark
            statistics.setRunnerName("Client library PG benchmark");
            executor.submit(
                new JavaClientBenchmarkRunner(
                    statistics,
                    tpccConfiguration,
                    pgAdapterConfiguration,
                    spannerConfiguration,
                    metrics,
                    Dialect.POSTGRESQL));
          } else if (tpccConfiguration
              .getBenchmarkRunner()
              .equals(TpccConfiguration.CLIENT_LIB_GSQL_RUNNER)) {
            // Run client library PG benchmark
            statistics.setRunnerName("Client library GSQL benchmark");
            executor.submit(
                new JavaClientBenchmarkRunner(
                    statistics,
                    tpccConfiguration,
                    pgAdapterConfiguration,
                    spannerConfiguration,
                    metrics,
                    Dialect.GOOGLE_STANDARD_SQL));
          } else if (tpccConfiguration
              .getBenchmarkRunner()
              .equals(TpccConfiguration.HIBERNATE_RUNNER)) {
            statistics.setRunnerName("Hibernate benchmark");
            executor.submit(
                new HibernateBenchmarkRunner(
                    statistics,
                    sessionFactory,
                    tpccConfiguration,
                    pgAdapterConfiguration,
                    spannerConfiguration,
                    hibernateConfiguration,
                    metrics,
                    Dialect.GOOGLE_STANDARD_SQL));
          }
        }

        Stopwatch watch = Stopwatch.createStarted();
        while (watch.elapsed().compareTo(tpccConfiguration.getBenchmarkDuration()) <= 0) {
          //noinspection BusyWait
          Thread.sleep(1_000L);
          //statistics.print(watch.elapsed());
        }
        executor.shutdownNow();
        if (!executor.awaitTermination(60L, TimeUnit.SECONDS)) {
          throw new TimeoutException("Timed out while waiting for benchmark runners to shut down");
        }
      } else {
        throw new RuntimeException(
            "Unknown benchmark runner option: " + tpccConfiguration.getBenchmarkRunner());
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    } finally {
      if (server != null) {
        server.stopServer();
        server.awaitTerminated();
      }
      if (sessionFactory != null) {
        sessionFactory.close();
        registry.close();
      }
    }
  }

  private long loadData(DataLoadStatus status, String connectionUrl) throws Exception {
    try (DataLoader loader = new DataLoader(status, connectionUrl, tpccConfiguration)) {
      return loader.loadData();
    }
  }

  private OpenTelemetry enableOpenTelemetryMetrics() throws IOException {
    // Enable OpenTelemetry metrics before injecting OpenTelemetry object.
    SpannerOptions.enableOpenTelemetryMetrics();

    AutoConfiguredOpenTelemetrySdkBuilder openTelemetryBuilder =
        AutoConfiguredOpenTelemetrySdk.builder();

    MetricExporter cloudMonitoringExporter =
        GoogleCloudMetricExporter.createWithConfiguration(
            MetricConfiguration.builder()
                // Configure the cloud project id.
                .setProjectId(spannerConfiguration.getProject())
                .build());
    openTelemetryBuilder.addMeterProviderCustomizer(
        (sdkMeterProviderBuilder, configProperties) ->
            sdkMeterProviderBuilder.registerMetricReader(
                PeriodicMetricReader.builder(cloudMonitoringExporter).build()));

    OpenTelemetry openTelemetry = openTelemetryBuilder.build().getOpenTelemetrySdk();
    GlobalOpenTelemetry.set(openTelemetry);
    return openTelemetry;
  }

  static Attributes createMetricAttributes(SpannerConfiguration spannerConfiguration) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    attributesBuilder.put("database", spannerConfiguration.getDatabase());
    attributesBuilder.put("instance_id", spannerConfiguration.getInstance());
    attributesBuilder.put("project_id", spannerConfiguration.getProject());
    return attributesBuilder.build();
  }

  private ProxyServer startPGAdapter() {
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(spannerConfiguration.getProject())
            .setInstance(spannerConfiguration.getInstance())
            .setDatabase(spannerConfiguration.getDatabase())
            .setNumChannels(pgAdapterConfiguration.getNumChannels())
            .setSessionPoolOptions(
                SessionPoolOptions.newBuilder()
                    .setTrackStackTraceOfSessionCheckout(false)
                    .setMinSessions(pgAdapterConfiguration.getMinSessions())
                    .setMaxSessions(pgAdapterConfiguration.getMaxSessions())
                    .build())
            .disableUnixDomainSockets();
    if (pgAdapterConfiguration.isEnableOpenTelemetry()) {
      builder
          .setEnableOpenTelemetry()
          .setOpenTelemetryTraceRatio(pgAdapterConfiguration.getOpenTelemetrySampleRate());
    }
    if (pgAdapterConfiguration.isEnableOpenTelemetryMetrics()) {
      SpannerOptions.enableOpenTelemetryMetrics();
      builder.setEnableOpenTelemetryMetrics();
    }
    if (!Strings.isNullOrEmpty(pgAdapterConfiguration.getCredentials())) {
      builder.setCredentialsFile(pgAdapterConfiguration.getCredentials());
    }
    ProxyServer server = new ProxyServer(builder.build());
    server.startServer();
    server.awaitRunning();

    return server;
  }

  private StandardServiceRegistry buildHibernateConfiguration() {
    return new StandardServiceRegistryBuilder()
        .configure()
        .applySetting("hibernate.show_sql", hibernateConfiguration.isShowSql())
        .applySetting("hibernate.jdbc.batch_size", hibernateConfiguration.getBatchSize())
        .applySetting("hibernate.connection.pool_size", hibernateConfiguration.getPoolSize())
        .applySetting("hibernate.order_inserts", hibernateConfiguration.isOrderInserts())
        .applySetting("hibernate.order_updates", hibernateConfiguration.isOrderUpdates())
        .applySetting(
            "hibernate.jdbc.batch_versioned_data", hibernateConfiguration.isBatchVersionedData())
        .build();
  }
}
