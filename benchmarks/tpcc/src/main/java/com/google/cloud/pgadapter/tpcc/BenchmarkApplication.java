package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.pgadapter.tpcc.config.PGAdapterConfiguration;
import com.google.cloud.pgadapter.tpcc.config.SpannerConfiguration;
import com.google.cloud.pgadapter.tpcc.config.TpccConfiguration;
import com.google.cloud.pgadapter.tpcc.dataloader.DataLoader;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Strings;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    SpringApplication.run(BenchmarkApplication.class, args);
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
      // loadData(connectionUrl);
      BenchmarkRunner runner = new BenchmarkRunner(connectionUrl);
      ExecutorService executor = Executors.newFixedThreadPool(8);
      executor.submit(runner);

      Thread.sleep(5000L);
      executor.shutdownNow();
      if (!executor.awaitTermination(60L, TimeUnit.SECONDS)) {
        throw new TimeoutException("Timed out while waiting for benchmark runners to shut down");
      }
    } finally {
      if (server != null) {
        server.stopServer();
        server.awaitTerminated();
      }
    }
  }

  private void loadData(String connectionUrl) throws Exception {
    try (DataLoader loader = new DataLoader(connectionUrl, tpccConfiguration)) {
      loader.loadData(tpccConfiguration.getFactor());
    }
  }

  private ProxyServer startPGAdapter() {
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(spannerConfiguration.getProject())
            .setInstance(spannerConfiguration.getInstance())
            .setDatabase(spannerConfiguration.getDatabase())
            .disableUnixDomainSockets();
    if (!Strings.isNullOrEmpty(pgAdapterConfiguration.getCredentials())) {
      builder.setCredentialsFile(pgAdapterConfiguration.getCredentials());
    }
    ProxyServer server = new ProxyServer(builder.build());
    server.startServer();
    server.awaitRunning();

    return server;
  }
}
