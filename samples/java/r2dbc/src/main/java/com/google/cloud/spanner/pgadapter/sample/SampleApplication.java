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

package com.google.cloud.spanner.pgadapter.sample;

import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.OPTIONS;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;

/**
 * Sample application for connecting to Cloud Spanner using the PostgreSQL R2DBC driver. This sample
 * application starts an embedded in-process PGAdapter instance together with the sample application
 * and connects to it.
 *
 * <p>Run the sample application using the Cloud Spanner Emulator as follows:
 *
 * <pre>{@code
 * mvn exec:java
 * }</pre>
 *
 * <p>Run the sample application using default credentials as follows:
 *
 * <pre>{@code
 * mvn exec:java \
 *   -Dexec.args=" \
 *     -p my-project \
 *     -i my-instance \
 *     -d my-database"
 * }</pre>
 *
 * <p>Run the sample application using a specific credential file as follows:
 *
 * <pre>{@code
 * mvn exec:java \
 *   -Dexec.args=" \
 *     -p my-project \
 *     -i my-instance \
 *     -d my-database \
 *     -c /path/to/credentials.json"
 * }</pre>
 */
public class SampleApplication {

  /**
   * Reads the options from the command line, starts PGAdapter, connects using the PostgreSQL R2DBC
   * driver, and executes a simple query on Cloud Spanner.
   */
  public static void main(String[] args) throws Exception {
    // Read command line arguments.
    CommandLine commandLine = parseCommandLine(args);
    // Use the Cloud Spanner Emulator if no project/instance/database has been specified.
    boolean useEmulator = commandLine.getOptions().length == 0;

    String project = commandLine.getOptionValue('p', "my-project");
    String instance = commandLine.getOptionValue('i', "my-instance");
    String database = commandLine.getOptionValue('d', "my-database");
    String credentials = commandLine.getOptionValue('c');

    // Start PGAdapter in-process or as a Docker container with the Cloud Spanner Emulator.
    // The server will be started on a random available port.
    Server server =
        useEmulator ? startPGAdapterWithEmulator() : startPGAdapter(project, instance, credentials);
    try {
      runSample(server.getPort(), database);
    } finally {
      server.shutdown();
      SpannerPool.closeSpannerPool();
    }
  }

  /** Connects to PGAdapter using the PostgreSQL R2DBC driver and executes a simple query. */
  static void runSample(int port, String database) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("lock_timeout", "10s");
    options.put("statement_timeout", "5m");

    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "postgresql")
        .option(HOST, "localhost")
        .option(PORT, port) 
        .option(USER, "")
        .option(PASSWORD, "")
        .option(DATABASE, database)
        .option(OPTIONS, options)
        .build());

    CountDownLatch finished = new CountDownLatch(1);
    Flux.usingWhen(
            connectionFactory.create(),
            connection ->
                Flux.from(connection.createStatement(
                            "select 'Hello World!' as greeting")
                        .execute())
                    .flatMap(result ->
                        result.map(row -> row.get("greeting", String.class))),
            Connection::close)
        .doOnNext(greeting -> System.out.printf("\nGreeting: %s\n\n", greeting))
        .doOnError(Throwable::printStackTrace)
        .doFinally(ignore -> finished.countDown())
        .subscribe();

    finished.await();
  }
  
  /**
   * Starts PGAdapter in-process and returns a reference to the server. Use this reference to
   * gracefully shut down the server when your application shuts down.
   *
   * @param project the Google Cloud project that PGAdapter should connect to
   * @param instance the Cloud Spanner instance that PGAdapter should connect to
   * @param credentialsFile the full path of a credentials file that PGAdapter should use, or <code>
   *     null</code> if PGAdapter should use the application default credentials
   */
  static Server startPGAdapter(String project, String instance, String credentialsFile) {
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            .setProject(project)
            .setInstance(instance)
            // Start PGAdapter on any available port.
            .setPort(0);
    if (credentialsFile != null) {
      builder.setCredentialsFile(credentialsFile);
    }
    ProxyServer server = new ProxyServer(builder.build());
    server.startServer();
    server.awaitRunning();

    return new PGAdapter(server);
  }

  /** Starts a Docker container that contains both PGAdapter and the Cloud Spanner Emulator. */
  static Server startPGAdapterWithEmulator() {
    GenericContainer<?> container =
        new GenericContainer<>(
            DockerImageName.parse("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator"));
    container.addExposedPort(5432);
    container.setWaitStrategy(Wait.forListeningPorts(5432));
    container.start();

    return new PGAdapterWithEmulator(container);
  }

  static CommandLine parseCommandLine(String[] args) {
    String commandLineArguments =
        "pgadapter [-p <project> -i <instance> -d <database> -c <credentials_file>]";

    CommandLineParser parser = new DefaultParser();
    HelpFormatter help = new HelpFormatter();
    help.setWidth(120);
    Options options = createOptions();
    try {
      CommandLine commandLine = parser.parse(options, args);
      if (commandLine.hasOption('h')) {
        help.printHelp(commandLineArguments, options);
        System.exit(0);
      }
      return commandLine;
    } catch (ParseException e) {
      help.printHelp(commandLineArguments, options);
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(
        "p", "project", true, "The Google Cloud project ID that PGAdapter should connect to.");
    options.addOption(
        "i",
        "instance",
        true,
        "The id of the Cloud Spanner instance that PGAdapter should connect to.");
    options.addOption(
        "d",
        "database",
        true,
        "The id of the database that the R2DBC driver should connect to.");
    options.addOption(
        "c",
        "credentials",
        true,
        "The full path of a Google Cloud credentials file that should be used. "
            + "If not specified, the sample application will try to read the application default "
            + "credentials.");
    return options;
  }

  interface Server {
    int getPort();

    void shutdown();
  }

  static class PGAdapterWithEmulator implements Server {
    private final GenericContainer<?> container;

    PGAdapterWithEmulator(GenericContainer<?> container) {
      this.container = container;
    }

    @Override
    public int getPort() {
      return container.getMappedPort(5432);
    }

    @Override
    public void shutdown() {
      container.stop();
    }
  }

  static class PGAdapter implements Server {
    private final ProxyServer server;

    PGAdapter(ProxyServer server) {
      this.server = server;
    }

    @Override
    public int getPort() {
      return server.getLocalPort();
    }

    @Override
    public void shutdown() {
      server.stopServer();
      server.awaitTerminated();
    }
  }
}
