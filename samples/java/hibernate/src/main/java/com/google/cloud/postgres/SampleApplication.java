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

package com.google.cloud.postgres;

import com.google.cloud.postgres.models.HibernateConfiguration;
import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Objects;
import java.util.Scanner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Sample application for connecting to a Cloud Spanner PostgreSQL database using Hibernate. This
 * sample application either starts an embedded in-process PGAdapter instance that connects to an
 * existing Cloud Spanner database, or it starts a Docker container that contains PGAdapter and the
 * Cloud Spanner emulator and automatically creates the database there.
 *
 * <p>Run the sample application using the Cloud Spanner Emulator as follows:
 *
 * <pre>{@code
 * mvn exec:java
 * }</pre>
 *
 * <p>Run the sample application on an existing Cloud Spanner database using default credentials as
 * follows:
 *
 * <pre>{@code
 * mvn exec:java \
 *   -Dexec.args=" \
 *     -p my-project \
 *     -i my-instance \
 *     -d my-database"
 * }</pre>
 *
 * <p>Run the sample application on an existing Cloud Spanner database using a specific credential
 * file as follows:
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
    String connectionUrl =
        String.format("jdbc:postgresql://localhost:%d/%s?", server.getPort(), database);
    try {
      // Create the sample data model.
      createDataModel(connectionUrl);

      // Run the sample application.
      runSample(connectionUrl);
    } finally {
      server.shutdown();
      SpannerPool.closeSpannerPool();
    }
  }

  static void runSample(String connectionUrl) {
    System.out.println("Starting Hibernate Sample");
    HibernateSample hibernateSample =
        new HibernateSample(HibernateConfiguration.createHibernateConfiguration(connectionUrl));
    hibernateSample.runHibernateSample();
    System.out.println("Hibernate Sample Ended Successfully");
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
        "The id of the database that the JDBC " + "driver should connect to.");
    options.addOption(
        "c",
        "credentials",
        true,
        "The full path of a Google Cloud credentials file that should be used. "
            + "If not specified, the sample application will try to read the application default "
            + "credentials.");
    return options;
  }

  /**
   * Generic interface for PGAdapter either running in-process, or in a Docker container together
   * with the Spanner emulator.
   */
  interface Server {
    int getPort();

    void shutdown();
  }

  /** A wrapper around a Docker container that runs PGAdapter + Spanner emulator. */
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

  static void createDataModel(String connectionUrl) throws SQLException, IOException {
    // Skip creating the schema if it already exists.
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select count(1) "
                      + "from information_schema.tables "
                      + "where table_schema='public' "
                      + "and table_name in ('singers', 'albums', 'tracks', 'venues', 'concerts')")) {
        if (resultSet.next()) {
          if (resultSet.getInt(1) == 5) {
            return;
          }
        }
      }
    }

    // Create the sample schema.
    StringBuilder builder = new StringBuilder();
    try (Scanner scanner =
        new Scanner(
            new FileReader(
                Objects.requireNonNull(SampleApplication.class.getResource("/sample-schema.sql"))
                    .getPath()))) {
      while (scanner.hasNextLine()) {
        builder.append(scanner.nextLine()).append("\n");
      }
    }
    // Note: We know that all semicolons in this file are outside of literals etc.
    String[] ddl = builder.toString().split(";");
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (Statement statement = connection.createStatement()) {
        for (String sql : ddl) {
          statement.execute(sql);
        }
      }
    }
  }
}
