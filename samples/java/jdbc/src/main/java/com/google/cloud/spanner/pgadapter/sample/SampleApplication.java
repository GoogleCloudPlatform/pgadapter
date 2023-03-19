// Copyright 2022 Google LLC
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

import com.google.cloud.spanner.connection.SpannerPool;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Sample application for connecting to Cloud Spanner using the PostgreSQL JDBC driver. This sample
 * application starts an embedded in-process PGAdapter instance together with the sample application
 * and connects to it using a Unix domain socket.
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
  static {
    try {
      Class.forName(org.postgresql.Driver.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load JDBC driver: " + e.getMessage(), e);
    }
  }

  /**
   * Reads the options from the command line, starts PGAdapter, connects using the PostgreSQL JDBC
   * driver, and executes a simple query on Cloud Spanner.
   */
  public static void main(String[] args) throws SQLException {
    // Read command line arguments.
    CommandLine commandLine = parseCommandLine(args);
    String project = commandLine.getOptionValue('p', "my-project");
    String instance = commandLine.getOptionValue('i', "my-instance");
    String database = commandLine.getOptionValue('d', "my-database");
    String credentials = commandLine.getOptionValue('c');

    // Start PGAdapter in-process. The server will be started on a random available port.
    ProxyServer server = startPGAdapter(project, instance, credentials);
    try {
      // Create a connection URL that will use Unix domain sockets to connect to PGAdapter.
      String connectionUrl =
          String.format(
              "jdbc:postgresql://localhost/%s?"
                  + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
                  + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
              database, server.getLocalPort());
      runSample(connectionUrl);
    } finally {
      server.stopServer();
      SpannerPool.closeSpannerPool();
    }
  }

  /** Connects to PGAdapter using the PostgreSQL JDBC driver and executes a simple query. */
  static void runSample(String connectionUrl) throws SQLException {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select 'Hello World!' as greeting")) {
        while (resultSet.next()) {
          System.out.printf("\nGreeting: %s\n\n", resultSet.getString("greeting"));
        }
      }
    }
  }

  /**
   * Starts PGAdapter in-process and returns a reference to the server. Use this reference to
   * gracefully shut down the server when your application shuts down.
   *
   * @param project the Google Cloud project that PGAdapter should connect to
   * @param instance the Cloud Spanner instance that PGAdapter should connect to
   * @param credentials the full path of a credentials file that PGAdapter should use, or <code>null
   *     </code> if PGAdapter should use the application default credentials
   */
  static ProxyServer startPGAdapter(String project, String instance, String credentials) {
    OptionsMetadata options =
        new OptionsMetadata(
            credentials == null
                ? new String[] {
                  "-p", project,
                  "-i", instance,
                  "-s", "0" // Start PGAdapter on any available port.
                }
                : new String[] {
                  "-p", project,
                  "-i", instance,
                  "-c", credentials,
                  "-s", "0" // Start PGAdapter on any available port.
                });
    ProxyServer server = new ProxyServer(options);
    server.startServer();

    return server;
  }

  static CommandLine parseCommandLine(String[] args) {
    String commandLineArguments =
        "pgadapter -p <project> -i <instance> -d <database> [-c <credentials_file>]";

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
        "p", "project", true, "The Google Cloud project ID that " + "PGAdapter should connect to.");
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
}
