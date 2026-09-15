// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter;

import com.google.cloud.spanner.pgadapter.logging.DefaultLogConfiguration;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * A simplified connector executable that starts PGAdapter on a dynamically assigned localhost port
 * and then runs a standard PostgreSQL client tool (for example {@code psql}) against it.
 *
 * <p>The tool is started with {@code PGHOST} and {@code PGPORT} pointing at the PGAdapter instance
 * that was started by this class. All command line arguments are passed through to the tool
 * unmodified, which means that any PostgreSQL tool that respects the standard {@code PG*}
 * environment variables works out of the box.
 *
 * <p>Usage: {@code spgc <command> [args...]}
 *
 * <p>Example: {@code spgc psql -d "projects/my-project/instances/my-inst/databases/my-db"}
 */
public class SpannerPGConnector {
  /** Name that is used in usage and error messages. */
  static final String PROGRAM_NAME = "spanner-pg-connector";

  static final String PROJECT_ENV_VAR = "GOOGLE_CLOUD_PROJECT";
  static final String INSTANCE_ENV_VAR = "SPANNER_INSTANCE";
  static final String DATABASE_ENV_VAR = "SPANNER_DATABASE";
  static final String EMULATOR_ENV_VAR = "SPANNER_EMULATOR_HOST";

  /** Exit code that is used by POSIX shells for 'command not found'. */
  static final int EXIT_CODE_COMMAND_NOT_FOUND = 127;

  /** Arguments that make this program print its usage information instead of running a command. */
  private static final ImmutableSet<String> HELP_ARGUMENTS =
      ImmutableSet.of("-?", "--help", "-help", "help");

  /** Arguments that make this program print its version instead of running a command. */
  private static final ImmutableSet<String> VERSION_ARGUMENTS =
      ImmutableSet.of("-V", "--version", "-version", "version");

  /**
   * Long options that would redirect the client tool to another server than the PGAdapter instance
   * that is started by this program.
   */
  private static final ImmutableSet<String> CONNECTION_OVERRIDE_LONG_OPTIONS =
      ImmutableSet.of("host", "hostaddr", "port");

  /**
   * Short options that would redirect the client tool to another server than the PGAdapter instance
   * that is started by this program. These are the standard libpq short options for host and port.
   */
  private static final String CONNECTION_OVERRIDE_SHORT_OPTIONS = "hp";

  /** Long options that are used by PostgreSQL tools to select the database. */
  private static final ImmutableSet<String> DATABASE_LONG_OPTIONS =
      ImmutableSet.of("dbname", "database");

  /**
   * Environment variables that must be removed from the environment of the client tool, because
   * they would either redirect it to a different server or require SSL, which the PGAdapter
   * instance that is started by this program does not support.
   */
  private static final ImmutableSet<String> REMOVED_ENVIRONMENT_VARIABLES =
      ImmutableSet.of(
          "PGHOSTADDR",
          "PGSERVICE",
          "PGSERVICEFILE",
          "PGREQUIRESSL",
          "PGCHANNELBINDING",
          "PGSSLNEGOTIATION");

  public static void main(String[] args) {
    System.exit(run(args, System.getenv(), System.out, System.err));
  }

  /**
   * Runs the given command against a PGAdapter instance that is started by this method and returns
   * the exit code of the command. This method is the testable equivalent of {@link #main(String[])}
   * and does not call {@link System#exit(int)}.
   */
  @VisibleForTesting
  static int run(String[] args, Map<String, String> environment, PrintStream out, PrintStream err) {
    if (args.length == 0) {
      printUsage(err);
      return 1;
    }
    if (HELP_ARGUMENTS.contains(args[0])) {
      printUsage(out);
      return 0;
    }
    if (VERSION_ARGUMENTS.contains(args[0])) {
      out.printf("%s %s%n", PROGRAM_NAME, Server.getVersion());
      return 0;
    }
    String connectionOverride = findConnectionOverride(args);
    if (connectionOverride != null) {
      err.printf(
          "Error: explicit host and port arguments are not supported (found '%s').%n",
          connectionOverride);
      err.printf(
          "%s starts PGAdapter on a dynamically assigned localhost port and routes the%n",
          PROGRAM_NAME);
      err.println("client to that port using the PGHOST and PGPORT environment variables.");
      err.println("Use -d/--dbname to select the Cloud Spanner database that you want to connect");
      err.printf("to, for example: %s psql -d my-database%n", PROGRAM_NAME);
      return 1;
    }

    try {
      // The client tool owns stdout and stderr, so PGAdapter must not write anything to them.
      DefaultLogConfiguration.disableLogging();
    } catch (IOException ignore) {
      // Failing to reconfigure logging must not prevent the client tool from starting.
    }
    ProxyServer proxyServer = null;
    AtomicBoolean proxyServerStopped = new AtomicBoolean(false);
    Thread shutdownHook = null;
    try {
      OptionsMetadata options = createOptionsMetadata(environment);
      OpenTelemetry openTelemetry = Server.setupOpenTelemetry(options);
      proxyServer = new ProxyServer(options, openTelemetry);
      proxyServer.startServer();

      // Stops the client tool and PGAdapter if this process is terminated. Server#handleTerm
      // routes SIGTERM back into this hook.
      shutdownHook =
          Server.createShutdownHook(
              proxyServer, proxyServerStopped, PROGRAM_NAME + "-shutdown-handler");
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      return runCommand(proxyServer, getDatabase(args, environment), args);
    } catch (IOException ioException) {
      // ProcessBuilder#start() throws an IOException if the command could not be started, which is
      // most commonly caused by the tool not being installed or not being on the PATH.
      err.printf("%s: %s: %s%n", PROGRAM_NAME, args[0], ioException.getMessage());
      err.printf(
          "%s does not install the client tool. Make sure that '%s' is installed and available%n",
          PROGRAM_NAME, args[0]);
      err.println("on your PATH.");
      return EXIT_CODE_COMMAND_NOT_FOUND;
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      Server.destroyClientProcess();
      return 130;
    } catch (Throwable throwable) {
      // getMessage() is null for exceptions such as NullPointerException.
      String message =
          throwable.getMessage() == null ? throwable.toString() : throwable.getMessage();
      err.printf("%s: failed to start PGAdapter: %s%n", PROGRAM_NAME, message);
      return 1;
    } finally {
      Server.stopProxyServer(proxyServer, proxyServerStopped);
      Server.removeShutdownHook(shutdownHook);
    }
  }

  /** Creates the {@link OptionsMetadata} that is used for the PGAdapter instance. */
  @VisibleForTesting
  static OptionsMetadata createOptionsMetadata(Map<String, String> environment) {
    OptionsMetadata.Builder builder =
        OptionsMetadata.newBuilder()
            // Use a dynamically assigned port to prevent port conflicts.
            .setPort(0)
            // The proxy only listens on localhost and is only used by the client tool that is
            // started by this program.
            .setSslMode(SslMode.Disable)
            // The default (Batch) rejects mixed DDL/DML batches and DDL inside an explicit
            // transaction, which breaks `psql --single-transaction -f schema.sql`. Despite its
            // name, Batch does not control DDL batching, so nothing is lost here.
            .setDdlTransactionMode(DdlTransactionMode.AutocommitExplicitTransaction)
            .disableUnixDomainSockets();

    String project = environment.get(PROJECT_ENV_VAR);
    if (!isNullOrEmpty(project)) {
      builder.setProject(project);
    }
    String instance = environment.get(INSTANCE_ENV_VAR);
    if (!isNullOrEmpty(instance)) {
      builder.setInstance(instance);
    }
    if (!isNullOrEmpty(environment.get(EMULATOR_ENV_VAR))) {
      builder.autoConfigureEmulator();
    }
    return builder.build();
  }

  /**
   * Starts the given command and waits for it to finish. The command is started with {@code PGHOST}
   * and {@code PGPORT} pointing at the given {@link ProxyServer}.
   */
  @VisibleForTesting
  static int runCommand(ProxyServer proxyServer, @Nullable String database, String... command)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(command);
    configureEnvironment(builder.environment(), proxyServer.getLocalPort(), database);
    builder.inheritIO();
    return Server.startAndWait(builder);
  }

  /**
   * Modifies the given environment so a standard PostgreSQL client tool connects to the PGAdapter
   * instance that was started by this program.
   */
  @VisibleForTesting
  static void configureEnvironment(
      Map<String, String> environment, int port, @Nullable String database) {
    environment.put("PGHOST", "localhost");
    environment.put("PGPORT", String.valueOf(port));
    if (database != null) {
      environment.put("PGDATABASE", database);
    }
    // PGAdapter is started without SSL, so a client with PGSSLMODE=require would otherwise fail.
    environment.put("PGSSLMODE", "disable");
    environment.put("PGGSSENCMODE", "disable");
    environment.keySet().removeAll(REMOVED_ENVIRONMENT_VARIABLES);
  }

  /**
   * Returns the database that the client tool will connect to, or null if it cannot be determined
   * from the command line arguments or the environment. The returned value is used as the {@code
   * PGDATABASE} environment variable for the client tool.
   */
  @VisibleForTesting
  @Nullable
  static String getDatabase(String[] args, Map<String, String> environment) {
    String database = getDatabaseFromArguments(args);
    if (database != null) {
      return database;
    }
    String databaseFromEnvironment = environment.get(DATABASE_ENV_VAR);
    return isNullOrEmpty(databaseFromEnvironment) ? null : databaseFromEnvironment;
  }

  /**
   * Returns the value of the {@code -d}, {@code --dbname} or {@code --database} argument, or null
   * if the command line arguments do not contain any of these.
   */
  @VisibleForTesting
  @Nullable
  static String getDatabaseFromArguments(String[] args) {
    // Skip args[0], as that is the name of the command that should be executed.
    for (int index = 1; index < args.length; index++) {
      String arg = args[index];
      if ("--".equals(arg)) {
        // Everything after '--' is a positional argument.
        break;
      }
      if (arg.startsWith("--")) {
        String name = arg.substring(2);
        int equalsIndex = name.indexOf('=');
        if (equalsIndex > -1) {
          if (DATABASE_LONG_OPTIONS.contains(name.substring(0, equalsIndex))) {
            return name.substring(equalsIndex + 1);
          }
        } else if (DATABASE_LONG_OPTIONS.contains(name)) {
          return valueOf(args, index);
        }
      } else if (arg.startsWith("-d") && arg.length() > 1) {
        return arg.length() == 2 ? valueOf(args, index) : arg.substring(2);
      }
    }
    return null;
  }

  /**
   * Returns the first command line argument that would redirect the client tool to a different
   * server, or null if there is none. Best-effort: recognizes the standard libpq spellings, but not
   * bundled short options such as {@code -tAh}.
   */
  @VisibleForTesting
  @Nullable
  static String findConnectionOverride(String[] args) {
    // Skip args[0], as that is the name of the command that should be executed.
    for (int index = 1; index < args.length; index++) {
      String arg = args[index];
      if ("--".equals(arg)) {
        // Everything after '--' is a positional argument.
        break;
      }
      if (arg.length() < 2 || !arg.startsWith("-")) {
        continue;
      }
      if (arg.startsWith("--")) {
        String name = arg.substring(2);
        int equalsIndex = name.indexOf('=');
        if (equalsIndex > -1) {
          name = name.substring(0, equalsIndex);
        }
        if (CONNECTION_OVERRIDE_LONG_OPTIONS.contains(name)) {
          return arg;
        }
      } else if (CONNECTION_OVERRIDE_SHORT_OPTIONS.indexOf(arg.charAt(1)) > -1) {
        return arg;
      }
    }
    return null;
  }

  /**
   * Returns the value that follows the option at the given index, or null if the option is the last
   * argument.
   */
  @Nullable
  private static String valueOf(String[] args, int index) {
    return index + 1 < args.length ? args[index + 1] : null;
  }

  private static boolean isNullOrEmpty(@Nullable String value) {
    return value == null || value.isEmpty();
  }

  private static void printUsage(PrintStream out) {
    out.printf("Usage: %s <command> [args...]%n", PROGRAM_NAME);
    out.println();
    out.println(
        "Starts PGAdapter on a dynamically assigned localhost port and runs <command> against it.");
    out.println(
        "All arguments are passed through to <command> unmodified. The command must be installed");
    out.println("and available on your PATH.");
    out.println();
    out.println("Environment variables:");
    out.printf("  %-24s Google Cloud project ID.%n", PROJECT_ENV_VAR);
    out.printf("  %-24s Cloud Spanner instance ID.%n", INSTANCE_ENV_VAR);
    out.printf(
        "  %-24s Default Cloud Spanner database. Used if the command does not%n", DATABASE_ENV_VAR);
    out.printf("  %-24s specify a database with -d/--dbname.%n", "");
    out.printf(
        "  %-24s Connect to the Cloud Spanner emulator at this host:port instead%n",
        EMULATOR_ENV_VAR);
    out.printf("  %-24s of Cloud Spanner. Must be unset for real Cloud Spanner.%n", "");
    out.println();
    out.println("Examples:");
    out.printf("  %s psql -d my-database%n", PROGRAM_NAME);
    out.printf("  %s psql -d my-database -c \"select 1\"%n", PROGRAM_NAME);
    out.printf(
        "  %s psql -d \"projects/my-project/instances/my-instance/databases/my-database\"%n",
        PROGRAM_NAME);
  }
}
