// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.myadapter.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.myadapter.translator.models.QueryReplacementConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.spanner.v1.DatabaseName;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/** Metadata extractor for CLI. */
public class OptionsMetadata {

  /** Returns true if the current JVM is Java 8. */
  public static boolean isJava8() {
    return System.getProperty("java.version").startsWith("1.8");
  }

  public enum SslMode {
    /** Disables SSL connections. This is the default. */
    Disable {
      @Override
      public boolean isSslEnabled() {
        return false;
      }
    },
    /** Enables SSL connections. Requires a valid key store to be configured. */
    Enable,
    /** Requires SSL connections. Non-SSL connections will be rejected. */
    Require;

    public boolean isSslEnabled() {
      return true;
    }
  }

  public enum DdlTransactionMode {

    // Disables all DDL batching and DDL statements in transactions. Only single DDL statements
    // outside batches and transactions are allowed.
    Single,

    // Allows batches that contain only DDL statements outside explicit transactions.
    // Does not allow mixing of DML, DQL and DDL statements in one batch.
    // This is the default mode.
    Batch,

    // DDL statements automatically commit the active implicit transaction.
    // Allows batches that contain mixes of DML, DQL and DDL statements.
    // Fails if a DDL statement is executed in an explicit transaction.
    AutocommitImplicitTransaction,

    // DDL statements automatically commit the active explicit or implicit transaction.
    // Allows batches that contain mixes of DML, DQL and DDL statements.
    // Explicit transaction blocks that contain DDL statements will not be atomic.
    AutocommitExplicitTransaction,
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = Logger.getLogger(OptionsMetadata.class.getName());
  private static final String DEFAULT_SERVER_VERSION = "8.0.31";
  // TODO change user agent to myspan-adapter or appropriate.
  private static final String DEFAULT_USER_AGENT = "pg-adapter";

  private static final String QUERY_TRANSLATOR_FILE_PATH = "j";
  private static final String DEFAULT_QUERY_TRANSLATOR_FILE =
      "metadata/unsupported_query_replacement.json";

  private static final String OPTION_SERVER_PORT = "s";
  private static final String OPTION_SOCKET_DIR = "dir";
  private static final String OPTION_MAX_BACKLOG = "max_backlog";
  private static final String OPTION_PROJECT_ID = "p";
  private static final String OPTION_INSTANCE_ID = "i";
  private static final String OPTION_DATABASE_NAME = "d";
  private static final String OPTION_CREDENTIALS_FILE = "c";
  private static final String OPTION_DISABLE_LOCALHOST_CHECK = "x";
  private static final String CLI_ARGS =
      "myadapter -p <project> -i <instance> -d <database> -c <credentials_file>";
  private static final String OPTION_HELP = "h";
  private static final String DEFAULT_PORT = "3306";
  private static final int MIN_PORT = 0, MAX_PORT = 65535;
  private static final String DEFAULT_SOCKET_DIR = "/tmp";
  private static final String SOCKET_FILE_NAME = ".s.MYSQL.%d";
  private static final int DEFAULT_MAX_BACKLOG = 1000;
  /*Note: this is a private preview feature, not meant for GA version. */
  private static final String OPTION_SPANNER_ENDPOINT = "e";
  private static final String OPTION_JDBC_PROPERTIES = "r";
  private static final String OPTION_SERVER_VERSION = "v";
  private static final String OPTION_DEBUG_MODE = "debug";

  private final String osName;
  private final CommandLine commandLine;
  private final String defaultConnectionUrl;
  private final int proxyPort;
  private final String socketFile;
  private final int maxBacklog;
  private final boolean disableLocalhostCheck;
  private final Map<String, String> propertyMap;
  private final String serverVersion;
  private final boolean debugMode;

  private QueryReplacementConfig queryReplacementConfig;

  public OptionsMetadata(String[] args) {
    this(System.getProperty("os.name", ""), args);
  }

  OptionsMetadata(String osName, String[] args) {
    this.osName = osName;
    this.commandLine = buildOptions(args);
    if (this.commandLine.hasOption(OPTION_DATABASE_NAME)
        && !(this.commandLine.hasOption(OPTION_PROJECT_ID)
            && this.commandLine.hasOption(OPTION_INSTANCE_ID))) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Cannot specify a database ID without also specifying a project ID and instance ID. "
              + "Use the options -p <project-id> -i <instance-id> -d <database-id> to specify the "
              + "database that all connections to this instance of PGAdapter should use.");
    }
    if (this.commandLine.hasOption(OPTION_PROJECT_ID)
        && this.commandLine.hasOption(OPTION_INSTANCE_ID)
        && this.commandLine.hasOption(OPTION_DATABASE_NAME)) {
      this.defaultConnectionUrl =
          buildConnectionURL(this.commandLine.getOptionValue(OPTION_DATABASE_NAME));
    } else {
      this.defaultConnectionUrl = null;
    }
    this.proxyPort = buildProxyPort(commandLine);
    this.socketFile = buildSocketFile(commandLine);
    this.maxBacklog = buildMaxBacklog(commandLine);
    this.propertyMap = parseProperties(commandLine.getOptionValue(OPTION_JDBC_PROPERTIES, ""));
    this.disableLocalhostCheck = commandLine.hasOption(OPTION_DISABLE_LOCALHOST_CHECK);
    this.serverVersion = commandLine.getOptionValue(OPTION_SERVER_VERSION, DEFAULT_SERVER_VERSION);
    this.debugMode = commandLine.hasOption(OPTION_DEBUG_MODE);

    this.queryReplacementConfig = parseQueryTranslatorFile(getQueryTranslatorFilePath());
  }

  public OptionsMetadata(String defaultConnectionUrl, int proxyPort) {
    this(System.getProperty("os.name", ""), defaultConnectionUrl, proxyPort);
  }

  OptionsMetadata(String osName, String defaultConnectionUrl, int proxyPort) {
    this.osName = osName;
    this.commandLine = null;
    this.defaultConnectionUrl = defaultConnectionUrl;
    this.proxyPort = proxyPort;
    this.socketFile = isWindows() ? "" : DEFAULT_SOCKET_DIR + File.separatorChar + SOCKET_FILE_NAME;
    this.maxBacklog = DEFAULT_MAX_BACKLOG;
    this.propertyMap = new HashMap<>();
    this.disableLocalhostCheck = false;
    this.serverVersion = DEFAULT_SERVER_VERSION;
    this.debugMode = false;
  }

  private Map<String, String> parseProperties(String propertyOptions) {
    Map<String, String> properties = new HashMap<>();
    if (!propertyOptions.isEmpty()) {
      String[] propertyList = propertyOptions.split(";");
      for (int i = 0; i < propertyList.length; ++i) {
        String[] keyValue = propertyList[i].split("=");
        if (keyValue.length == 2) {
          properties.put(keyValue[0], keyValue[1]);
        } else {
          throw new IllegalArgumentException("Invalid JDBC property specified: " + propertyOptions);
        }
      }
    }
    return properties;
  }

  static SslMode parseSslMode(String value) {
    if (value == null) {
      return SslMode.Disable;
    }
    for (SslMode mode : SslMode.values()) {
      if (mode.name().equalsIgnoreCase(value)) {
        return mode;
      }
    }
    throw new IllegalArgumentException(
        String.format("Invalid SSL mode value specified: %s", value));
  }

  private DdlTransactionMode parseDdlTransactionMode(String value) {
    if (value == null) {
      return DdlTransactionMode.Batch;
    }
    try {
      return DdlTransactionMode.valueOf(value);
    } catch (IllegalArgumentException e) {
      // Catch and rethrow to give a better error message.
      throw new IllegalArgumentException(
          String.format("Invalid ddl-batching mode value specified: %s", value));
    }
  }

  /**
   * Takes the proxy port option result and parses it accordingly to fit port specs.
   *
   * @param commandLine The parsed options for CLI
   * @return The designated port if any, otherwise the default port.
   */
  private int buildProxyPort(CommandLine commandLine) {
    int port = Integer.parseInt(commandLine.getOptionValue(OPTION_SERVER_PORT, DEFAULT_PORT));
    if (port < MIN_PORT || port > MAX_PORT) {
      throw new IllegalArgumentException("Port must be between " + MIN_PORT + " and " + MAX_PORT);
    }
    return port;
  }

  private String buildSocketFile(CommandLine commandLine) {
    // Unix domain sockets are disabled by default on Windows.
    String directory =
        commandLine.getOptionValue(OPTION_SOCKET_DIR, isWindows() ? "" : DEFAULT_SOCKET_DIR).trim();
    if (!Strings.isNullOrEmpty(directory)) {
      if (directory.charAt(directory.length() - 1) != File.separatorChar) {
        directory += File.separatorChar;
      }
      return directory + SOCKET_FILE_NAME;
    }
    return "";
  }

  private int buildMaxBacklog(CommandLine commandLine) {
    int backlog =
        Integer.parseInt(
            commandLine
                .getOptionValue(OPTION_MAX_BACKLOG, String.valueOf(DEFAULT_MAX_BACKLOG))
                .trim());
    if (backlog <= 0) {
      throw new IllegalArgumentException("Max backlog must be greater than 0");
    }
    return backlog;
  }

  public QueryReplacementConfig getQueryReplacementConfig() {
    return queryReplacementConfig;
  }

  @VisibleForTesting
  private QueryReplacementConfig parseQueryTranslatorFile(String filePath) {
    try {
      return parse(this.getClass().getClassLoader().getResourceAsStream(filePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private QueryReplacementConfig parse(InputStream inputStream)
      throws JsonIOException, JsonSyntaxException, IOException {
    return objectMapper.readValue(inputStream, QueryReplacementConfig.class);
  }

  private String getQueryTranslatorFilePath() {
    final String commandMetadataFileName = commandLine.getOptionValue(QUERY_TRANSLATOR_FILE_PATH);
    return commandMetadataFileName == null
        ? DEFAULT_QUERY_TRANSLATOR_FILE
        : commandMetadataFileName;
  }

  /**
   * Get credential file path from either command line or application default. If neither throw
   * error.
   *
   * @return The absolute path of the credentials file.
   */
  public String buildCredentialsFile() {
    if (!commandLine.hasOption(OPTION_CREDENTIALS_FILE)) {
      try {
        // This will throw an IOException if no default credentials are available.
        tryGetDefaultCredentials();
      } catch (IOException e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.FAILED_PRECONDITION,
            "There are no credentials specified in the command line arguments for MyAdapter, "
                + "and there are no default credentials in the current runtime environment. Start MyAdapter with the -c <credentials-file.json> option "
                + "or set the GOOGLE_APPLICATION_CREDENTIALS environment variable.",
            e);
      }
    }
    return commandLine.getOptionValue(OPTION_CREDENTIALS_FILE);
  }

  @VisibleForTesting
  void tryGetDefaultCredentials() throws IOException {
    GoogleCredentials.getApplicationDefault();
  }

  /**
   * Takes user inputs and builds a JDBC connection string from them.
   *
   * @return The parsed JDBC connection string.
   */
  public String buildConnectionURL(String database) {
    Preconditions.checkNotNull(database);
    // Check if it is a full database name, or only a database ID.
    DatabaseName databaseName = getDatabaseName(database);
    String host = commandLine.getOptionValue(OPTION_SPANNER_ENDPOINT, "");
    String endpoint;
    if (host.isEmpty()) {
      endpoint = "cloudspanner:/";
    } else {
      endpoint = "cloudspanner://" + host + "/";
      logger.log(
          Level.INFO,
          () ->
              String.format(
                  "MySQL Adapter will connect to the following Cloud Spanner service endpoint: %s",
                  host));
    }

    // Note that Credentials here is the credentials file, not the actual credentials
    String url = String.format("%s%s;userAgent=%s", endpoint, databaseName, DEFAULT_USER_AGENT);

    String credentials = buildCredentialsFile();
    if (!Strings.isNullOrEmpty(credentials)) {
      url = String.format("%s;credentials=%s", url, credentials);
    }

    return url;
  }

  /** Returns the fully qualified database name based on the given database id or name. */
  public DatabaseName getDatabaseName(String database) {
    DatabaseName databaseName;
    if (DatabaseName.isParsableFrom(database)) {
      databaseName = DatabaseName.parse(database);
    } else {
      String projectId;
      if (commandLine.hasOption(OPTION_PROJECT_ID)) {
        projectId = commandLine.getOptionValue(OPTION_PROJECT_ID);
      } else {
        projectId = getDefaultProjectId();
      }
      if (Strings.isNullOrEmpty(projectId)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.FAILED_PRECONDITION,
            "The database name does not include a project ID, and there is no default project ID in the environment. "
                + "Either start MyAdapter with the -p <project-id> command line argument, set the GOOGLE_CLOUD_PROJECT environment variable, "
                + "or specify the database as a fully qualified database name in the format 'projects/my-project/instances/my-instance/database/my-database'.");
      }
      String instanceId;
      if (commandLine.hasOption(OPTION_INSTANCE_ID)) {
        instanceId = commandLine.getOptionValue(OPTION_INSTANCE_ID);
      } else {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.FAILED_PRECONDITION,
            "The database name does not include an instance ID, and there is no default instance ID in the command line arguments of PGAdapter. "
                + "Either start MyAdapter with the -i <instance-id> command line argument, or specify the database as a fully qualified database name in the format 'projects/my-project/instances/my-instance/database/my-database'.");
      }
      databaseName =
          DatabaseName.newBuilder()
              .setProject(projectId)
              .setInstance(instanceId)
              .setDatabase(database)
              .build();
    }
    return databaseName;
  }

  @VisibleForTesting
  String getDefaultProjectId() {
    return SpannerOptions.getDefaultProjectId();
  }

  /**
   * Simple setup for command line option parsing.
   *
   * @param args user's CLI args
   * @return The parsed command line options.
   */
  private CommandLine buildOptions(String[] args) {
    Options options = new Options();
    options.addOption(
        OPTION_SERVER_PORT, "server-port", true, "This proxy's port number (Default 5432).");
    options.addOption(
        null,
        OPTION_SOCKET_DIR,
        true,
        String.format(
            "This proxy's domain socket directory (Default %s). "
                + "Domain sockets are disabled by default on Windows. Set this property to a non-empty value on Windows to enable domain sockets. "
                + "Set this property to the empty string on other operating systems to disable domain sockets.",
            DEFAULT_SOCKET_DIR));
    options.addOption(
        null,
        OPTION_MAX_BACKLOG,
        true,
        String.format(
            "Maximum queue length of incoming connections. Defaults to %d.", DEFAULT_MAX_BACKLOG));
    options.addOption(
        OPTION_PROJECT_ID,
        "project",
        true,
        "The id of the GCP project wherein lives the Spanner database.");
    options.addOption(
        OPTION_INSTANCE_ID,
        "instance",
        true,
        "The id of the Spanner instance within the GCP project.");
    options.addOption(
        OPTION_DATABASE_NAME,
        "database",
        true,
        "The default Spanner database within the GCP project to use. "
            + "If specified, PGAdapter will always connect to this database. "
            + "Any database name in the connection request from the client will be ignored. "
            + "Omit this option to be able to connect to different databases using a single "
            + "PGAdapter instance.");
    options.addOption(
        OPTION_CREDENTIALS_FILE,
        "credentials-file",
        true,
        "The full path of the file location wherein lives the GCP credentials."
            + "If not specified, will try to read application default credentials.");
    options.addOption(
        OPTION_SPANNER_ENDPOINT, "spanner-endpoint", true, "The Cloud Spanner service endpoint.");
    options.addOption(OPTION_HELP, "help", false, "Print help.");
    options.addOption(
        OPTION_JDBC_PROPERTIES,
        "jdbc-properties",
        true,
        "This option specifies additional properties that will be used with the JDBC connection. "
            + "They should be in the format <key1>=<value1>;<key2>=<value2>;...");
    options.addOption(
        OPTION_DISABLE_LOCALHOST_CHECK,
        "disable-localhost-check-for-docker",
        false,
        "By default, for safety, PG Adapter only accepts connections from localhost. "
            + "When running inside docker however the docker host IP will not show as localhost. "
            + "Instead, set this flag and restrict connections from localhost using docker, e.g: "
            + "`-p 127.0.0.1:5432:5432`");
    options.addOption(
        OPTION_SERVER_VERSION,
        "server-version",
        true,
        "This option specifies what server_version PG Adapter should claim to be. If not specified "
            + " it will default to version "
            + DEFAULT_SERVER_VERSION
            + ".\nBe careful when changing this value. Unless otherwise specified, all clients and drivers that "
            + "have been tested with PGAdapter have been tested using the default value for this option. Changing "
            + "the value of this option could cause a client or driver to alter its behavior and cause unexpected "
            + "errors when used with PGAdapter.");
    options.addOption(
        OPTION_DEBUG_MODE,
        "debug-mode",
        false,
        "-- ONLY USE FOR DEBUGGING -- This option only intended for debugging. It will "
            + "instruct the server to keep track of all messages it receives.");

    CommandLineParser parser = new DefaultParser();
    HelpFormatter help = new HelpFormatter();
    help.setWidth(120);
    try {
      CommandLine commandLine = parser.parse(options, args);
      if (commandLine.hasOption(OPTION_HELP)) {
        help.printHelp(CLI_ARGS, options);
        System.exit(0);
      }
      printDeprecatedWarnings(commandLine);
      return commandLine;
    } catch (ParseException e) {
      help.printHelp(CLI_ARGS, options);
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  static void printDeprecatedWarnings(CommandLine commandLine) {}

  public boolean isDebugMode() {
    return this.debugMode;
  }

  /**
   * @return the default connection URL that is used by the server.
   * @deprecated use {@link #getDefaultConnectionUrl()}
   */
  @Deprecated
  public String getConnectionURL() {
    return this.defaultConnectionUrl;
  }

  /**
   * @return true if the server uses a default connection URL and ignores the database in a
   *     connection request
   */
  public boolean hasDefaultConnectionUrl() {
    return this.defaultConnectionUrl != null;
  }

  /** Returns the id of the default database or null if no default has been selected. */
  public DatabaseId getDefaultDatabaseId() {
    return this.hasDefaultConnectionUrl()
        ? DatabaseId.of(
            commandLine.getOptionValue(OPTION_PROJECT_ID),
            commandLine.getOptionValue(OPTION_INSTANCE_ID),
            commandLine.getOptionValue(OPTION_DATABASE_NAME))
        : null;
  }

  /** Returns true if these options contain a default instance id. */
  public boolean hasDefaultInstanceId() {
    return commandLine.hasOption(OPTION_PROJECT_ID) && commandLine.hasOption(OPTION_INSTANCE_ID);
  }

  /** Returns the id of the default instance or null if no default has been selected. */
  public InstanceId getDefaultInstanceId() {
    if (hasDefaultInstanceId()) {
      return InstanceId.of(
          commandLine.getOptionValue(OPTION_PROJECT_ID),
          commandLine.getOptionValue(OPTION_INSTANCE_ID));
    }
    return null;
  }

  /**
   * Returns the default connection URL that is used by the server. If a default connection URL has
   * been set, the database parameter in a connection request will be ignored, and the database in
   * this connection URL will be used instead.
   *
   * @return the default connection URL that is used by the server.
   */
  public String getDefaultConnectionUrl() {
    return defaultConnectionUrl;
  }

  public int getProxyPort() {
    return this.proxyPort;
  }

  public boolean isDomainSocketEnabled() {
    return !Strings.isNullOrEmpty(this.socketFile);
  }

  public String getSocketFile(int localPort) {
    return String.format(this.socketFile, localPort);
  }

  public int getMaxBacklog() {
    return this.maxBacklog;
  }

  public boolean disableLocalhostCheck() {
    return this.disableLocalhostCheck;
  }

  public Map<String, String> getPropertyMap() {
    return this.propertyMap;
  }

  public String getServerVersion() {
    return serverVersion;
  }

  public String getServerVersionNum() {
    return toServerVersionNum(this.serverVersion);
  }

  public static String toServerVersionNum(String version) {
    String[] components = version.split("\\.");
    if (components.length >= 2) {
      int major = tryParseInt(components[0]);
      int minor = tryParseInt(components[1].split("\\s+")[0]);
      if (major > -1 && minor > -1) {
        return String.valueOf(major * 10000 + minor);
      }
    }
    return version;
  }

  private static int tryParseInt(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignore) {
      return -1;
    }
  }

  /** Returns true if the OS is Windows. */
  public boolean isWindows() {
    return osName.toLowerCase().startsWith("windows");
  }
}
