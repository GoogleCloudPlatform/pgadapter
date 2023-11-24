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

package com.google.cloud.spanner.pgadapter.metadata;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.Server;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.DatabaseName;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONObject;

/** Metadata extractor for CLI. */
public class OptionsMetadata {
  /** These system properties can be used to turn on the use of virtual threads. */
  public static String USE_VIRTUAL_THREADS_SYSTEM_PROPERTY_NAME = "pgadapter.use_virtual_threads";

  public static String USE_VIRTUAL_GRPC_TRANSPORT_THREADS_SYSTEM_PROPERTY_NAME =
      "pgadapter.use_virtual_grpc_transport_threads";

  /** The environment variable used by Spanner to specify the emulator host:port. */
  private static final String SPANNER_EMULATOR_HOST_ENV_VAR = "SPANNER_EMULATOR_HOST";

  static Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofSeconds(30L);

  /**
   * Builder class for creating an instance of {@link OptionsMetadata}.
   *
   * <p>A default instance of this Builder will create a PGAdapter proxy server that:
   *
   * <ol>
   *   <li>Accepts connections to any Cloud Spanner database. The database name must be given as a
   *       fully qualified database name in the PostgreSQL connection URL.
   *   <li>Accepts connections on a dynamically assigned port. You can get the port that was
   *       assigned to the server after the server has started by calling {@link
   *       ProxyServer#getLocalPort()}.
   *   <li>Uses the default credentials in the current runtime environment.
   *   <li>Listens for Unix domain socket connections on '/tmp'.
   * </ol>
   */
  public static class Builder {
    private Map<String, String> environment = System.getenv();
    private String project;
    private String instance;
    private String database;
    private SessionPoolOptions sessionPoolOptions;
    private Integer numChannels;
    private String databaseRole;
    private DdlTransactionMode ddlTransactionMode;
    private String credentialsFile;
    private Credentials credentials;
    private boolean disableInternalRetries;
    private boolean requireAuthentication;
    private boolean enableOpenTelemetry;
    private Double openTelemetryTraceRatio;
    private boolean skipLocalhostCheck;
    private boolean useVirtualThreads;
    private boolean useVirtualGrpcTransportThreads;
    private SslMode sslMode;
    private int port;
    private String unixDomainSocketDirectory;
    private boolean autoConfigEmulator;
    private boolean debugMode;
    private String endpoint;
    private boolean usePlainText;
    private Duration startupTimeout = DEFAULT_STARTUP_TIMEOUT;

    Builder() {}

    Builder setEnvironment(Map<String, String> environment) {
      this.environment = Preconditions.checkNotNull(environment);
      return this;
    }

    /**
     * (Optional) The Google Cloud project ID that PGAdapter should connect to.
     *
     * <p>It is not required to set a project ID for PGAdapter, but you are required to use a fully
     * qualified database name in the form
     * `projects/my-project/instances/my-instance/databases/my-database` in any connection URL if
     * you do not set a project ID here.
     */
    public Builder setProject(String project) {
      this.project = project;
      return this;
    }

    /**
     * (Optional) The Google Cloud Spanner instance ID that PGAdapter should connect to.
     *
     * <p>It is not required to set an instance ID for PGAdapter, but you are required to use a
     * fully qualified database name in the form
     * `projects/my-project/instances/my-instance/databases/my-database` in any connection URL if
     * you do not set an instance ID here.
     */
    public Builder setInstance(String instance) {
      this.instance = instance;
      return this;
    }

    /**
     * (Optional) The Google Cloud Spanner database ID that PGAdapter should connect to. You can
     * only set this property if you also set the project and instance ID that PGAdapter should use.
     *
     * <p><b>Note:</b>Setting a project, instance and database ID will lock PGAdapter to only this
     * specific database. Any database information in a PostgreSQL connection URL that PGAdapter
     * receives will be ignored.
     *
     * <p>It is not required to set a database ID for PGAdapter. You must include a database name in
     * the PostgreSQL connection URL if you do not set a database name on this builder. The database
     * name in the connection URL can be:
     *
     * <ul>
     *   <li>A database ID: This is only supported if you have set a project and instance ID on this
     *       builder. The database ID must be a valid database ID on the selected Cloud Spanner
     *       instance.
     *   <li>A fully qualified database name in the form
     *       `projects/my-project/instances/my-instance/databases/my-database`. This database can be
     *       on any Cloud Spanner instance, and is not restricted to any project and instance ID
     *       that might have been set on this builder.
     * </ul>
     */
    public Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    /** (Optional) The {@link SessionPoolOptions} to use for connections to Cloud Spanner. */
    public Builder setSessionPoolOptions(SessionPoolOptions sessionPoolOptions) {
      this.sessionPoolOptions = Preconditions.checkNotNull(sessionPoolOptions);
      return this;
    }

    /** (Optional) The number of gRPC channels to use for connections to Cloud Spanner. */
    public Builder setNumChannels(int numChannels) {
      this.numChannels = numChannels;
      return this;
    }

    /** (Optional) The database role to use for connections to Cloud Spanner. */
    public Builder setDatabaseRole(String databaseRole) {
      this.databaseRole = databaseRole;
      return this;
    }

    /**
     * (Optional) Sets the default DDL transaction mode. Cloud Spanner does not support DDL
     * statements in transactions, but some tools and frameworks will assume that it does. This
     * option enables you to specify the behavior that PGAdapter should use when it encounters a DDL
     * statement in a read/write transaction.
     *
     * <p>See also <a
     * href="https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/ddl.md">DDL
     * Options</a> for more information.
     */
    public Builder setDdlTransactionMode(DdlTransactionMode ddlTransactionMode) {
      this.ddlTransactionMode = Preconditions.checkNotNull(ddlTransactionMode);
      return this;
    }

    /**
     * (Optional) The credentials file (user credentials or service account key file) that PGAdapter
     * should use to connect to Cloud Spanner.
     *
     * <p><b>Note:</b>Specifying a credentials file is only necessary if you do not want PGAdapter
     * to use the default credentials of the runtime environment.
     *
     * <p>You can only specify either a credentials file using this method or a {@link Credentials}
     * instance using {@link #setCredentials(Credentials)}.
     */
    public Builder setCredentialsFile(String credentialsFile) {
      Preconditions.checkState(
          credentials == null, "Cannot set both credentials and credentialsFile");
      this.credentialsFile = Preconditions.checkNotNull(credentialsFile);
      return this;
    }

    /**
     * (Optional) The credentials that PGAdapter should use to connect to Cloud Spanner.
     *
     * <p><b>Note:</b>Specifying a credentials file is only necessary if you do not want PGAdapter
     * to use the default credentials of the runtime environment.
     *
     * <p>You can only specify either a credentials instance using this method or a credentials file
     * using {@link #setCredentialsFile(String)}.
     */
    public Builder setCredentials(Credentials credentials) {
      Preconditions.checkState(
          this.credentialsFile == null, "Cannot set both credentials and credentialsFile");
      this.credentials = Preconditions.checkNotNull(credentials);
      return this;
    }

    /** Disables internal retries of aborted read/write transactions. */
    public Builder setDisableInternalRetries() {
      this.disableInternalRetries = true;
      return this;
    }

    /**
     * Require PostgreSQL clients that connect to PGAdapter to authenticate. The PostgreSQL client
     * should provide the serialized credentials that PGAdapter should use to connect to Cloud
     * Spanner as a password.
     *
     * <p>See <a
     * href="https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/authentication.md">authentication.md</a>
     * for more information.
     */
    public Builder setRequireAuthentication() {
      this.requireAuthentication = true;
      return this;
    }

    /** Enables OpenTelemetry tracing for PGAdapter. */
    public Builder setEnableOpenTelemetry() {
      this.enableOpenTelemetry = true;
      return this;
    }

    /** Sets the trace sampling ratio for OpenTelemetry. */
    public Builder setOpenTelemetryTraceRatio(double ratio) {
      Preconditions.checkArgument(
          ratio >= 0.0d && ratio <= 1.0d, "ration must be in the range [0.0, 1.0]");
      this.openTelemetryTraceRatio = ratio;
      return this;
    }

    /**
     * PGAdapter by default only allows connections from localhost. Call this method to disable this
     * check. You should only allow connections from private networks, unless you are also using SSL
     * and require clients to authenticate.
     */
    public Builder setDisableLocalhostCheck() {
      this.skipLocalhostCheck = true;
      return this;
    }

    /**
     * Creates a virtual thread for each connection instead of a platform thread. Enabling this
     * option only has effect if PGAdapter is running on Java 21 or higher. Enabling virtual threads
     * can reduce memory consumption for PGAdapter instances that have a large number of incoming
     * connections, and reduces the total number of platform threads that are created.
     *
     * <p>See <a href="https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html">Virtual
     * threads</a> for more information on virtual threads.
     */
    public Builder useVirtualThreads() {
      this.useVirtualThreads = true;
      return this;
    }

    /**
     * Uses virtual threads instead of platform threads for the gRPC executor pool. Enabling this
     * option only has effect if PGAdapter is running on Java 21 or higher. Enabling virtual threads
     * for gRPC can reduce memory consumption for PGAdapter instances that execute a large number of
     * parallel queries or transactions, and reduces the total number of platform threads that are
     * created.
     *
     * <p>See <a href="https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html">Virtual
     * threads</a> for more information on virtual threads.
     */
    public Builder useVirtualGrpcTransportThreads() {
      this.useVirtualGrpcTransportThreads = true;
      return this;
    }

    /**
     * PGAdapter by default does not support SSL connections. Call this method to allow or require
     * SSL connections.
     *
     * <p>See <a
     * href="https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/ssl.md">SSL
     * options</a> for more information on setting up SSL for PGAdapter.
     */
    public Builder setSslMode(SslMode sslMode) {
      this.sslMode = Preconditions.checkNotNull(sslMode);
      return this;
    }

    /**
     * (Optional) Sets the port that PGAdapter should use to listen for incoming connections. The
     * default is 0, which dynamically assigns an available port to PGAdapter. The port that was
     * assigned can be retrieved by calling {@link ProxyServer#getLocalPort()} after the server has
     * started.
     */
    public Builder setPort(int port) {
      Preconditions.checkArgument(port >= 0, "Port must be >= 0");
      this.port = port;
      return this;
    }

    /**
     * (Optional) The directory to use for Unix domain socket connections. The default is '/tmp'.
     */
    public Builder setUnixDomainSocketDirectory(String directory) {
      this.unixDomainSocketDirectory = Preconditions.checkNotNull(directory);
      return this;
    }

    /**
     * Disables the use of Unix domain sockets. This is necessary on systems that do not support
     * Unix domain sockets, or if you do not intend to use Unix domain sockets to connect to
     * PGAdapter.
     */
    public Builder disableUnixDomainSockets() {
      this.unixDomainSocketDirectory = "";
      return this;
    }

    /**
     * Instructs PGAdapter to connect to the Cloud Spanner emulator and to automatically create the
     * instance and the database in the connection request if the instance or database does not yet
     * exist on the emulator. Setting this flag takes care of everything you need to connect to the
     * emulator (except for starting the emulator), and the auto-creation of the instance and
     * database removes the need for any additional scripts that must be executed before connecting
     * to the emulator.
     */
    public Builder autoConfigureEmulator() {
      this.autoConfigEmulator = true;
      return this.setCredentials(NoCredentials.getInstance());
    }

    Builder enableDebugMode() {
      this.debugMode = true;
      return this;
    }

    Builder setEndpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    Builder setUsePlainText() {
      this.usePlainText = true;
      return this;
    }

    Builder setStartupTimeout(Duration timeout) {
      this.startupTimeout = timeout;
      return this;
    }

    public OptionsMetadata build() {
      if (Strings.isNullOrEmpty(project) && !Strings.isNullOrEmpty(instance)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "You must also specify a project if you specify an instance that PGAdapter should connect to.");
      }
      if (Strings.isNullOrEmpty(instance) && !Strings.isNullOrEmpty(database)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "You must also specify an instance if you specify a database that PGAdapter should connect to.");
      }
      if (!(Strings.isNullOrEmpty(credentialsFile)
              && (credentials == null || NoCredentials.getInstance().equals(credentials)))
          && requireAuthentication) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Cannot set both a credentialsFile and requireAuthentication. If you set requireAuthentication, the PostgreSQL client that connects to PGAdapter must supply the credentials in the password message.");
      }
      return new OptionsMetadata(this);
    }

    private String[] toCommandLineArguments() {
      ImmutableList.Builder<String> args = ImmutableList.builder();
      if (!Strings.isNullOrEmpty(project)) {
        addOption(args, OPTION_PROJECT_ID, project);
      }
      if (!Strings.isNullOrEmpty(instance)) {
        addOption(args, OPTION_INSTANCE_ID, instance);
      }
      if (!Strings.isNullOrEmpty(database)) {
        addOption(args, OPTION_DATABASE_NAME, database);
      }
      if (!Strings.isNullOrEmpty(credentialsFile)) {
        addOption(args, OPTION_CREDENTIALS_FILE, credentialsFile);
      }
      if (requireAuthentication) {
        addOption(args, OPTION_AUTHENTICATE);
      }
      if (enableOpenTelemetry) {
        addOption(args, OPTION_ENABLE_OPEN_TELEMETRY);
      }
      if (openTelemetryTraceRatio != null) {
        addLongOption(
            args, OPTION_OPEN_TELEMETRY_TRACE_RATIO, String.valueOf(openTelemetryTraceRatio));
      }
      if (skipLocalhostCheck) {
        addOption(args, OPTION_DISABLE_LOCALHOST_CHECK);
      }
      if (sslMode != null) {
        addLongOption(args, OPTION_SSL, sslMode.name());
      }
      if (ddlTransactionMode != null) {
        addLongOption(args, OPTION_DDL_TRANSACTION_MODE, ddlTransactionMode.name());
      }
      if (unixDomainSocketDirectory != null) {
        addLongOption(args, OPTION_SOCKET_DIR, unixDomainSocketDirectory);
      }
      if (endpoint != null) {
        addOption(args, OPTION_SPANNER_ENDPOINT, endpoint);
      }
      if (usePlainText
          || numChannels != null
          || databaseRole != null
          || autoConfigEmulator
<<<<<<< HEAD
          || useVirtualThreads
          || useVirtualGrpcTransportThreads) {
=======
          || disableInternalRetries) {
>>>>>>> 4ebaf915 (feat: add option for disabling retries)
        StringBuilder jdbcOptionBuilder = new StringBuilder();
        if (usePlainText) {
          jdbcOptionBuilder.append("usePlainText=true;");
        }
        if (numChannels != null) {
          jdbcOptionBuilder.append("numChannels=").append(numChannels).append(";");
        }
        if (databaseRole != null) {
          jdbcOptionBuilder.append("databaseRole=").append(databaseRole).append(";");
        }
        if (autoConfigEmulator) {
          jdbcOptionBuilder.append("autoConfigEmulator=true;");
        }
<<<<<<< HEAD
        if (useVirtualThreads) {
          jdbcOptionBuilder
              .append(ConnectionOptions.USE_VIRTUAL_THREADS_PROPERTY_NAME)
              .append("=true;");
        }
        if (useVirtualGrpcTransportThreads) {
          jdbcOptionBuilder
              .append(ConnectionOptions.USE_VIRTUAL_GRPC_TRANSPORT_THREADS_PROPERTY_NAME)
              .append("=true;");
=======
        if (disableInternalRetries) {
          jdbcOptionBuilder.append("retryAbortsInternally=false;");
>>>>>>> 4ebaf915 (feat: add option for disabling retries)
        }
        addOption(args, OPTION_JDBC_PROPERTIES, jdbcOptionBuilder.toString());
      }
      if (debugMode) {
        addOption(args, OPTION_INTERNAL_DEBUG_MODE);
        addOption(args, OPTION_SKIP_INTERNAL_DEBUG_MODE_WARNING);
      }
      addOption(args, OPTION_SERVER_PORT, String.valueOf(port));
      return args.build().toArray(new String[0]);
    }
  }

  private static void addOption(ImmutableList.Builder<String> builder, String option) {
    builder.add("-" + option);
  }

  private static void addOption(
      ImmutableList.Builder<String> builder, String option, String value) {
    builder.add("-" + option, value);
  }

  private static void addLongOption(
      ImmutableList.Builder<String> builder, String option, String value) {
    builder.add("-" + option + "=" + value);
  }

  /** Creates a {@link Builder} for an {@link OptionsMetadata} instance. */
  public static Builder newBuilder() {
    return new Builder();
  }

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

  private static final Logger logger = Logger.getLogger(OptionsMetadata.class.getName());
  public static final String DEFAULT_SERVER_VERSION = "14.1";
  private static final String DEFAULT_USER_AGENT = "pg-adapter";

  private static final String OPTION_SERVER_PORT = "s";
  private static final String OPTION_SOCKET_DIR = "dir";
  private static final String OPTION_MAX_BACKLOG = "max_backlog";
  private static final String OPTION_PROJECT_ID = "p";
  private static final String OPTION_INSTANCE_ID = "i";
  private static final String OPTION_DATABASE_NAME = "d";
  private static final String OPTION_CREDENTIALS_FILE = "c";
  private static final String OPTION_BINARY_FORMAT = "b";
  private static final String OPTION_AUTHENTICATE = "a";
  private static final String OPTION_ENABLE_OPEN_TELEMETRY = "enable_otel";
  private static final String OPTION_OPEN_TELEMETRY_TRACE_RATIO = "otel_trace_ratio";
  private static final String OPTION_SSL = "ssl";
  private static final String OPTION_DISABLE_AUTO_DETECT_CLIENT = "disable_auto_detect_client";
  private static final String OPTION_DISABLE_DEFAULT_LOCAL_STATEMENTS =
      "disable_default_local_statements";
  private static final String OPTION_DISABLE_PG_CATALOG_REPLACEMENTS =
      "disable_pg_catalog_replacements";
  private static final String OPTION_PSQL_MODE = "q";
  private static final String OPTION_DDL_TRANSACTION_MODE = "ddl";
  private static final String OPTION_JDBC_MODE = "jdbc";
  private static final String OPTION_COMMAND_METADATA_FILE = "j";
  private static final String OPTION_DISABLE_LOCALHOST_CHECK = "x";
  private static final String CLI_ARGS =
      "pgadapter -p <project> -i <instance> -d <database> -c <credentials_file>";
  private static final String OPTION_HELP = "h";
  private static final String DEFAULT_PORT = "5432";
  private static final int MIN_PORT = 0, MAX_PORT = 65535;
  private static final String DEFAULT_SOCKET_DIR = "/tmp";
  private static final String SOCKET_FILE_NAME = ".s.PGSQL.%d";
  private static final int DEFAULT_MAX_BACKLOG = 1000;
  /*Note: this is a private preview feature, not meant for GA version. */
  private static final String OPTION_SPANNER_ENDPOINT = "e";
  private static final String OPTION_JDBC_PROPERTIES = "r";
  private static final String OPTION_SERVER_VERSION = "v";
  private static final String OPTION_INTERNAL_DEBUG_MODE = "internal_debug";
  private static final String OPTION_SKIP_INTERNAL_DEBUG_MODE_WARNING =
      "skip_internal_debug_warning";
  private static final String OPTION_DEBUG_MODE = "debug";

  private final Map<String, String> environment;
  private final String osName;
  private final CommandLine commandLine;
  private final Credentials credentials;
  private final SessionPoolOptions sessionPoolOptions;
  private final CommandMetadataParser commandMetadataParser;
  private final String defaultConnectionUrl;
  private final int proxyPort;
  private final String socketFile;
  private final int maxBacklog;
  private final TextFormat textFormat;
  private final boolean binaryFormat;
  private final boolean authenticate;
  private final boolean enableOpenTelemetry;
  private final Double openTelemetryTraceRatio;
  private final SslMode sslMode;
  private final boolean disableAutoDetectClient;
  private final boolean disableDefaultLocalStatements;
  private final boolean disablePgCatalogReplacements;
  private final boolean requiresMatcher;
  private final DdlTransactionMode ddlTransactionMode;
  private final boolean replaceJdbcMetadataQueries;
  private final boolean disableLocalhostCheck;
  private final JSONObject commandMetadataJSON;
  private final Map<String, String> propertyMap;
  private final String serverVersion;
  private final boolean debugMode;
  private final Duration startupTimeout;

  /**
   * Creates a new instance of {@link OptionsMetadata} from the given arguments.
   *
   * <p>It is recommended to use {@link #newBuilder()} to create an options {@link Builder} instead
   * of calling this method directly.
   */
  public OptionsMetadata(String[] args) {
    this(System.getenv(), System.getProperty("os.name", ""), DEFAULT_STARTUP_TIMEOUT, args);
  }

  private OptionsMetadata(Builder builder) {
    this(
        builder.environment,
        System.getProperty("os.name", ""),
        builder.toCommandLineArguments(),
        builder.credentials,
        builder.sessionPoolOptions,
        builder.startupTimeout);
  }

  OptionsMetadata(
      Map<String, String> environment, String osName, Duration startupTimeout, String[] args) {
    this(environment, osName, args, null, null, startupTimeout);
  }

  OptionsMetadata(
      Map<String, String> environment,
      String osName,
      String[] args,
      @Nullable Credentials credentials,
      @Nullable SessionPoolOptions sessionPoolOptions,
      Duration startupTimeout) {
    this.environment = Preconditions.checkNotNull(environment);
    this.osName = osName;
    this.commandLine = buildOptions(args);
    this.credentials = credentials;
    this.sessionPoolOptions = sessionPoolOptions;
    this.commandMetadataParser = new CommandMetadataParser();
    if (this.commandLine.hasOption(OPTION_AUTHENTICATE)
        && this.commandLine.hasOption(OPTION_CREDENTIALS_FILE)
        && !Strings.isNullOrEmpty(this.commandLine.getOptionValue(OPTION_CREDENTIALS_FILE))) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Cannot enable authentication on PGAdapter when a credentials file is used. "
              + "Use either -a to require the client to supply the credentials as a username/password combination, "
              + "OR use -c to set the credentials in PGAdapter and use these credentials for all connections.");
    }
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
    this.textFormat = TextFormat.POSTGRESQL;
    this.binaryFormat = commandLine.hasOption(OPTION_BINARY_FORMAT);
    this.authenticate = commandLine.hasOption(OPTION_AUTHENTICATE);
    this.enableOpenTelemetry = commandLine.hasOption(OPTION_ENABLE_OPEN_TELEMETRY);
    this.openTelemetryTraceRatio =
        parseOpenTelemetryTraceRatio(commandLine.getOptionValue(OPTION_OPEN_TELEMETRY_TRACE_RATIO));
    this.sslMode = parseSslMode(commandLine.getOptionValue(OPTION_SSL));
    this.disableAutoDetectClient = commandLine.hasOption(OPTION_DISABLE_AUTO_DETECT_CLIENT);
    this.disableDefaultLocalStatements =
        commandLine.hasOption(OPTION_DISABLE_DEFAULT_LOCAL_STATEMENTS);
    this.disablePgCatalogReplacements =
        commandLine.hasOption(OPTION_DISABLE_PG_CATALOG_REPLACEMENTS);
    this.requiresMatcher =
        commandLine.hasOption(OPTION_PSQL_MODE)
            || commandLine.hasOption(OPTION_COMMAND_METADATA_FILE);
    this.ddlTransactionMode =
        parseDdlTransactionMode(commandLine.getOptionValue(OPTION_DDL_TRANSACTION_MODE));
    this.replaceJdbcMetadataQueries = commandLine.hasOption(OPTION_JDBC_MODE);
    this.commandMetadataJSON = buildCommandMetadataJSON(commandLine);
    this.propertyMap = parseProperties(commandLine.getOptionValue(OPTION_JDBC_PROPERTIES, ""));
    this.disableLocalhostCheck = commandLine.hasOption(OPTION_DISABLE_LOCALHOST_CHECK);
    this.serverVersion = commandLine.getOptionValue(OPTION_SERVER_VERSION, DEFAULT_SERVER_VERSION);
    this.debugMode = commandLine.hasOption(OPTION_INTERNAL_DEBUG_MODE);
    this.startupTimeout = startupTimeout;
  }

  /**
   * @deprecated Use {@link #newBuilder()} to create an options builder, and then call {@link
   *     Builder#build()} instead of using this constructor.
   */
  @Deprecated
  public OptionsMetadata(
      String defaultConnectionUrl,
      int proxyPort,
      TextFormat textFormat,
      boolean forceBinary,
      boolean authenticate,
      boolean requiresMatcher,
      boolean replaceJdbcMetadataQueries,
      JSONObject commandMetadata) {
    this(
        System.getenv(),
        System.getProperty("os.name", ""),
        defaultConnectionUrl,
        proxyPort,
        textFormat,
        forceBinary,
        authenticate,
        requiresMatcher,
        replaceJdbcMetadataQueries,
        commandMetadata);
  }

  @VisibleForTesting
  OptionsMetadata(
      Map<String, String> environment,
      String osName,
      String defaultConnectionUrl,
      int proxyPort,
      TextFormat textFormat,
      boolean forceBinary,
      boolean authenticate,
      boolean requiresMatcher,
      boolean replaceJdbcMetadataQueries,
      JSONObject commandMetadata) {
    this.environment = Preconditions.checkNotNull(environment);
    this.osName = osName;
    this.commandLine = null;
    this.credentials = null;
    this.sessionPoolOptions = null;
    this.commandMetadataParser = new CommandMetadataParser();
    this.defaultConnectionUrl =
        defaultConnectionUrl.startsWith("jdbc:")
            ? defaultConnectionUrl.substring("jdbc:".length())
            : defaultConnectionUrl;
    this.proxyPort = proxyPort;
    this.socketFile = isWindows() ? "" : DEFAULT_SOCKET_DIR + File.separatorChar + SOCKET_FILE_NAME;
    this.maxBacklog = DEFAULT_MAX_BACKLOG;
    this.textFormat = textFormat;
    this.binaryFormat = forceBinary;
    this.authenticate = authenticate;
    this.enableOpenTelemetry = false;
    this.openTelemetryTraceRatio = null;
    this.sslMode = SslMode.Disable;
    this.disableAutoDetectClient = false;
    this.disableDefaultLocalStatements = false;
    this.disablePgCatalogReplacements = false;
    this.requiresMatcher = requiresMatcher;
    this.ddlTransactionMode = DdlTransactionMode.AutocommitImplicitTransaction;
    this.replaceJdbcMetadataQueries = replaceJdbcMetadataQueries;
    this.commandMetadataJSON = commandMetadata;
    this.propertyMap = new HashMap<>();
    this.disableLocalhostCheck = false;
    this.serverVersion = DEFAULT_SERVER_VERSION;
    this.debugMode = false;
    this.startupTimeout = DEFAULT_STARTUP_TIMEOUT;
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
    // Set virtual thread properties from system properties if:
    // 1. The virtual thread property has not been set in the command line arguments.
    // 2. The corresponding system property has been set.
    if (properties.get(ConnectionOptions.USE_VIRTUAL_THREADS_PROPERTY_NAME) == null
        && Boolean.parseBoolean(System.getProperty(USE_VIRTUAL_THREADS_SYSTEM_PROPERTY_NAME))) {
      properties.put(ConnectionOptions.USE_VIRTUAL_THREADS_PROPERTY_NAME, "true");
    }
    if (properties.get(ConnectionOptions.USE_VIRTUAL_GRPC_TRANSPORT_THREADS_PROPERTY_NAME) == null
        && Boolean.parseBoolean(
            System.getProperty(USE_VIRTUAL_GRPC_TRANSPORT_THREADS_SYSTEM_PROPERTY_NAME))) {
      properties.put(ConnectionOptions.USE_VIRTUAL_GRPC_TRANSPORT_THREADS_PROPERTY_NAME, "true");
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

  private Double parseOpenTelemetryTraceRatio(String value) {
    if (value == null) {
      return null;
    }
    try {
      double ratio = Double.parseDouble(value);
      if (ratio < 0.0d || ratio > 1.0d) {
        throw new IllegalArgumentException(
            String.format(
                "OpenTelemetry trace ratio must be in the range [0.0, 1.0]. "
                    + "Specified value is invalid: %s",
                value));
      }
      return ratio;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid OpenTelemetry trace ratio: %s", value));
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

  /**
   * Returns the {@link Credentials} instance that has been set for this {@link OptionsMetadata}.
   * This overrides both any credentials file and any default credentials in the current runtime
   * environment.
   */
  @Nullable
  public Credentials getCredentials() {
    return this.credentials;
  }

  /** Returns the {@link SessionPoolOptions} that has been set for this {@link OptionsMetadata}. */
  @Nullable
  public SessionPoolOptions getSessionPoolOptions() {
    return this.sessionPoolOptions;
  }

  /**
   * Get credential file path from either command line or application default. If neither are set,
   * then throw an error.
   *
   * @return The absolute path of the credentials file.
   */
  public String buildCredentialsFile() {
    // Skip if a com.google.auth.Credentials instance has been set.
    if (credentials != null) {
      return null;
    }
    if (!commandLine.hasOption(OPTION_CREDENTIALS_FILE)) {
      try {
        // This will throw an IOException if no default credentials are available.
        tryGetDefaultCredentials();
      } catch (IOException e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.FAILED_PRECONDITION,
            "There are no credentials specified in the command line arguments for PGAdapter, "
                + "and there are no default credentials in the current runtime environment. Start PGAdapter with the -c <credentials-file.json> option "
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
    boolean usePlainText = false;
    if (Strings.isNullOrEmpty(host)
        && !Strings.isNullOrEmpty(environment.get(SPANNER_EMULATOR_HOST_ENV_VAR))) {
      host = environment.get(SPANNER_EMULATOR_HOST_ENV_VAR);
      usePlainText = true;
    }
    String endpoint;
    if (host.isEmpty()) {
      endpoint = "cloudspanner:/";
    } else {
      endpoint = "cloudspanner://" + host + "/";
      String finalHost = host;
      logger.log(
          Level.INFO,
          () ->
              String.format(
                  "PG Adapter will connect to the following Cloud Spanner service endpoint: %s",
                  finalHost));
    }

    // Note that Credentials here is the credentials file, not the actual credentials
    String url = String.format("%s%s;userAgent=%s", endpoint, databaseName, DEFAULT_USER_AGENT);

    if (!shouldAuthenticate()
        && Strings.isNullOrEmpty(environment.get(SPANNER_EMULATOR_HOST_ENV_VAR))) {
      String credentials = buildCredentialsFile();
      if (!Strings.isNullOrEmpty(credentials)) {
        url = String.format("%s;credentials=%s", url, credentials);
      }
    }
    if (usePlainText) {
      url += ";usePlainText=true";
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
                + "Either start PGAdapter with the -p <project-id> command line argument, set the GOOGLE_CLOUD_PROJECT environment variable, "
                + "or specify the database as a fully qualified database name in the format 'projects/my-project/instances/my-instance/database/my-database'.");
      }
      String instanceId;
      if (commandLine.hasOption(OPTION_INSTANCE_ID)) {
        instanceId = commandLine.getOptionValue(OPTION_INSTANCE_ID);
      } else {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.FAILED_PRECONDITION,
            "The database name does not include an instance ID, and there is no default instance ID in the command line arguments of PGAdapter. "
                + "Either start PGAdapter with the -i <instance-id> command line argument, or specify the database as a fully qualified database "
                + "name in the format 'projects/my-project/instances/my-instance/databases/my-database'.");
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
   * Takes the content of the specified (or default) command file and parses it into JSON format. If
   * finding the file fails in any-way, print an error and keep going with an empty spec. Custom
   * metadata json file is allowed without PSQL mode.
   *
   * @param commandLine The parsed options for CLI
   * @return The JSON object corresponding to the string contained within the specified (or default)
   *     command file.
   */
  private JSONObject buildCommandMetadataJSON(CommandLine commandLine) {
    if (commandLine.hasOption(OPTION_COMMAND_METADATA_FILE)
        && commandLine.hasOption(OPTION_PSQL_MODE)) {
      throw new IllegalArgumentException(
          "PSQL Mode shouldn't be toggled (-q) together with the custom command metadata file"
              + " (-j).");
    }

    final String commandMetadataFileName = commandLine.getOptionValue(OPTION_COMMAND_METADATA_FILE);
    try {
      if (commandMetadataFileName != null) {
        return commandMetadataParser.parse(commandMetadataFileName);
      } else {
        return commandMetadataParser.defaultCommands();
      }
    } catch (IOException e) {
      System.err.printf(
          "Specified command metadata file %s not found! Ignoring commands metadata file.%n",
          commandMetadataFileName);

      try {
        return commandMetadataParser.emptyCommands();
      } catch (IOException | org.json.simple.parser.ParseException ex) {
        throw new IllegalArgumentException(
            "Something went wrong! Processing empty JSON file failed!", ex);
      }
    } catch (org.json.simple.parser.ParseException e) {
      throw new IllegalArgumentException(
          "Unable to process provided JSON file: " + commandMetadataFileName, e);
    }
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
    options.addOption(
        OPTION_AUTHENTICATE,
        "authenticate",
        false,
        "Whether you wish the proxy to perform an authentication step.");
    options.addOption(null, OPTION_ENABLE_OPEN_TELEMETRY, false, "Enable OpenTelemetry tracing.");
    options.addOption(
        null,
        OPTION_OPEN_TELEMETRY_TRACE_RATIO,
        true,
        "OpenTelemetry trace sampling ration. Value must be in the range [0.0, 1.0].");
    options.addOption(
        OPTION_SSL,
        "sslmode",
        true,
        "Enable SSL connections for this server. SSL only works if you "
            + "have also configured a keystore for the Java Virtual Machine.\n"
            + "See https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/ssl.md "
            + "for more information.");
    options.addOption(
        null,
        OPTION_DISABLE_AUTO_DETECT_CLIENT,
        false,
        "This option turns off automatic detection of well-known clients. "
            + "Use this option if you do not want PGAdapter to automatically apply query "
            + "replacements based on the client that is connected to PGAdapter.");
    options.addOption(
        null,
        OPTION_DISABLE_DEFAULT_LOCAL_STATEMENTS,
        false,
        "This option turns off translations for commonly used statements that are "
            + "currently not supported by Cloud Spanner (e.g. `select current_schema`). "
            + "Use this option if you do not want PGAdapter to automatically apply query "
            + "replacements for these statements.");
    options.addOption(
        null,
        OPTION_DISABLE_PG_CATALOG_REPLACEMENTS,
        false,
        "This option turns off automatic replacement of pg_catalog tables with "
            + "common table expressions for pg_catalog tables that are not yet supported by "
            + "Cloud Spanner. Use this option if you do not want PGAdapter to automatically apply "
            + "replacements for pg_catalog tables.");
    options.addOption(
        OPTION_PSQL_MODE,
        "psql-mode",
        false,
        "DEPRECATED: PGAdapter will automatically detect connections from psql."
            + " This option turns on PSQL mode. This mode allows better compatibility to PSQL, with an"
            + " added performance cost. PSQL mode is implemented using predefined dynamic matchers"
            + " and as such cannot be used with the option -j. This mode should not be used for"
            + " production, and we do not guarantee its functionality beyond the basics.");
    options.addOption(
        OPTION_DDL_TRANSACTION_MODE,
        "ddl-transaction-mode",
        true,
        String.format(
            "Sets the way PGAdapter should handle DDL statements in implicit and explicit "
                + "transactions. Cloud Spanner does not support DDL transactions. The possible modes "
                + "are:\n"
                + "%s: Only allows single DDL statements outside implicit and explicit transactions.\n"
                + "%s: Allows batches that contain only DDL statements. "
                + "Does not allow mixed batches of DDL and other statements, or DDL statements in transactions.\n"
                + "%s: Allows mixed batches of DDL and other statements. "
                + "Automatically commits the implicit transaction when a DDL statement is encountered in a batch. "
                + "DDL statements in explicit transactions are not allowed.\n"
                + "%s: Allows mixed batches of DDL and other statements. "
                + "Automatically commits the current transaction when a DDL statement is encountered.",
            DdlTransactionMode.Single,
            DdlTransactionMode.Batch,
            DdlTransactionMode.AutocommitImplicitTransaction,
            DdlTransactionMode.AutocommitExplicitTransaction));
    options.addOption(
        OPTION_JDBC_MODE,
        "jdbc-mode",
        false,
        "DEPRECATED: PGAdapter will automatically detect connections from JDBC. "
            + "This option turns on JDBC mode. This mode allows better compatibility with the "
            + "PostgreSQL JDBC driver. It will automatically inspect incoming queries to look for "
            + "known JDBC metadata queries, and replace these with queries that are compatible with "
            + "Cloud Spanner. JDBC mode is implemented using predefined fixed matchers and should "
            + "not be used in combination with options -q (psql mode) or -j (custom matchers). It "
            + "should be enabled if you intend to connect to PGAdapter using the PostgreSQL JDBC "
            + "driver.");
    options.addOption(
        OPTION_COMMAND_METADATA_FILE,
        "options-metadata",
        true,
        "This option specifies the full path of the file containing the metadata specifications for"
            + " custom dynamic matchers. Each item in this matcher will create a runtime-generated"
            + " command which will translate incoming commands into whatever back-end SQL is"
            + " desired. This feature allows re-writing queries that are outside the user's control"
            + " (e.g: issued by client libraries and / or tools) and are not currently supported by"
            + " the backend with equivalent supported queries, but comes at a performance cost."
            + " This option cannot be used with option -q.");
    options.addOption(OPTION_HELP, "help", false, "Print help.");
    options.addOption(
        OPTION_BINARY_FORMAT,
        "force-binary-format",
        false,
        "DEPRECATED: This option violates the PostgreSQL wire-protocol.\n"
            + "Force the server to send data back in binary PostgreSQL format when no specific "
            + "format has been requested. The PostgreSQL wire protocol specifies that the server "
            + "should send data in text format in those cases. This setting overrides this default "
            + "and should be used with caution, for example for testing purposes, as clients might "
            + "not accept this behavior. This setting only affects query results in extended query "
            + "mode. Queries in simple query mode will always return results in text format. If "
            + "you do not know what extended query mode and simple query mode is, then you should "
            + "probably not be using this setting.");
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
        OPTION_INTERNAL_DEBUG_MODE,
        "internal-debug-mode",
        false,
        "-- ONLY USE FOR INTERNAL DEBUGGING -- This option only intended for INTERNAL debugging. It will "
            + "instruct the server to keep track of all messages it receives.\n"
            + "You should not enable this option unless you want to debug PGAdapter, for example if"
            + "you are developing a new feature for PGAdapter that you want to test.\n"
            + "This option will NOT enable any additional LOGGING.\n"
            + "You should not enable this option if you are just trying out PGAdapter.");
    options.addOption(
        OPTION_SKIP_INTERNAL_DEBUG_MODE_WARNING,
        "skip-internal-debug-mode-warning",
        false,
        "Disables the warning that is printed when internal debug mode is enabled.");
    options.addOption(
        OPTION_DEBUG_MODE,
        "debug-mode",
        false,
        "-- DEPRECATED -- This option currently does not have any effect.\n"
            + "This option used to enable the internal debug mode for PGAdapter.\n"
            + "Use the option -internal_debug to enable internal debug mode.\n"
            + "You most probably do not want to turn internal debug mode on. It is only intended for\n"
            + "internal test cases in PGAdapter that need to verify that it receives the correct \n"
            + "wire-protocol messages.");
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

  static void printDeprecatedWarnings(CommandLine commandLine) {
    if (commandLine.hasOption(OPTION_DEBUG_MODE)) {
      System.out.println(
          "\n -- DEPRECATED -- \n"
              + "Debug mode (-debug) has been deprecated and currently has no effect.\n"
              + "This option was sometimes used by accident by users who thought it would enable\n"
              + "additional logging. The option is only intended for internal testing purposes and should\n"
              + "only be used for tests that need to verify that PGAdapter receives the correct wire-protocol\n"
              + "messages. Do not enable this option if you are just trying out PGAdapter.\n");
    }
    if (!commandLine.hasOption(OPTION_SKIP_INTERNAL_DEBUG_MODE_WARNING)
        && commandLine.hasOption(OPTION_INTERNAL_DEBUG_MODE)) {
      System.out.println(
          "\n -- WARNING -- \n"
              + "Internal debug mode is enabled.\n"
              + "This mode should only be enabled for internal test cases.\n"
              + "Do not enable this mode for trying out PGAdapter with an application or driver.\n");
    }
    if (commandLine.hasOption(OPTION_BINARY_FORMAT)) {
      System.out.println(
          "Forcing the server to return results using the binary format is a violation "
              + "of the PostgreSQL wire-protocol. Using this option can cause unexpected errors.\nIt is "
              + "recommended not to use the -b option.");
    }
    if (!commandLine.hasOption(OPTION_DISABLE_AUTO_DETECT_CLIENT)) {
      if (commandLine.hasOption(OPTION_PSQL_MODE)) {
        System.out.printf(
            "It is no longer necessary to add psql mode (-%s) to the command line arguments.\n"
                + "PGAdapter now automatically recognizes connections from psql.\n",
            OPTION_PSQL_MODE);
      }
      if (commandLine.hasOption(OPTION_JDBC_MODE)) {
        System.out.printf(
            "It is no longer necessary to add JDBC mode (-%s) to the command line arguments.\n"
                + "PGAdapter now automatically recognizes connections from the PostgreSQL JDBC driver.\n",
            OPTION_JDBC_MODE);
      }
    }
  }

  public DdlTransactionMode getDdlTransactionMode() {
    return this.ddlTransactionMode;
  }

  public boolean isBinaryFormat() {
    return this.binaryFormat;
  }

  public boolean isDebugMode() {
    return this.debugMode;
  }

  public boolean isUseVirtualThreads() {
    return Boolean.parseBoolean(
        getPropertyMap().get(ConnectionOptions.USE_VIRTUAL_THREADS_PROPERTY_NAME));
  }

  public boolean isUseGrpcTransportVirtualThreads() {
    return Boolean.parseBoolean(
        getPropertyMap().get(ConnectionOptions.USE_VIRTUAL_GRPC_TRANSPORT_THREADS_PROPERTY_NAME));
  }

  public Duration getStartupTimeout() {
    return this.startupTimeout;
  }

  public JSONObject getCommandMetadataJSON() {
    return this.commandMetadataJSON;
  }

  /**
   * @deprecated use {@link #getDefaultConnectionUrl()}
   * @return the default connection URL that is used by the server.
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

  public String getTelemetryProjectId() {
    // TODO: Add separate command line argument for telemetry project id.
    if (commandLine.hasOption(OPTION_PROJECT_ID)) {
      return commandLine.getOptionValue(OPTION_PROJECT_ID);
    }
    return null;
  }

  public Credentials getTelemetryCredentials() throws IOException {
    // TODO: Add separate command line argument for telemetry credentials.
    if (credentials != null) {
      return credentials;
    }
    if (commandLine.hasOption(OPTION_CREDENTIALS_FILE)) {
      return GoogleCredentials.fromStream(
          Files.newInputStream(Paths.get(commandLine.getOptionValue(OPTION_CREDENTIALS_FILE))));
    }
    return null;
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

  public TextFormat getTextFormat() {
    return this.textFormat;
  }

  public boolean shouldAuthenticate() {
    return this.authenticate;
  }

  public boolean isEnableOpenTelemetry() {
    return this.enableOpenTelemetry;
  }

  public Double getOpenTelemetryTraceRatio() {
    return this.openTelemetryTraceRatio;
  }

  public SslMode getSslMode() {
    return this.sslMode;
  }

  public boolean shouldAutoDetectClient() {
    return !this.disableAutoDetectClient;
  }

  public boolean useDefaultLocalStatements() {
    return !this.disableDefaultLocalStatements;
  }

  public boolean replacePgCatalogTables() {
    return !this.disablePgCatalogReplacements;
  }

  public boolean requiresMatcher() {
    return this.requiresMatcher;
  }

  public boolean isReplaceJdbcMetadataQueries() {
    return this.replaceJdbcMetadataQueries;
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

  /**
   * The PostgreSQL wire protocol can send data in both binary and text format. When using text
   * format, the {@link Server} will normally send output back to the client using a format
   * understood by PostgreSQL clients. If you are using the server with a text-only client that does
   * not try to interpret the data that is returned by the server, such as for example psql, then it
   * is advisable to use Cloud Spanner formatting. The server will then return all data in a format
   * understood by Cloud Spanner.
   *
   * <p>The default format used by the server is {@link TextFormat}.
   */
  public enum TextFormat {
    /**
     * The default format. Data is returned to the client in a format that PostgreSQL clients should
     * be able to understand and stringParse. Use this format if you are using the {@link Server}
     * with a client that tries to interpret the data that is returned by the server, such as for
     * example the PostgreSQL JDBC driver.
     */
    POSTGRESQL,
    /**
     * Data is returned to the client in Cloud Spanner format. Use this format if you are using the
     * server with a text-only client, such as psql, that does not try to interpret and stringParse
     * the data that is returned.
     */
    SPANNER
  }
}
