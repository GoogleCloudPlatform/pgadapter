// Copyright 2021 Google LLC
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

import static org.junit.Assert.assertEquals;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.ConnectionOptions.ExternalChannelProvider;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.opentelemetry.api.OpenTelemetry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Test wrapper used for integration tests. */
public class PgAdapterTestEnv {
  private static final int PROTOCOL_MESSAGE_SIZE = 8;
  private static final int INIT_PROTOCOL = 80877103;
  private static final int OPTIONS_PROTOCOL = 196608;
  private static final String CHANNEL_PROVIDER_PROPERTY = "CHANNEL_PROVIDER";

  // GCP credentials file should be set through the 'GOOGLE_APPLICATION_CREDENTIALS' environment
  // variable.
  public static final String GCP_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

  // Spanner host URL should be set through this system property. The default is the default Spanner
  // host URL.
  public static final String TEST_SPANNER_URL_PROPERTY = "PG_ADAPTER_HOST";

  // ProjectId should be set through this system property.
  public static final String TEST_PROJECT_PROPERTY = "PG_ADAPTER_PROJECT";

  // InstanceId should be set through this system property.
  public static final String TEST_INSTANCE_PROPERTY = "PG_ADAPTER_INSTANCE";

  // DatabaseId should be set through this system property.
  public static final String TEST_DATABASE_PROPERTY = "PG_ADAPTER_DATABASE";

  // PgAdapter host address (when using an external PGAdapter instance).
  public static final String PG_ADAPTER_ADDRESS = System.getProperty("PG_ADAPTER_ADDRESS", null);

  // PgAdapter port should be set through this system property.
  public static final String PG_ADAPTER_PORT = "PG_ADAPTER_PORT";

  // Environment variable that can be used to force the test env to assume that the test database
  // already exists. This can be used to speed up local testing by manually creating the test
  // database and re-run the tests multiple times against the same database without the need to
  // recreate it for every test run.
  public static final String USE_EXISTING_DB = "PG_ADAPTER_USE_EXISTING_DB";

  // Default fallback project Id will be used if one isn't set via the system property.
  private static final String DEFAULT_PROJECT_ID = "span-cloud-testing";

  // Default instance id.
  private static final String DEFAULT_INSTANCE_ID = "spanner-testing";

  // Default database id.
  private static final String DEFAULT_DATABASE_ID = "pgtest-db";

  // Default Spanner url. Null indicates that the default URL should be used.
  static final String DEFAULT_SPANNER_URL = null;

  public static final ImmutableList<String> DEFAULT_DATA_MODEL =
      ImmutableList.of(
          "create table numbers (num int not null primary key, name varchar(100))",
          "create table all_types ("
              + "col_bigint bigint not null primary key, "
              + "col_bool bool, "
              + "col_bytea bytea, "
              + "col_float8 float8, "
              + "col_int int, "
              + "col_numeric numeric, "
              + "col_timestamptz timestamptz, "
              + "col_date date, "
              + "col_varchar varchar(100), "
              + "col_jsonb jsonb, "
              + "col_array_bigint bigint[], "
              + "col_array_bool bool[], "
              + "col_array_bytea bytea[], "
              + "col_array_float8 float8[], "
              + "col_array_int int[], "
              + "col_array_numeric numeric[], "
              + "col_array_timestamptz timestamptz[], "
              + "col_array_date date[], "
              + "col_array_varchar varchar(100)[], "
              + "col_array_jsonb jsonb[])");

  public static ImmutableList<String> getOnlyAllTypesDdl() {
    return DEFAULT_DATA_MODEL.subList(1, 2);
  }

  // The project Id. This can be overwritten.
  private String projectId;

  // The instance name.
  private String instanceId;

  // The database name.
  private String databaseId;

  // The host url.
  private String hostUrl;

  // File path for gcp credentials.
  private String gcpCredentials;

  // Port used by the pgadapter.
  private int port = -1;

  // Shared Spanner instance that is automatically created and closed.
  private Spanner spanner;

  // Spanner options for creating a client.
  private SpannerOptions options;

  // Spanner URL.
  private String spannerHost;

  // Log stream for the test process.
  private static final Logger logger = Logger.getLogger(PgAdapterTestEnv.class.getName());

  private final List<Database> databases = new ArrayList<>();

  private ProxyServer server;

  public static boolean isRunningOnEmulator() {
    return System.getenv("SPANNER_EMULATOR_HOST") != null;
  }

  public void setUp() {
    spannerHost = getSpannerUrl();
    logger.info("Using Spanner host: " + spannerHost);
    options = createSpannerOptions();
  }

  public void startPGAdapterServer(Iterable<String> additionalPGAdapterOptions) {
    startPGAdapterServer(null, additionalPGAdapterOptions, OpenTelemetry.noop());
  }

  public void startPGAdapterServerWithDefaultDatabase(
      DatabaseId databaseId, Iterable<String> additionalPGAdapterOptions) {
    startPGAdapterServer(
        databaseId.getDatabase(), additionalPGAdapterOptions, OpenTelemetry.noop());
  }

  public void startPGAdapterServerWithDefaultDatabase(
      DatabaseId databaseId,
      Iterable<String> additionalPGAdapterOptions,
      OpenTelemetry openTelemetry) {
    startPGAdapterServer(databaseId.getDatabase(), additionalPGAdapterOptions, openTelemetry);
  }

  private void startPGAdapterServer(
      String databaseId, Iterable<String> additionalPGAdapterOptions, OpenTelemetry openTelemetry) {
    if (PG_ADAPTER_ADDRESS == null) {
      additionalPGAdapterOptions = maybeAddAutoConfigEmulator(additionalPGAdapterOptions);
      String credentials = getCredentials();
      ImmutableList.Builder<String> argsListBuilder =
          ImmutableList.<String>builder().add("-p", getProjectId(), "-i", getInstanceId());
      if (databaseId != null) {
        argsListBuilder.add("-d", databaseId);
      }
      argsListBuilder.add("-s", String.valueOf(0));
      if (getSpannerUrl() != null) {
        String host = getSpannerUrl();
        if (host.startsWith("https://")) {
          host = host.substring("https://".length());
        }
        argsListBuilder.add("-e", host);
      }
      if (credentials != null) {
        argsListBuilder.add("-c", getCredentials());
      }
      argsListBuilder.addAll(additionalPGAdapterOptions);
      String[] args = argsListBuilder.build().toArray(new String[0]);
      server = new ProxyServer(new OptionsMetadata(args), openTelemetry);
      server.startServer();
    }
  }

  private Iterable<String> maybeAddAutoConfigEmulator(Iterable<String> additionalPGAdapterOptions) {
    if (System.getenv("SPANNER_EMULATOR_HOST") == null) {
      return additionalPGAdapterOptions;
    }
    List<String> result = new ArrayList<>();
    Iterables.addAll(result, additionalPGAdapterOptions);
    boolean foundExistingOption = false;
    for (int index = 0; index < result.size(); index++) {
      String option = result.get(index);
      if ("-r".equals(option)) {
        foundExistingOption = true;
        // Append to the existing property.
        if (index < result.size() - 1) {
          String value = result.get(index + 1);
          result.remove(index + 1);
          if (value == null) {
            result.add(index + 1, "autoConfigEmulator=true");
          } else {
            result.add(index + 1, value + ";autoConfigEmulator=true");
          }
        } else {
          result.add("autoConfigEmulator=true");
        }
        break;
      }
    }
    if (!foundExistingOption) {
      result.add("-r");
      result.add("autoConfigEmulator=true");
    }
    return result;
  }

  public void stopPGAdapterServer() {
    if (server != null) {
      server.stopServer();
    }
  }

  public ProxyServer getServer() {
    return server;
  }

  public Spanner getSpanner() {
    if (spanner == null) {
      spanner = options.getService();
    }
    return spanner;
  }

  public String getCredentials() {
    if (System.getenv().get(GCP_CREDENTIALS) == null) {
      return null;
    }

    if (gcpCredentials == null) {
      gcpCredentials = System.getenv().get(GCP_CREDENTIALS);
      if (gcpCredentials.isEmpty()) {
        throw new IllegalArgumentException("Invalid GCP credentials file.");
      }
    }
    return gcpCredentials;
  }

  public String getPGAdapterHostAndPort() {
    if (server != null) {
      return String.format("localhost:%d", server.getLocalPort());
    }
    return String.format("%s:%s", PG_ADAPTER_ADDRESS, PG_ADAPTER_PORT);
  }

  public String getPGAdapterHost() {
    if (server != null) {
      return "localhost";
    }
    return PG_ADAPTER_ADDRESS;
  }

  public int getPGAdapterPort() {
    if (server != null) {
      return server.getLocalPort();
    }
    return Integer.parseInt(PG_ADAPTER_PORT);
  }

  public String getSpannerUrl() {
    if (hostUrl == null) {
      hostUrl = System.getProperty(TEST_SPANNER_URL_PROPERTY, DEFAULT_SPANNER_URL);
    }
    return hostUrl;
  }

  void setSpannerUrl(String spannerUrl) {
    this.hostUrl = spannerUrl;
  }

  public String getProjectId() {
    if (projectId == null) {
      projectId = System.getProperty(TEST_PROJECT_PROPERTY, DEFAULT_PROJECT_ID);
    }
    return projectId;
  }

  public String getInstanceId() {
    if (instanceId == null) {
      instanceId = System.getProperty(TEST_INSTANCE_PROPERTY, DEFAULT_INSTANCE_ID);
    }
    return instanceId;
  }

  public String getDatabaseId() {
    if (databaseId == null) {
      databaseId = System.getProperty(TEST_DATABASE_PROPERTY, DEFAULT_DATABASE_ID);
    }
    String id = String.format("%s_%s", databaseId, UUID.randomUUID().toString().replace('-', '_'));
    // Make sure the database id is not longer than the max allowed 30 characters.
    if (id.length() > 30) {
      id = id.substring(0, 30);
    }
    // Database ids may not end with a hyphen or an underscore.
    if (id.endsWith("-") || id.endsWith("_")) {
      id = id.substring(0, id.length() - 1);
    }
    return id;
  }

  public boolean isUseExistingDb() {
    return Boolean.parseBoolean(System.getProperty(USE_EXISTING_DB, "false"));
  }

  public Database getExistingDatabase() {
    if (databaseId == null) {
      databaseId = System.getProperty(TEST_DATABASE_PROPERTY, DEFAULT_DATABASE_ID);
    }
    return getSpanner().getDatabaseAdminClient().getDatabase(instanceId, databaseId);
  }

  // Create database.
  public Database createDatabase(Iterable<String> ddlStatements) {
    if (isUseExistingDb()) {
      return getExistingDatabase();
    }

    if (isUseExistingDb()) {
      throw new IllegalStateException(
          "Cannot create a new test database if " + USE_EXISTING_DB + " is true.");
    }
    String databaseId = getDatabaseId();
    Spanner spanner = getSpanner();

    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    try {
      instanceAdminClient.getInstance(instanceId);
    } catch (InstanceNotFoundException notFoundException) {
      try {
        InstanceConfigId instanceConfigId =
            instanceAdminClient.listInstanceConfigs().iterateAll().iterator().next().getId();
        instanceAdminClient
            .createInstance(
                instanceAdminClient
                    .newInstanceBuilder(InstanceId.of(projectId, instanceId))
                    .setInstanceConfigId(instanceConfigId)
                    .setDisplayName("PGAdapter test instance")
                    .setNodeCount(1)
                    .build())
            .get();
      } catch (ExecutionException executionException) {
        SpannerException spannerException =
            SpannerExceptionFactory.asSpannerException(executionException.getCause());
        // Ignore if it ALREADY_EXISTS. This is caused by multiple test runs trying simultaneously
        // to create an instance.
        if (spannerException.getErrorCode() != ErrorCode.ALREADY_EXISTS) {
          throw spannerException;
        }
      } catch (InterruptedException interruptedException) {
        throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
      }
    }

    DatabaseAdminClient client = spanner.getDatabaseAdminClient();
    OperationFuture<Database, CreateDatabaseMetadata> op =
        client.createDatabase(
            client
                .newDatabaseBuilder(DatabaseId.of(projectId, instanceId, databaseId))
                .setDialect(Dialect.POSTGRESQL)
                .build(),
            Collections.emptyList());
    try {
      Database db = op.get();
      databases.add(db);
      logger.log(Level.INFO, "Created database [" + db.getId() + "]");
      if (!Iterables.isEmpty(ddlStatements)) {
        updateDdl(databaseId, ddlStatements);
      }

      return db;
    } catch (ExecutionException e) {
      throw SpannerExceptionFactory.asSpannerException(e.getCause());
    } catch (InterruptedException e) {
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  public void updateDdl(String databaseId, Iterable<String> statements)
      throws ExecutionException, InterruptedException {
    Spanner spanner = getSpanner();
    DatabaseAdminClient client = spanner.getDatabaseAdminClient();
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        client.updateDatabaseDdl(instanceId, databaseId, statements, null);
    op.get();
    logger.log(Level.INFO, "DDL was updated by {0}.", String.join(" and ", statements));
  }

  // Update tables of the database.
  public void updateTables(String databaseId, Iterable<String> statements) {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(db);
    dbClient
        .readWriteTransaction()
        .run(
            transaction -> {
              List<Statement> batchStatements = new ArrayList<>();
              for (String dml : statements) {
                batchStatements.add(Statement.of(dml));
              }
              if (!batchStatements.isEmpty()) {
                transaction.batchUpdate(batchStatements);
              }
              logger.log(
                  Level.INFO, "Tables were updated by {0}.", String.join(" and ", statements));
              return null;
            });
  }

  /**
   * Writes data to the given test database.
   *
   * @param databaseId The id of the database to write to
   * @param mutations The mutations to write
   */
  public void write(String databaseId, Iterable<Mutation> mutations) {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(db);
    dbClient.write(mutations);
  }

  // Setup spanner options.
  private SpannerOptions createSpannerOptions() {
    projectId = getProjectId();
    instanceId = getInstanceId();

    Map<String, String> env = System.getenv();
    gcpCredentials = env.get(GCP_CREDENTIALS);
    GoogleCredentials credentials = null;
    if (!Strings.isNullOrEmpty(gcpCredentials)) {
      try {
        credentials = GoogleCredentials.fromStream(new FileInputStream(gcpCredentials));
      } catch (IOException e) {
        throw SpannerExceptionFactory.asSpannerException(e);
      }
    }
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(projectId);
    // Use a custom channel provider if that has been configured.
    // This also forces the connection to use no credentials.
    if (System.getProperty(CHANNEL_PROVIDER_PROPERTY) != null) {
      if (spannerHost == null) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "ChannelProvider can only be used in combination with a specific host");
      }
      String channelProvider = System.getProperty(CHANNEL_PROVIDER_PROPERTY);
      logger.info("Using custom channel provider: " + channelProvider);
      try {
        URL url = new URL(spannerHost);
        ExternalChannelProvider provider =
            (ExternalChannelProvider)
                Class.forName(channelProvider).getDeclaredConstructor().newInstance();
        builder.setChannelProvider(provider.getChannelProvider(url.getHost(), url.getPort()));
        builder.setCredentials(NoCredentials.getInstance());
      } catch (Exception e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            String.format(
                "%s : Failed to create channel with external provider: %s", e, channelProvider));
      }
    } else if (spannerHost != null) {
      builder.setHost(spannerHost);
    }

    if (System.getProperty(CHANNEL_PROVIDER_PROPERTY) == null && credentials != null) {
      builder.setCredentials(credentials);
    }
    return builder.build();
  }

  public static class PGMessage {
    public PGMessage(char type, byte[] payload) {
      this.type = type;
      this.payload = payload;
    }

    public char getType() {
      return type;
    }

    public byte[] getPayload() {
      return payload;
    }

    public String toString() {
      return "Type: " + type + " Payload: " + Arrays.toString(payload);
    }

    char type;
    byte[] payload;
  }

  public static class Parameter {
    public Parameter(int oidType, String value) {
      this.oidType = oidType;
      this.value = value.getBytes();
    }

    public int getType() {
      return oidType;
    }

    public int getSize() {
      return value.length;
    }

    public byte[] getValue() {
      return value;
    }

    int oidType;
    byte[] value;
  }

  void waitForServer() throws Exception {
    if (server != null) {
      server.awaitRunning(1L, TimeUnit.SECONDS);
    }
  }

  void initializeConnection(DataOutputStream out) throws Exception {
    // Send start message.
    {
      byte[] metadata =
          ByteBuffer.allocate(PROTOCOL_MESSAGE_SIZE)
              .putInt(PROTOCOL_MESSAGE_SIZE)
              .putInt(INIT_PROTOCOL)
              .array();
      out.write(metadata, 0, metadata.length);
    }

    // Send options.
    {
      String payload =
          "user\0"
              + System.getProperty("user.name")
              + "\0database\0"
              + getDatabaseId()
              + "\0client_encoding\0UTF8\0DateStyle\0ISO\0TimeZone\0America/Los_Angeles\0extra_float_digits\0"
              + "2\0\0";
      byte[] metadata =
          ByteBuffer.allocate(PROTOCOL_MESSAGE_SIZE)
              .putInt(payload.length() + PROTOCOL_MESSAGE_SIZE)
              .putInt(OPTIONS_PROTOCOL)
              .array();
      byte[] message = Bytes.concat(metadata, payload.getBytes());
      out.write(message, 0, message.length);
    }
    logger.log(Level.INFO, "Connected to database " + getDatabaseId());
  }

  PGMessage consumePGMessage(char expectedType, DataInputStream in) throws java.io.IOException {
    char type = (char) in.readByte();
    assertEquals(expectedType, type);
    int length = in.readInt();
    // The length of payload is total length - bytes to express length (4 bytes)
    byte[] payload = new byte[length - 4];
    in.readFully(payload);
    return new PGMessage(type, payload);
  }

  void consumeStartUpMessages(DataInputStream in) throws java.io.IOException {
    // Get result from initialization request. Skip first byte since it is metadata ('N' character).
    assertEquals('N', in.readByte());

    // Check for correct message type identifiers.
    // See https://www.postgresql.org/docs/13/protocol-message-formats.html for more details.
    consumePGMessage('R', in); // AuthenticationOk
    consumePGMessage('K', in); // BackendKeyData
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('Z', in); // ReadyForQuery
  }

  // Drop all the databases we created explicitly.
  public void cleanUp() {
    if (!isUseExistingDb()) {
      for (Database db : databases) {
        try {
          db.drop();
        } catch (Exception e) {
          logger.log(Level.WARNING, "Failed to drop test database " + db.getId(), e);
        }
      }
    }
    if (spanner != null) {
      spanner.close();
      spanner = null;
    }
  }
}
