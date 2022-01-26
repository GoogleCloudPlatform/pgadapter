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

import static com.google.cloud.spanner.pgadapter.ProxyServer.ServerStatus;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Test wrapper used for integration tests. */
public final class PgAdapterTestEnv {
  private static final int PROTOCOL_MESSAGE_SIZE = 8;
  private static final int INIT_PROTOCOL = 80877103;
  private static final int OPTIONS_PROTOCOL = 196608;

  // GCP credentials file should be set through the 'GOOGLE_APPLICATION_CREDENTIALS' environment
  // variable.
  public static final String GCP_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

  // HostUrl should be set through this system property.
  public static final String TEST_HOST_PROPERTY = "PG_ADAPTER_HOST";

  // ProjectId should be set through this system property.
  public static final String TEST_PROJECT_PROPERTY = "PG_ADAPTER_PROJECT";

  // InstanceId should be set through this system property.
  public static final String TEST_INSTANCE_PROPERTY = "PG_ADAPTER_INSTANCE";

  // DatabaseId should be set through this system property.
  public static final String TEST_DATABASE_PROPERTY = "PG_ADAPTER_DATABASE";

  // PgAdapter port should be set through this system property.
  public static final String SERVICE_PORT = "PG_ADAPTER_PORT";

  // Default fallback project Id will be used if one isn't set via the system property.
  private static final String DEFAULT_PROJECT_ID = "span-cloud-testing";

  // Default instance id.
  private static final String DEFAULT_INSTANCE_ID = "spanner-testing";

  // Default database id.
  private static final String DEFAULT_DATABASE_ID = "pgtest-db";

  // Default host url
  private static final String DEFAULT_HOST_URL =
      "https://staging-wrenchworks.sandbox.googleapis.com:443";

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
  private int port = 0;

  // Spanner options for creating a client.
  private SpannerOptions options;

  // Shared Spanner instance that is automatically created and closed.
  private Spanner spanner;

  // Spanner URL.
  private URL spannerURL;

  // Log stream for the test process.
  private static final Logger logger = Logger.getLogger(PgAdapterTestEnv.class.getName());

  private final List<Database> databases = new ArrayList<>();

  public void setUp() throws Exception {
    spannerURL = new URL(getHostUrl());
    options = createSpannerOptions();
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

  public int getPort() {
    if (port == 0) {
      Random rand = new Random(System.currentTimeMillis());
      int defaultPort = rand.nextInt(10000) + 10000;
      port = Integer.parseInt(System.getProperty(SERVICE_PORT, String.valueOf(defaultPort)));
    }
    return port;
  }

  public String getHostUrl() {
    if (hostUrl == null) {
      hostUrl = System.getProperty(TEST_HOST_PROPERTY, DEFAULT_HOST_URL);
    }
    return hostUrl;
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
    return String.format(
        "%s_%d_%d", databaseId, System.currentTimeMillis(), new Random().nextInt(Short.MAX_VALUE));
  }

  public URL getUrl() {
    return spannerURL;
  }

  // Create database.
  public Database createDatabase() throws Exception {
    String databaseId = getDatabaseId();
    Spanner spanner = getSpanner();
    DatabaseAdminClient client = spanner.getDatabaseAdminClient();
    OperationFuture<Database, CreateDatabaseMetadata> op =
        client.createDatabase(
            client
                .newDatabaseBuilder(DatabaseId.of(projectId, instanceId, databaseId))
                .setDialect(Dialect.POSTGRESQL)
                .build(),
            Collections.emptyList());
    Database db = op.get();
    databases.add(db);
    logger.log(Level.INFO, "Created database [" + db.getId() + "]");
    return db;
  }

  public void updateDdl(String databaseId, Iterable<String> statements) throws Exception {
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
  private SpannerOptions createSpannerOptions() throws Exception {
    projectId = getProjectId();
    instanceId = getInstanceId();

    Map<String, String> env = System.getenv();
    gcpCredentials = env.get(GCP_CREDENTIALS);
    GoogleCredentials credentials = null;
    if (!Strings.isNullOrEmpty(gcpCredentials)) {
      credentials = GoogleCredentials.fromStream(new FileInputStream(gcpCredentials));
    }
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder().setProjectId(projectId).setHost(spannerURL.toString());
    if (credentials != null) {
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

  void waitForServer(ProxyServer server) throws Exception {
    // Wait for up to 1 second if server has not started.
    for (int i = 0; i < 10; ++i) {
      if (server.getServerStatus() == ServerStatus.STARTED) {
        return;
      }
      Thread.sleep(100);
    }
    // Throw exception if server has still not started.
    throw new IllegalStateException("ProxyServer failed to start.");
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
    assertThat(type, is(equalTo(expectedType)));
    int length = in.readInt();
    // The length of payload is total length - bytes to express length (4 bytes)
    byte[] payload = new byte[length - 4];
    in.readFully(payload);
    return new PGMessage(type, payload);
  }

  void consumeStartUpMessages(DataInputStream in) throws java.io.IOException {
    // Get result from initialization request. Skip first byte since it is metadata ('N' character).
    assertThat((char) in.readByte(), is(equalTo('N')));

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
    for (Database db : databases) {
      try {
        db.drop();
      } catch (Exception e) {
        logger.log(Level.WARNING, "Failed to drop test database " + db.getId(), e);
      }
    }
    if (spanner != null) {
      spanner.close();
    }
  }
}
