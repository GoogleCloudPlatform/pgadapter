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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Test wrapper used for integration tests. */
public final class PgAdapterTestEnv {
  // GCP credentials file should be set through the 'GOOGLE_APPLICATION_CREDENTIALS' environment
  // variable.
  public static final String GCP_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

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

  // The project Id. This can be overwritten.
  private String projectId;

  // The instance name.
  private String instanceId;

  // The database name.
  private String databaseId;

  // File path for gcp credentials.
  private String gcpCredentials;

  // Port used by the pgadapter.
  private int port = 0;

  // Spanner options for creating a client.
  private SpannerOptions options;

  // Spanner URL.
  private URL spannerURL;

  // Log stream for the test process.
  private static final Logger logger = Logger.getLogger(PgAdapterTestEnv.class.getName());

  // Utility class for setting up test connection.
  private RemoteSpannerHelper spannerHelper;

  public void setUp() throws Exception {
    spannerURL = new URL("https://staging-wrenchworks.sandbox.googleapis.com:443");
    options = createSpannerOptions();
  }

  public SpannerOptions spannerOptions() {
    return options;
  }

  public String getCredentials() throws Exception {
    if (gcpCredentials == null) {
      Map<String, String> env = System.getenv();
      gcpCredentials = env.get(GCP_CREDENTIALS);
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
    return databaseId;
  }

  public URL getUrl() {
    return spannerURL;
  }

  // Create database.
  public Database createDatabase(String databaseId, Iterable<String> statements) throws Exception {
    Spanner spanner = options.getService();
    DatabaseAdminClient client = spanner.getDatabaseAdminClient();
    Database db = null;
    OperationFuture<Database, CreateDatabaseMetadata> op =
        client.createDatabase(instanceId, databaseId, statements);
    db = op.get();
    logger.log(Level.INFO, "Created database [" + db.getId() + "]");
    return db;
  }

  // Setup spanner options.
  private SpannerOptions createSpannerOptions() throws Exception {
    projectId = System.getProperty(TEST_PROJECT_PROPERTY, DEFAULT_PROJECT_ID);
    instanceId = System.getProperty(TEST_INSTANCE_PROPERTY, DEFAULT_INSTANCE_ID);

    Map<String, String> env = System.getenv();
    gcpCredentials = env.get(GCP_CREDENTIALS);
    GoogleCredentials credentials = null;
    credentials = GoogleCredentials.fromStream(new FileInputStream(gcpCredentials));
    return SpannerOptions.newBuilder()
        .setProjectId(projectId)
        .setHost(spannerURL.toString())
        .setCredentials(credentials)
        .build();
  }
}
