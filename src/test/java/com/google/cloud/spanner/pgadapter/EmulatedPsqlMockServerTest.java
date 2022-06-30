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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.rpc.ResourceInfo;
import com.google.spanner.admin.database.v1.Database;
import com.google.spanner.admin.database.v1.DatabaseDialect;
import com.google.spanner.admin.database.v1.ListDatabasesResponse;
import com.google.spanner.admin.instance.v1.Instance;
import com.google.spanner.admin.instance.v1.ListInstanceConfigsResponse;
import com.google.spanner.admin.instance.v1.ListInstancesResponse;
import com.google.spanner.v1.DatabaseName;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.SessionName;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.PGProperty;

@RunWith(JUnit4.class)
public class EmulatedPsqlMockServerTest extends AbstractMockServerTest {
  // @Rule public Timeout globalTimeout = Timeout.seconds(10);

  private static final String INSERT1 = "insert into foo values (1)";
  private static final String INSERT2 = "insert into foo values (2)";

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter without a default database.
    doStartMockSpannerAndPgAdapterServers(null, ImmutableList.of());

    mockSpanner.putStatementResults(
        StatementResult.update(Statement.of(INSERT1), 1L),
        StatementResult.update(Statement.of(INSERT2), 1L));
  }

  @After
  public void removeExecutionTimes() {
    mockSpanner.removeAllExecutionTimes();
  }

  /**
   * Creates a JDBC connection string that instructs the PG JDBC driver to use the default simple
   * mode for queries and DML statements. This makes the JDBC driver behave in (much) the same way
   * as psql. It also adds 'psql' as the application name, which will make PGAdapter automatically
   * recognize the connection as a psql connection.
   */
  private String createUrl(String database) {
    return String.format(
        "jdbc:postgresql://localhost:%d/%s?preferQueryMode=simple&%s=psql&%s=090000",
        pgServer.getLocalPort(),
        database,
        PGProperty.APPLICATION_NAME.getName(),
        PGProperty.ASSUME_MIN_SERVER_VERSION.getName());
  }

  @Test
  public void testConnectToDifferentDatabases() throws SQLException {
    final ImmutableList<String> databases = ImmutableList.of("db1", "db2");
    for (String database : databases) {
      try (Connection connection = DriverManager.getConnection(createUrl(database))) {
        connection.createStatement().execute(INSERT1);
      }
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(databases.size(), requests.size());
    for (int i = 0; i < requests.size(); i++) {
      assertEquals(databases.get(i), SessionName.parse(requests.get(i).getSession()).getDatabase());
    }
  }

  @Test
  public void testConnectToFullDatabasePath() throws Exception {
    String databaseName =
        "projects/full-path-test-project/instances/full-path-test-instance/databases/full-path-test-database";
    // Note that we need to URL encode the database name as it contains multiple forward slashes.
    try (Connection connection =
        DriverManager.getConnection(
            createUrl(URLEncoder.encode(databaseName, StandardCharsets.UTF_8.name())))) {
      connection.createStatement().execute(INSERT1);
    }

    List<ExecuteSqlRequest> requests = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class);
    assertEquals(1, requests.size());
    SessionName sessionName = SessionName.parse(requests.get(0).getSession());
    DatabaseName gotDatabaseName =
        DatabaseName.of(
            sessionName.getProject(), sessionName.getInstance(), sessionName.getDatabase());
    assertEquals(DatabaseName.parse(databaseName), gotDatabaseName);
  }

  @Ignore(
      "Skipped because of a bug in the gRPC server implementation that causes random NullPointerExceptions")
  @Test
  public void testConnectToNonExistingDatabase() {
    try {
      mockSpanner.setBatchCreateSessionsExecutionTime(
          SimulatedExecutionTime.stickyDatabaseNotFoundException("non-existing-db"));
      // The Connection API calls listInstanceConfigs(..) once first when the connection is a
      // localhost connection. It does so to verify that the connection is valid and to quickly
      // return an error if someone is for example trying to connect to the emulator while the
      // emulator is not running. This does not happen when you connect to a remote host. We
      // therefore need to add a response for the listInstanceConfigs as well.
      mockInstanceAdmin.addResponse(ListInstanceConfigsResponse.getDefaultInstance());
      mockInstanceAdmin.addResponse(
          Instance.newBuilder()
              .setName("projects/p/instances/i")
              .setConfig("projects/p/instanceConfigs/ic")
              .build());
      mockDatabaseAdmin.addResponse(
          ListDatabasesResponse.newBuilder()
              .addDatabases(
                  Database.newBuilder()
                      .setName("projects/p/instances/i/databases/d")
                      .setDatabaseDialect(DatabaseDialect.POSTGRESQL)
                      .build())
              .addDatabases(
                  Database.newBuilder()
                      .setName("projects/p/instances/i/databases/google-sql-db")
                      .setDatabaseDialect(DatabaseDialect.GOOGLE_STANDARD_SQL)
                      .build())
              .build());

      SQLException exception =
          assertThrows(
              SQLException.class, () -> DriverManager.getConnection(createUrl("non-existing-db")));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));
      assertTrue(
          exception.getMessage(),
          exception
              .getMessage()
              .contains(
                  "These PostgreSQL databases are available on instance projects/p/instances/i:"));
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("\tprojects/p/instances/i/databases/d\n"));
      assertFalse(
          exception.getMessage(),
          exception.getMessage().contains("\tprojects/p/instances/i/databases/google-sql-db\n"));
    } finally {
      closeSpannerPool(true);
    }
  }

  @Test
  public void testConnectToNonExistingInstance() {
    try {
      mockSpanner.setExecuteStreamingSqlExecutionTime(
          SimulatedExecutionTime.ofStickyException(
              newStatusResourceNotFoundException(
                  "i",
                  "type.googleapis.com/google.spanner.admin.instance.v1.Instance",
                  "projects/p/instances/i")));
      // The Connection API calls listInstanceConfigs(..) once first when the connection is a
      // localhost connection. It does so to verify that the connection is valid and to quickly
      // return an error if someone is for example trying to connect to the emulator while the
      // emulator is not running. This does not happen when you connect to a remote host. We
      // therefore need to add a response for the listInstanceConfigs as well.
      mockInstanceAdmin.addResponse(ListInstanceConfigsResponse.getDefaultInstance());
      mockInstanceAdmin.addResponse(
          ListInstancesResponse.newBuilder()
              .addInstances(
                  Instance.newBuilder()
                      .setConfig("projects/p/instanceConfigs/ic")
                      .setName("projects/p/instances/i")
                      .build())
              .build());

      SQLException exception =
          assertThrows(
              SQLException.class, () -> DriverManager.getConnection(createUrl("non-existing-db")));
      assertTrue(exception.getMessage(), exception.getMessage().contains("NOT_FOUND"));
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("These instances are available in project p:"));
      assertTrue(
          exception.getMessage(), exception.getMessage().contains("\tprojects/p/instances/i\n"));
    } finally {
      closeSpannerPool(true);
    }
  }

  @Test
  public void testTwoInserts() throws SQLException {
    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      connection.createStatement().execute(String.format("%s; %s", INSERT1, INSERT2));
    }

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    ExecuteBatchDmlRequest request =
        mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0);
    assertEquals(2, request.getStatementsCount());
    assertEquals(INSERT1, request.getStatements(0).getSql());
    assertEquals(INSERT2, request.getStatements(1).getSql());
  }

  @Test
  public void testNestedBlockComment() throws SQLException {
    String sql1 =
        "/* This block comment surrounds a query which itself has a block comment...\n"
            + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
            + "*/\n"
            + "SELECT 1";
    String sql2 = "-- This is a line comment\n SELECT 2";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql1), SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql2), SELECT2_RESULTSET));

    try (Connection connection = DriverManager.getConnection(createUrl("my-db"))) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertTrue(statement.execute(String.format("%s;%s;", sql1, sql2)));
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(1L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals(2L, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
        assertFalse(statement.getMoreResults());
      }
    }
  }

  static StatusRuntimeException newStatusResourceNotFoundException(
      String shortName, String resourceType, String resourceName) {
    ResourceInfo resourceInfo =
        ResourceInfo.newBuilder()
            .setResourceType(resourceType)
            .setResourceName(resourceName)
            .build();
    Metadata.Key<ResourceInfo> key =
        Metadata.Key.of(
            resourceInfo.getDescriptorForType().getFullName() + Metadata.BINARY_HEADER_SUFFIX,
            ProtoLiteUtils.metadataMarshaller(resourceInfo));
    Metadata trailers = new Metadata();
    trailers.put(key, resourceInfo);
    String message =
        String.format("%s not found: %s with id %s not found", shortName, shortName, resourceName);
    return Status.NOT_FOUND.withDescription(message).asRuntimeException(trailers);
  }
}
