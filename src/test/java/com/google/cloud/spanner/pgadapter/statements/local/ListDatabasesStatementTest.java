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

package com.google.cloud.spanner.pgadapter.statements.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelperTest;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ListDatabasesStatementTest {

  @Test
  public void testGetSql() {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ListDatabasesStatement statement = new ListDatabasesStatement(connectionHandler);

    assertArrayEquals(new String[] {ListDatabasesStatement.LIST_DATABASES_SQL}, statement.getSql());
  }

  @Test
  public void testEquals() {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ListDatabasesStatement statement1 = new ListDatabasesStatement(connectionHandler);
    ListDatabasesStatement statement2 = new ListDatabasesStatement(connectionHandler);
    ListDatabasesStatement statement3 = new ListDatabasesStatement(mock(ConnectionHandler.class));

    assertEquals(statement1, statement2);
    assertNotEquals(statement1, statement3);
    assertNotEquals(statement1, null);
  }

  @Test
  public void testHashCode() {
    ConnectionHandler connectionHandler1 = mock(ConnectionHandler.class);
    ConnectionHandler connectionHandler2 = mock(ConnectionHandler.class);
    ListDatabasesStatement statement1 = new ListDatabasesStatement(connectionHandler1);
    ListDatabasesStatement statement2 = new ListDatabasesStatement(connectionHandler1);
    ListDatabasesStatement statement3 = new ListDatabasesStatement(connectionHandler2);

    assertEquals(statement1.hashCode(), statement2.hashCode());
    assertNotEquals(statement1.hashCode(), statement3.hashCode());
  }

  @Test
  public void testExecuteNoDefault() {
    Spanner spanner = mock(Spanner.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    when(spanner.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    Connection connection = ConnectionOptionsHelperTest.setupMockConnection(spanner);
    OptionsMetadata options = mock(OptionsMetadata.class);
    DatabaseId currentDatabaseId = DatabaseId.of("my-project", "my-instance", "my-database");
    ConnectionHandler connectionHandler = setupMockConnectionHandler(options, currentDatabaseId);

    // Setup results for databaseAdminClient.listDatabases(currentInstanceId);
    Database currentDatabase = mock(Database.class);
    when(currentDatabase.getId()).thenReturn(currentDatabaseId);
    when(currentDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database otherPgDatabase = mock(Database.class);
    when(otherPgDatabase.getId())
        .thenReturn(DatabaseId.of("my-project", "my-instance", "other-pg-database"));
    when(otherPgDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database otherGsqlDatabase = mock(Database.class);
    when(otherGsqlDatabase.getId())
        .thenReturn(DatabaseId.of("my-project", "my-instance", "other-gsql-database"));
    when(otherGsqlDatabase.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    Page<Database> page =
        createMockPage(ImmutableList.of(otherPgDatabase, otherGsqlDatabase, currentDatabase));
    when(databaseAdminClient.listDatabases(currentDatabaseId.getInstanceId().getInstance()))
        .thenReturn(page);
    BackendConnection backendConnection = mock(BackendConnection.class);
    when(backendConnection.getSpannerConnection()).thenReturn(connection);

    ListDatabasesStatement statement = new ListDatabasesStatement(connectionHandler);
    StatementResult result = statement.execute(backendConnection);

    assertEquals(ResultType.RESULT_SET, result.getResultType());
    ResultSet resultSet = result.getResultSet();
    assertTrue(resultSet.next());
    assertEquals(currentDatabaseId.toString(), resultSet.getString("Name"));
    assertTrue(resultSet.next());
    assertEquals(otherPgDatabase.getId().toString(), resultSet.getString("Name"));
    assertFalse(resultSet.next());
  }

  @Test
  public void testExecuteWithDefaultInstance() {
    Spanner spanner = mock(Spanner.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    when(spanner.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    Connection connection = ConnectionOptionsHelperTest.setupMockConnection(spanner);
    DatabaseId currentDatabaseId = DatabaseId.of("my-project", "my-instance", "my-database");
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.hasDefaultInstanceId()).thenReturn(true);
    when(options.getDefaultInstanceId()).thenReturn(currentDatabaseId.getInstanceId());
    ConnectionHandler connectionHandler = setupMockConnectionHandler(options, currentDatabaseId);

    // Setup results for databaseAdminClient.listDatabases(currentInstanceId);
    Database currentDatabase = mock(Database.class);
    when(currentDatabase.getId()).thenReturn(currentDatabaseId);
    when(currentDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database otherPgDatabase = mock(Database.class);
    when(otherPgDatabase.getId())
        .thenReturn(DatabaseId.of("my-project", "my-instance", "other-pg-database"));
    when(otherPgDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database otherGsqlDatabase = mock(Database.class);
    when(otherGsqlDatabase.getId())
        .thenReturn(DatabaseId.of("my-project", "my-instance", "other-gsql-database"));
    when(otherGsqlDatabase.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    Page<Database> page =
        createMockPage(ImmutableList.of(otherPgDatabase, otherGsqlDatabase, currentDatabase));
    when(databaseAdminClient.listDatabases(currentDatabaseId.getInstanceId().getInstance()))
        .thenReturn(page);
    BackendConnection backendConnection = mock(BackendConnection.class);
    when(backendConnection.getSpannerConnection()).thenReturn(connection);

    ListDatabasesStatement statement = new ListDatabasesStatement(connectionHandler);
    StatementResult result = statement.execute(backendConnection);

    assertEquals(ResultType.RESULT_SET, result.getResultType());
    ResultSet resultSet = result.getResultSet();
    assertTrue(resultSet.next());
    assertEquals(currentDatabaseId.getDatabase(), resultSet.getString("Name"));
    assertTrue(resultSet.next());
    assertEquals(otherPgDatabase.getId().getDatabase(), resultSet.getString("Name"));
    assertFalse(resultSet.next());
  }

  @Test
  public void testExecuteWithDefaultInstanceWhileConnectedToDifferentInstance() {
    Spanner spanner = mock(Spanner.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    when(spanner.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    Connection connection = ConnectionOptionsHelperTest.setupMockConnection(spanner);
    DatabaseId currentDatabaseId = DatabaseId.of("my-project", "my-instance", "my-database");
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.hasDefaultInstanceId()).thenReturn(true);
    when(options.getDefaultInstanceId())
        .thenReturn(InstanceId.of("my-project", "default-instance"));
    ConnectionHandler connectionHandler = setupMockConnectionHandler(options, currentDatabaseId);

    // Setup results for databaseAdminClient.listDatabases(currentInstanceId);
    Database currentDatabase = mock(Database.class);
    when(currentDatabase.getId()).thenReturn(currentDatabaseId);
    when(currentDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database otherPgDatabase = mock(Database.class);
    when(otherPgDatabase.getId())
        .thenReturn(DatabaseId.of("my-project", "my-instance", "other-pg-database"));
    when(otherPgDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database otherGsqlDatabase = mock(Database.class);
    when(otherGsqlDatabase.getId())
        .thenReturn(DatabaseId.of("my-project", "my-instance", "other-gsql-database"));
    when(otherGsqlDatabase.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    Page<Database> page =
        createMockPage(ImmutableList.of(otherPgDatabase, otherGsqlDatabase, currentDatabase));
    when(databaseAdminClient.listDatabases(currentDatabaseId.getInstanceId().getInstance()))
        .thenReturn(page);
    BackendConnection backendConnection = mock(BackendConnection.class);
    when(backendConnection.getSpannerConnection()).thenReturn(connection);

    ListDatabasesStatement statement = new ListDatabasesStatement(connectionHandler);
    StatementResult result = statement.execute(backendConnection);

    assertEquals(ResultType.RESULT_SET, result.getResultType());
    ResultSet resultSet = result.getResultSet();
    assertTrue(resultSet.next());
    assertEquals(currentDatabaseId.toString(), resultSet.getString("Name"));
    assertTrue(resultSet.next());
    assertEquals(otherPgDatabase.getId().toString(), resultSet.getString("Name"));
    assertFalse(resultSet.next());
  }

  @Test
  public void testExecuteWithDefaultDatabase() {
    Spanner spanner = mock(Spanner.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    when(spanner.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    Connection connection = ConnectionOptionsHelperTest.setupMockConnection(spanner);
    DatabaseId currentDatabaseId = DatabaseId.of("my-project", "my-instance", "my-database");
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.hasDefaultConnectionUrl()).thenReturn(true);
    when(options.getDefaultDatabaseId()).thenReturn(currentDatabaseId);
    when(options.hasDefaultInstanceId()).thenReturn(true);
    when(options.getDefaultInstanceId()).thenReturn(currentDatabaseId.getInstanceId());
    ConnectionHandler connectionHandler = setupMockConnectionHandler(options, currentDatabaseId);

    // Setup results for databaseAdminClient.listDatabases(currentInstanceId);
    Database currentDatabase = mock(Database.class);
    when(currentDatabase.getId()).thenReturn(currentDatabaseId);
    when(currentDatabase.getDialect()).thenReturn(Dialect.POSTGRESQL);
    Database.Builder databaseBuilder = mock(Database.Builder.class);
    when(databaseBuilder.setDialect(Dialect.POSTGRESQL)).thenReturn(databaseBuilder);
    when(databaseBuilder.build()).thenReturn(currentDatabase);
    when(databaseAdminClient.newDatabaseBuilder(currentDatabaseId)).thenReturn(databaseBuilder);

    BackendConnection backendConnection = mock(BackendConnection.class);
    when(backendConnection.getSpannerConnection()).thenReturn(connection);

    ListDatabasesStatement statement = new ListDatabasesStatement(connectionHandler);
    StatementResult result = statement.execute(backendConnection);

    assertEquals(ResultType.RESULT_SET, result.getResultType());
    ResultSet resultSet = result.getResultSet();
    assertTrue(resultSet.next());
    assertEquals(currentDatabaseId.getDatabase(), resultSet.getString("Name"));
    assertFalse(resultSet.next());
  }

  private ConnectionHandler setupMockConnectionHandler(
      OptionsMetadata options, DatabaseId databaseId) {
    ProxyServer server = mock(ProxyServer.class);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(connectionHandler.getDatabaseId()).thenReturn(databaseId);
    when(server.getOptions()).thenReturn(options);

    return connectionHandler;
  }

  @SuppressWarnings("unchecked")
  private Page<Database> createMockPage(Iterable<Database> databases) {
    Page<Database> result = mock(Page.class);
    when(result.iterateAll()).thenReturn(databases);

    return result;
  }
}
