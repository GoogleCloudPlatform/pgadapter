// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import static com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage.createStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.ReleaseStatement;
import com.google.cloud.spanner.pgadapter.statements.RollbackToStatement;
import com.google.cloud.spanner.pgadapter.statements.SavepointStatement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParseMessageTest {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  private static ConnectionHandler connectionHandler;

  @BeforeClass
  public static void setupMockConnection() {
    connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(server.getOptions()).thenReturn(options);
    when(connectionHandler.getServer()).thenReturn(server);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
  }

  @Test
  public void testCreateStatementSavepoint() throws Exception {
    Statement statement = Statement.of("savepoint foo");
    IntermediatePreparedStatement preparedStatement =
        createStatement(connectionHandler, "", PARSER.parse(statement), statement, new int[] {});
    assertEquals(SavepointStatement.class, preparedStatement.getClass());
    SavepointStatement savepointStatement = (SavepointStatement) preparedStatement;
    assertEquals("foo", savepointStatement.getSavepointName());
    assertEquals("SAVEPOINT", savepointStatement.getCommandTag());
    assertEquals(StatementType.CLIENT_SIDE, savepointStatement.getStatementType());
    assertNull(savepointStatement.describeAsync(mock(BackendConnection.class)).get());
  }

  @Test
  public void testCreateStatementRelease() throws Exception {
    Statement statement = Statement.of("release foo");
    IntermediatePreparedStatement preparedStatement =
        createStatement(connectionHandler, "", PARSER.parse(statement), statement, new int[] {});
    assertEquals(ReleaseStatement.class, preparedStatement.getClass());
    ReleaseStatement releaseStatement = (ReleaseStatement) preparedStatement;
    assertEquals("foo", releaseStatement.getSavepointName());
    assertEquals("RELEASE", releaseStatement.getCommandTag());
    assertEquals(StatementType.CLIENT_SIDE, releaseStatement.getStatementType());
    assertNull(releaseStatement.describeAsync(mock(BackendConnection.class)).get());
  }

  @Test
  public void testCreateStatementRollbackTo() throws Exception {
    Statement statement = Statement.of("rollback to foo");
    IntermediatePreparedStatement preparedStatement =
        createStatement(connectionHandler, "", PARSER.parse(statement), statement, new int[] {});
    assertEquals(RollbackToStatement.class, preparedStatement.getClass());
    RollbackToStatement rollbackToStatement = (RollbackToStatement) preparedStatement;
    assertEquals("foo", rollbackToStatement.getSavepointName());
    assertEquals("ROLLBACK", rollbackToStatement.getCommandTag());
    assertEquals(StatementType.CLIENT_SIDE, rollbackToStatement.getStatementType());
    assertNull(rollbackToStatement.describeAsync(mock(BackendConnection.class)).get());
  }

  @Test
  public void testCreateStatementRollback() {
    // Verify that 'rollback' is interpreted as a statement that is not handled by PGAdapter.
    Statement statement = Statement.of("rollback");
    ParsedStatement parsedStatement = PARSER.parse(statement);
    IntermediatePreparedStatement preparedStatement =
        createStatement(connectionHandler, "", parsedStatement, statement, new int[] {});
    assertEquals(IntermediatePreparedStatement.class, preparedStatement.getClass());
    assertEquals(StatementType.CLIENT_SIDE, parsedStatement.getType());
    assertEquals(ClientSideStatementType.ROLLBACK, parsedStatement.getClientSideStatementType());
  }
}
