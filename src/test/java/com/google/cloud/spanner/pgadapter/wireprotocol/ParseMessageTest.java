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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.ExtendedQueryProtocolHandler;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.InvalidStatement;
import com.google.cloud.spanner.pgadapter.statements.ReleaseStatement;
import com.google.cloud.spanner.pgadapter.statements.RollbackToStatement;
import com.google.cloud.spanner.pgadapter.statements.SavepointStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.concurrent.ExecutionException;
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

  @Test
  public void testInvalidStatementLifecycle() {
    Statement statement = Statement.of("copy bad_syntax");
    IntermediatePreparedStatement preparedStatement =
        createStatement(connectionHandler, "", PARSER.parse(statement), statement, new int[] {});
    assertEquals(InvalidStatement.class, preparedStatement.getClass());
    InvalidStatement invalidStatement = (InvalidStatement) preparedStatement;
    assertTrue(invalidStatement.hasException());
    assertSame(
        invalidStatement,
        invalidStatement.createPortal("", new byte[0][], new short[0], new short[0]));

    BackendConnection backendConnection = mock(BackendConnection.class);
    invalidStatement.autoDescribeParameters(new byte[][] {"1".getBytes()}, backendConnection);
    invalidStatement.executeAsync(backendConnection);
    assertTrue(invalidStatement.isExecuted());
    verify(backendConnection, never()).analyze(any(), any(), any());
    verify(backendConnection, never()).execute(any(), any(), any(), any());

    assertThrows(PGException.class, invalidStatement::describe);
    ExecutionException executionException =
        assertThrows(
            ExecutionException.class,
            () -> invalidStatement.describeAsync(backendConnection).get());
    assertEquals(invalidStatement.getException(), executionException.getCause());
  }

  @Test
  public void testParseMessageUnnamedAndInvalidLifecycle() throws Exception {
    ConnectionHandler localConnection = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    ExtendedQueryProtocolHandler protocolHandler = mock(ExtendedQueryProtocolHandler.class);
    BackendConnection backendConnection = mock(BackendConnection.class);
    ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(outputBytes);

    when(server.getOptions()).thenReturn(options);
    when(localConnection.getServer()).thenReturn(server);
    when(localConnection.getWellKnownClient()).thenReturn(WellKnownClient.UNSPECIFIED);
    when(localConnection.getConnectionMetadata()).thenReturn(metadata);
    when(metadata.getOutputStream()).thenReturn(outputStream);
    when(localConnection.getExtendedQueryProtocolHandler()).thenReturn(protocolHandler);
    when(protocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(localConnection.hasStatement("")).thenReturn(true);

    Statement statement = Statement.of("copy bad_syntax");
    ParseMessage parseMessage =
        new ParseMessage(localConnection, PARSER.parse(statement), statement);
    // Constructing a ParseMessage should not mutate ConnectionHandler statements before buffer().
    verify(localConnection, never()).closeStatement("");
    verify(localConnection, never()).registerStatement(eq(""), any());
    assertEquals(InvalidStatement.class, parseMessage.getStatement().getClass());

    when(localConnection.getStatement("")).thenReturn(parseMessage.getStatement());
    parseMessage.buffer(backendConnection);
    verify(backendConnection).execute((InvalidStatement) parseMessage.getStatement());
    verify(localConnection).registerStatement("", parseMessage.getStatement());
    // No ErrorResponse should be written during buffer().
    assertEquals(0, outputBytes.size());

    // flush() must close the unnamed statement "" and emit ErrorResponse.
    parseMessage.flush();
    verify(localConnection).closeStatement("");
    assertTrue(parseMessage.isReturnedErrorResponse());
    assertTrue(outputBytes.size() > 0);
  }

  @Test
  public void testParseMessageDuplicateStatementName() throws Exception {
    ConnectionHandler localConnection = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    ExtendedQueryProtocolHandler protocolHandler = mock(ExtendedQueryProtocolHandler.class);
    BackendConnection backendConnection = mock(BackendConnection.class);
    IntermediatePreparedStatement existingStatement = mock(IntermediatePreparedStatement.class);
    ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(outputBytes);

    when(server.getOptions()).thenReturn(options);
    when(localConnection.getServer()).thenReturn(server);
    when(localConnection.getWellKnownClient()).thenReturn(WellKnownClient.UNSPECIFIED);
    when(localConnection.getConnectionMetadata()).thenReturn(metadata);
    when(metadata.getOutputStream()).thenReturn(outputStream);
    when(localConnection.getExtendedQueryProtocolHandler()).thenReturn(protocolHandler);
    when(protocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(localConnection.hasStatement("test_stmt")).thenReturn(true);
    when(localConnection.getStatement("test_stmt")).thenReturn(existingStatement);

    Statement statement = Statement.of("select 1");
    ParseMessage parseMessage =
        new ParseMessage(
            localConnection, "test_stmt", new int[0], PARSER.parse(statement), statement);

    // buffer() should not throw IllegalStateException, should not overwrite existingStatement,
    // and should execute InvalidStatement with SQLState.DuplicatePreparedStatement.
    parseMessage.buffer(backendConnection);
    assertTrue(parseMessage.getStatement() instanceof InvalidStatement);
    assertEquals(
        SQLState.DuplicatePreparedStatement,
        ((InvalidStatement) parseMessage.getStatement()).getException().getSQLState());
    verify(backendConnection).execute((InvalidStatement) parseMessage.getStatement());
    verify(localConnection, never()).registerStatement(eq("test_stmt"), any());

    // flush() should send ErrorResponse without removing the existing statement from
    // ConnectionHandler.
    parseMessage.flush();
    verify(localConnection, never()).closeStatement("test_stmt");
    assertTrue(parseMessage.isReturnedErrorResponse());
    assertTrue(outputBytes.size() > 0);

    // abort() should also not close the existing statement.
    parseMessage.abort();
    verify(localConnection, never()).closeStatement("test_stmt");
  }
}
