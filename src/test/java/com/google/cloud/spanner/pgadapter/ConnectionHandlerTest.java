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

import static com.google.cloud.spanner.pgadapter.ConnectionHandler.buildConnectionURL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectionHandlerTest {

  @Test
  public void testRegisterStatement() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    IntermediatePreparedStatement statement = mock(IntermediatePreparedStatement.class);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerStatement("my-statement", statement);

    assertTrue(connection.hasStatement("my-statement"));
    assertSame(statement, connection.getStatement("my-statement"));

    connection.closeStatement("my-statement");
    assertFalse(connection.hasStatement("my-statement"));
  }

  @Test
  public void testCloseUnknownStatement() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    assertThrows(PGException.class, () -> connection.closeStatement("unknown"));
  }

  @Test
  public void testCloseAll() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    IntermediatePreparedStatement statement1 = mock(IntermediatePreparedStatement.class);
    IntermediatePreparedStatement statement2 = mock(IntermediatePreparedStatement.class);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerStatement("my-statement1", statement1);
    connection.registerStatement("my-statement2", statement2);

    connection.closeAllStatements();

    assertFalse(connection.hasStatement("my-statement1"));
    assertFalse(connection.hasStatement("my-statement2"));
  }

  @Test
  public void testTerminateClosesSocket() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    verify(socket).close();
  }

  @Test
  public void testTerminateDoesNotCloseSocketTwice() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.isClosed()).thenReturn(false, true);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    // Calling terminate a second time should be a no-op.
    connection.terminate();

    // Verify that close was only called once.
    verify(socket).close();
  }

  @Test
  public void testTerminateHandlesCloseError() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    // IOException should be handled internally in terminate().
    doThrow(new IOException("test exception")).when(socket).close();

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    verify(socket).close();
  }

  @Test
  public void testTerminateClosesAllPortals() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    IntermediatePortalStatement portal1 = mock(IntermediatePortalStatement.class);
    IntermediatePortalStatement portal2 = mock(IntermediatePortalStatement.class);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerPortal("portal1", portal1);
    connection.registerPortal("portal2", portal2);

    connection.terminate();

    verify(portal1).close();
    verify(portal2).close();
  }

  @Test
  public void testTerminateIgnoresPortalCloseError() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    IntermediatePortalStatement portal1 = mock(IntermediatePortalStatement.class);
    IntermediatePortalStatement portal2 = mock(IntermediatePortalStatement.class);
    doThrow(new Exception("test")).when(portal2).close();

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerPortal("portal1", portal1);
    connection.registerPortal("portal2", portal2);

    connection.terminate();

    verify(portal1).close();
    verify(portal2).close();
  }

  @Test
  public void testGetPortal() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    IntermediatePortalStatement portal1 = mock(IntermediatePortalStatement.class);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerPortal("portal1", portal1);

    assertSame(portal1, connection.getPortal("portal1"));
  }

  @Test
  public void testGetUnknownPortal() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    PGException exception =
        assertThrows(PGException.class, () -> connection.getPortal("unknown portal"));
    assertEquals(SQLState.InvalidCursorName, exception.getSQLState());
  }

  @Test
  public void testClosePortal() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    IntermediatePortalStatement portal1 = mock(IntermediatePortalStatement.class);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerPortal("portal1", portal1);

    connection.closePortal("portal1");
    verify(portal1).close();
  }

  @Test
  public void testCloseUnknownPortal() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    PGException exception =
        assertThrows(PGException.class, () -> connection.closePortal("unknown portal"));
    assertEquals(SQLState.InvalidCursorName, exception.getSQLState());
  }

  @Test
  public void testHandleMessages_NonFatalException() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    DataOutputStream dataOutputStream = new DataOutputStream(new ByteArrayOutputStream());
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.getOutputStream()).thenReturn(dataOutputStream);
    WireMessage message = mock(WireMessage.class);
    when(server.recordMessage(message)).thenReturn(message);
    doThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION, "non-fatal test exception"))
        .when(message)
        .send();

    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          public ConnectionMetadata getConnectionMetadata() {
            return connectionMetadata;
          }
        };

    connection.setMessageState(message);
    connection.handleMessages();

    assertEquals(ConnectionStatus.UNAUTHENTICATED, connection.getStatus());
  }

  @Test
  public void testHandleMessages_FatalException() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    DataOutputStream dataOutputStream = new DataOutputStream(new ByteArrayOutputStream());
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.getOutputStream()).thenReturn(dataOutputStream);
    WireMessage message = mock(WireMessage.class);
    when(server.recordMessage(message)).thenReturn(message);
    doThrow(new EOFException("fatal test exception")).when(message).send();

    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          public ConnectionMetadata getConnectionMetadata() {
            return connectionMetadata;
          }
        };

    connection.setMessageState(message);
    connection.handleMessages();

    assertEquals(ConnectionStatus.TERMINATED, connection.getStatus());
  }

  @Test
  public void testCancelActiveStatement() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    when(address.getHostAddress()).thenReturn("address1");
    Connection spannerConnection = mock(Connection.class);

    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket, mock(Connection.class));
    ConnectionHandler connectionHandlerToCancel =
        new ConnectionHandler(server, socket, spannerConnection);

    // Cancelling yourself is not allowed.
    assertFalse(
        connectionHandler.cancelActiveStatement(
            connectionHandler.getConnectionId(), connectionHandler.getSecret()));
    // Cancelling a random non-existing connection should not work.
    assertFalse(connectionHandler.cancelActiveStatement(100, 100));
    // Cancelling another connecting using the wrong secret is not allowed.
    assertFalse(
        connectionHandler.cancelActiveStatement(
            connectionHandlerToCancel.getConnectionId(),
            connectionHandlerToCancel.getSecret() - 1));

    assertTrue(
        connectionHandler.cancelActiveStatement(
            connectionHandlerToCancel.getConnectionId(), connectionHandlerToCancel.getSecret()));

    // The method should just return false if an error occurs.
    doThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.INTERNAL, "test error"))
        .when(spannerConnection)
        .cancel();
    assertFalse(
        connectionHandler.cancelActiveStatement(
            connectionHandlerToCancel.getConnectionId(), connectionHandlerToCancel.getSecret()));
  }

  @Test
  public void testRestartConnectionWithSsl_CreatesSslSocket() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    AtomicBoolean calledCreateSSLSocket = new AtomicBoolean();
    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          @Override
          void createSSLSocket() {
            calledCreateSSLSocket.set(true);
          }
        };
    connection.restartConnectionWithSsl();

    assertTrue(calledCreateSSLSocket.get());
  }

  @Test
  public void testRestartConnectionWithSsl_SslSocketCreationFailureIsConvertedToPGException() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          @Override
          void createSSLSocket() throws IOException {
            throw new IOException();
          }
        };
    PGException exception = assertThrows(PGException.class, connection::restartConnectionWithSsl);
    assertEquals("Failed to create SSL socket: java.io.IOException", exception.getMessage());
  }

  @Test
  public void
      testRestartConnectionWithSsl_SslSocketCreationFailureIsConvertedToPGExceptionWithMessage() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          @Override
          void createSSLSocket() throws IOException {
            throw new IOException("test error");
          }
        };
    PGException exception = assertThrows(PGException.class, connection::restartConnectionWithSsl);
    assertEquals("Failed to create SSL socket: test error", exception.getMessage());
  }

  @Test
  public void testRestartConnectionWithSsl_SendsPGException() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          @Override
          void createSSLSocket() throws IOException {
            throw new IOException("test exception");
          }

          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };
    PGException pgException = assertThrows(PGException.class, connection::restartConnectionWithSsl);
    assertEquals(Severity.FATAL, pgException.getSeverity());
    assertEquals(SQLState.InternalError, pgException.getSQLState());
  }

  @Test
  public void testRestartConnectionWithSsl_IgnoresExceptionErrorHandling() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);

    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          @Override
          void createSSLSocket() throws IOException {
            throw new IOException("test exception");
          }

          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }

          @Override
          void handleError(PGException exception) {
            throw new RuntimeException("test error during error handling");
          }
        };
    PGException pgException = assertThrows(PGException.class, connection::restartConnectionWithSsl);
    assertEquals(Severity.FATAL, pgException.getSeverity());
    assertEquals(SQLState.InternalError, pgException.getSQLState());
  }

  @Test
  public void testCheckValidConnection_loopback() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };

    when(address.isLoopbackAddress()).thenReturn(true);
    assertTrue(connectionHandler.checkValidConnection(false));

    when(address.isLoopbackAddress()).thenReturn(false);
    assertFalse(connectionHandler.checkValidConnection(false));
  }

  @Test
  public void testCheckValidConnection_anyLocalAddress() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };

    when(address.isAnyLocalAddress()).thenReturn(true);
    assertTrue(connectionHandler.checkValidConnection(false));

    when(address.isAnyLocalAddress()).thenReturn(false);
    assertFalse(connectionHandler.checkValidConnection(false));
  }

  @Test
  public void testCheckValidConnection_ssl() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };

    when(options.getSslMode()).thenReturn(SslMode.Enable);
    when(address.isLoopbackAddress()).thenReturn(true);
    assertTrue(connectionHandler.checkValidConnection(false));

    assertTrue(connectionHandler.checkValidConnection(true));
    when(address.isLoopbackAddress()).thenReturn(false);
    assertTrue(connectionHandler.checkValidConnection(true));

    when(options.getSslMode()).thenReturn(SslMode.Require);
    assertFalse(connectionHandler.checkValidConnection(false));
    assertTrue(connectionHandler.checkValidConnection(true));
  }

  @Test
  public void testCheckValidConnection_localhostCheckDisabled() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.disableLocalhostCheck()).thenReturn(true);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    assertTrue(connectionHandler.checkValidConnection(false));
  }

  @Test
  public void testMaybeDetermineWellKnownClient_remainsUnspecifiedForUnknownStatement() {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    when(options.shouldAutoDetectClient()).thenReturn(true);
    connectionHandler.setWellKnownClient(WellKnownClient.UNSPECIFIED);

    connectionHandler.maybeDetermineWellKnownClient(Statement.of("select 1"));

    assertEquals(WellKnownClient.UNSPECIFIED, connectionHandler.getWellKnownClient());
  }

  @Test
  public void testMaybeDetermineWellKnownClient_changesFromUnspecifiedWithKnownStatement() {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    when(options.shouldAutoDetectClient()).thenReturn(true);
    connectionHandler.setWellKnownClient(WellKnownClient.UNSPECIFIED);

    connectionHandler.maybeDetermineWellKnownClient(
        Statement.of(
            "SELECT version();\n"
                + "\n"
                + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid\n"));

    assertEquals(WellKnownClient.NPGSQL, connectionHandler.getWellKnownClient());
  }

  @Test
  public void testMaybeDetermineWellKnownClient_respectsAutoDetectClientSetting() {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    when(options.shouldAutoDetectClient()).thenReturn(false);
    connectionHandler.setWellKnownClient(WellKnownClient.UNSPECIFIED);

    connectionHandler.maybeDetermineWellKnownClient(
        Statement.of(
            "SELECT version();\n"
                + "\n"
                + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid\n"));

    assertEquals(WellKnownClient.UNSPECIFIED, connectionHandler.getWellKnownClient());
  }

  @Test
  public void testMaybeDetermineWellKnownClient_skipsClientSideStatements() {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    when(options.shouldAutoDetectClient()).thenReturn(true);
    connectionHandler.setWellKnownClient(WellKnownClient.UNSPECIFIED);

    ParseMessage parseMessage = mock(ParseMessage.class);
    IntermediatePreparedStatement statement = mock(IntermediatePreparedStatement.class);
    when(statement.getStatementType()).thenReturn(StatementType.CLIENT_SIDE);
    when(parseMessage.getStatement()).thenReturn(statement);

    connectionHandler.maybeDetermineWellKnownClient(parseMessage);

    assertEquals(WellKnownClient.UNSPECIFIED, connectionHandler.getWellKnownClient());
    assertFalse(connectionHandler.isHasDeterminedClientUsingQuery());
    assertEquals(
        ImmutableList.of(parseMessage), connectionHandler.getSkippedAutoDetectParseMessages());
  }

  @Test
  public void testMaybeDetermineWellKnownClient_stopsSkippingParseMessagesAfter10Messages() {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    when(options.shouldAutoDetectClient()).thenReturn(true);
    connectionHandler.setWellKnownClient(WellKnownClient.UNSPECIFIED);

    ParseMessage parseMessage = mock(ParseMessage.class);
    IntermediatePreparedStatement statement = mock(IntermediatePreparedStatement.class);
    when(statement.getStatementType()).thenReturn(StatementType.CLIENT_SIDE);
    when(parseMessage.getStatement()).thenReturn(statement);

    for (int i = 0; i < 10; i++) {
      connectionHandler.maybeDetermineWellKnownClient(parseMessage);
    }

    assertEquals(WellKnownClient.UNSPECIFIED, connectionHandler.getWellKnownClient());
    assertFalse(connectionHandler.isHasDeterminedClientUsingQuery());
    assertEquals(10, connectionHandler.getSkippedAutoDetectParseMessages().size());

    // This should cause the connection to stop buffering Parse messages and just set the client to
    // UNSPECIFIED.
    connectionHandler.maybeDetermineWellKnownClient(parseMessage);

    assertTrue(connectionHandler.isHasDeterminedClientUsingQuery());
    assertTrue(connectionHandler.getSkippedAutoDetectParseMessages().isEmpty());
  }

  @Test
  public void testBuildConnectionUrl() {
    OptionsMetadata options = OptionsMetadata.newBuilder().build();
    // Check that the dialect is included in the connection URL. This is required to support the
    // 'autoConfigEmulator' property.
    assertEquals(
        "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;dialect=postgresql",
        buildConnectionURL(
            "projects/my-project/instances/my-instance/databases/my-database",
            options,
            buildProperties(ImmutableMap.of())));
    assertEquals(
        "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;key1=value1;dialect=postgresql",
        buildConnectionURL(
            "projects/my-project/instances/my-instance/databases/my-database",
            options,
            buildProperties(ImmutableMap.of("key1", "value1"))));

    // If the options contain a full database specification, then the database in the connection
    // request is ignored.
    assertEquals(
        "cloudspanner:/projects/test-project/instances/test-instance/databases/test-database;userAgent=pg-adapter;dialect=postgresql",
        buildConnectionURL(
            "projects/my-project/instances/my-instance/databases/my-database",
            OptionsMetadata.newBuilder()
                .setProject("test-project")
                .setInstance("test-instance")
                .setDatabase("test-database")
                .build(),
            buildProperties(ImmutableMap.of())));
    // Enable the autoConfigEmulator flag through the options builder.
    OptionsMetadata emulatorOptions = OptionsMetadata.newBuilder().autoConfigureEmulator().build();
    assertEquals(
        "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;autoConfigEmulator=true;dialect=postgresql",
        buildConnectionURL(
            "projects/my-project/instances/my-instance/databases/my-database",
            emulatorOptions,
            buildProperties(emulatorOptions.getPropertyMap())));
  }

  static Properties buildProperties(Map<String, String> map) {
    Properties properties = new Properties();
    properties.putAll(map);
    return properties;
  }
}
