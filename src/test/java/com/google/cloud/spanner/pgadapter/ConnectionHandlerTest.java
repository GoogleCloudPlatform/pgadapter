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

import static com.google.cloud.spanner.pgadapter.ConnectionHandler.appendPropertiesToUrl;
import static com.google.cloud.spanner.pgadapter.ConnectionHandler.buildConnectionURL;
import static com.google.cloud.spanner.pgadapter.ConnectionHandler.listDatabasesOrInstances;
import static com.google.cloud.spanner.pgadapter.EmulatedPsqlMockServerTest.newStatusResourceNotFoundException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.paging.Pages;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerException.ResourceNotFoundException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.TestChannelProvider;
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
import com.google.spanner.v1.DatabaseName;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    assertThrows(PGException.class, () -> connection.closeStatement("unknown"));
  }

  @Test
  public void testCloseAll() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    verify(socket).close();
  }

  @Test
  public void testTerminateDoesNotCloseSocketTwice() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.isClosed()).thenReturn(false, true);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    // IOException should be handled internally in terminate().
    doThrow(new IOException("test exception")).when(socket).close();

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.setThread(mock(Thread.class));

    connection.terminate();
    verify(socket).close();
  }

  @Test
  public void testTerminateClosesAllPortals() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    IntermediatePortalStatement portal1 = mock(IntermediatePortalStatement.class);

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    connection.registerPortal("portal1", portal1);

    assertSame(portal1, connection.getPortal("portal1"));
  }

  @Test
  public void testGetUnknownPortal() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    PGException exception =
        assertThrows(PGException.class, () -> connection.getPortal("unknown portal"));
    assertEquals(SQLState.InvalidCursorName, exception.getSQLState());
  }

  @Test
  public void testClosePortal() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

    ConnectionHandler connection = new ConnectionHandler(server, socket);
    PGException exception =
        assertThrows(PGException.class, () -> connection.closePortal("unknown portal"));
    assertEquals(SQLState.InvalidCursorName, exception.getSQLState());
  }

  @Test
  public void testHandleMessages_NonFatalException() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    connection.setThread(mock(Thread.class));

    connection.setMessageState(message);
    connection.handleMessages();

    assertEquals(ConnectionStatus.UNAUTHENTICATED, connection.getStatus());
  }

  @Test
  public void testHandleMessages_FatalException() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    connection.setThread(mock(Thread.class));

    connection.setMessageState(message);
    connection.handleMessages();

    assertEquals(ConnectionStatus.TERMINATED, connection.getStatus());
  }

  @Test
  public void testCancelActiveStatement() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    Connection spannerConnection = mock(Connection.class);

    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket, mock(Connection.class));
    ConnectionHandler connectionHandlerToCancel =
        new ConnectionHandler(server, socket, spannerConnection);
    connectionHandlerToCancel.setThread(mock(Thread.class));

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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    AtomicBoolean calledCreateSSLSocket = new AtomicBoolean();
    ConnectionHandler connection =
        new ConnectionHandler(server, socket) {
          @Override
          void createSSLSocket() {
            calledCreateSSLSocket.set(true);
          }
        };
    connection.setThread(mock(Thread.class));
    connection.restartConnectionWithSsl();

    assertTrue(calledCreateSSLSocket.get());
  }

  @Test
  public void testRestartConnectionWithSsl_SslSocketCreationFailureIsConvertedToPGException() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };

    assertTrue(connectionHandler.checkValidConnection(false));
  }

  @Test
  public void testCheckValidConnection_anyLocalAddress() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };

    assertTrue(connectionHandler.checkValidConnection(false));
  }

  @Test
  public void testCheckValidConnection_ssl() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return new ConnectionMetadata(mock(InputStream.class), mock(OutputStream.class));
          }
        };
    connectionHandler.setThread(mock(Thread.class));

    when(options.getSslMode()).thenReturn(SslMode.Enable);
    assertTrue(connectionHandler.checkValidConnection(false));

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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket);

    assertTrue(connectionHandler.checkValidConnection(false));
  }

  @Test
  public void testMaybeDetermineWellKnownClient_remainsUnspecifiedForUnknownStatement() {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
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
    assumeTrue(
        System.getProperty(OptionsMetadata.USE_VIRTUAL_THREADS_SYSTEM_PROPERTY_NAME) == null);
    assumeTrue(
        System.getProperty(OptionsMetadata.USE_VIRTUAL_GRPC_TRANSPORT_THREADS_SYSTEM_PROPERTY_NAME)
            == null);

    OptionsMetadata options =
        OptionsMetadata.newBuilder().setCredentials(NoCredentials.getInstance()).build();
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
                .setCredentials(NoCredentials.getInstance())
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

    // Set a channel provider.
    String currentChannelProvider = System.getProperty("CHANNEL_PROVIDER");
    String currentEnableChannelProvider = System.getProperty("ENABLE_CHANNEL_PROVIDER");
    try {
      System.setProperty("CHANNEL_PROVIDER", TestChannelProvider.class.getName());
      assertEquals(
          "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database"
              + ";userAgent=pg-adapter;dialect=postgresql"
              + ";channelProvider=com.google.cloud.spanner.connection.TestChannelProvider"
              + ";usePlainText=true",
          buildConnectionURL(
              "projects/my-project/instances/my-instance/databases/my-database",
              options,
              buildProperties(ImmutableMap.of())));
      assertEquals("true", System.getProperty("ENABLE_CHANNEL_PROVIDER"));

      // Set an invalid channel provider.
      System.clearProperty("ENABLE_CHANNEL_PROVIDER");
      System.setProperty("CHANNEL_PROVIDER", "foo");
      assertThrows(
          SpannerException.class,
          () ->
              buildConnectionURL(
                  "projects/my-project/instances/my-instance/databases/my-database",
                  options,
                  buildProperties(ImmutableMap.of())));
      assertNull(System.getProperty("ENABLE_CHANNEL_PROVIDER"));
    } finally {
      if (currentChannelProvider == null) {
        System.clearProperty("CHANNEL_PROVIDER");
      } else {
        System.setProperty("CHANNEL_PROVIDER", currentChannelProvider);
      }
      if (currentEnableChannelProvider == null) {
        System.clearProperty("ENABLE_CHANNEL_PROVIDER");
      } else {
        System.setProperty("ENABLE_CHANNEL_PROVIDER", currentEnableChannelProvider);
      }
    }

    // Verify that the virtual threads properties are carried over to the connection URL.
    assertEquals(
        "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;useVirtualThreads=true;dialect=postgresql",
        buildConnectionURL(
            "projects/my-project/instances/my-instance/databases/my-database",
            options,
            buildProperties(ImmutableMap.of("useVirtualThreads", "true"))));
    assertEquals(
        "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;useVirtualGrpcTransportThreads=true;dialect=postgresql",
        buildConnectionURL(
            "projects/my-project/instances/my-instance/databases/my-database",
            options,
            buildProperties(ImmutableMap.of("useVirtualGrpcTransportThreads", "true"))));

    runWithSystemProperty(
        OptionsMetadata.USE_VIRTUAL_THREADS_SYSTEM_PROPERTY_NAME,
        "true",
        () -> {
          OptionsMetadata optionsWithSystemProperty =
              OptionsMetadata.newBuilder().setCredentials(NoCredentials.getInstance()).build();
          assertEquals(
              "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;useVirtualThreads=true;dialect=postgresql",
              buildConnectionURL(
                  "projects/my-project/instances/my-instance/databases/my-database",
                  optionsWithSystemProperty,
                  buildProperties(optionsWithSystemProperty.getPropertyMap())));
        });
    runWithSystemProperty(
        OptionsMetadata.USE_VIRTUAL_GRPC_TRANSPORT_THREADS_SYSTEM_PROPERTY_NAME,
        "true",
        () -> {
          OptionsMetadata optionsWithSystemProperty =
              OptionsMetadata.newBuilder().setCredentials(NoCredentials.getInstance()).build();
          assertEquals(
              "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database;userAgent=pg-adapter;useVirtualGrpcTransportThreads=true;dialect=postgresql",
              buildConnectionURL(
                  "projects/my-project/instances/my-instance/databases/my-database",
                  optionsWithSystemProperty,
                  buildProperties(optionsWithSystemProperty.getPropertyMap())));
        });
  }

  @Test
  public void testGetName() {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    Connection connection = mock(Connection.class);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket, connection);

    assertThrows(IllegalStateException.class, connectionHandler::getName);

    Thread thread = new Thread(() -> {}, "test-thread");
    connectionHandler.setThread(thread);
    assertEquals("test-thread", connectionHandler.getName());
  }

  @Test
  public void testStart() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    Connection connection = mock(Connection.class);
    ConnectionHandler connectionHandler = new ConnectionHandler(server, socket, connection);

    assertThrows(IllegalStateException.class, connectionHandler::start);

    SettableApiFuture<Boolean> started = SettableApiFuture.create();
    Thread thread = new Thread(() -> started.set(true), "test-thread");
    connectionHandler.setThread(thread);

    connectionHandler.start();
    assertTrue(started.get(10L, TimeUnit.SECONDS));
  }

  @Test
  public void testAppendPropertiesToUrl() {
    assertEquals(
        "cloudspanner:/project/p/instances/i/databases/d",
        appendPropertiesToUrl("cloudspanner:/project/p/instances/i/databases/d", new Properties()));
    assertEquals(
        "cloudspanner:/project/p/instances/i/databases/d",
        appendPropertiesToUrl("cloudspanner:/project/p/instances/i/databases/d", null));
    assertEquals(
        "cloudspanner:/project/p/instances/i/databases/d",
        appendPropertiesToUrl(
            "cloudspanner:/project/p/instances/i/databases/d",
            buildProperties(ImmutableMap.of("key", ""))));
    assertEquals(
        "cloudspanner:/project/p/instances/i/databases/d;key=value",
        appendPropertiesToUrl(
            "cloudspanner:/project/p/instances/i/databases/d",
            buildProperties(ImmutableMap.of("key", "value"))));
  }

  @Test
  public void testCheckValidConnection_External() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    ProxyServer server = mock(ProxyServer.class);
    when(server.getOptions()).thenReturn(options);
    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(InetAddress.getByName("google.com"));
    Connection connection = mock(Connection.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ConnectionMetadata connectionMetadata =
        new ConnectionMetadata(new ByteArrayInputStream(new byte[0]), bos);

    ConnectionHandler connectionHandler =
        new ConnectionHandler(server, socket, connection) {
          @Override
          public ConnectionMetadata getConnectionMetadata() {
            return connectionMetadata;
          }
        };
    connectionHandler.setThread(new Thread(() -> {}, "test-thread"));

    assertFalse(connectionHandler.checkValidConnection(false));
    byte[] bytes = bos.toByteArray();
    assertEquals(74, bytes.length);
    assertEquals('E', bytes[0]); // Error
    // The total length of the Error message is 68.
    assertArrayEquals(new byte[] {0, 0, 0, 68}, Arrays.copyOfRange(bytes, 1, 5));
    // The next message is a Terminate ('X') message.
    assertEquals('X', bytes[69]);
  }

  @Test
  public void testListDatabasesOrInstances() {
    Spanner spanner = mock(Spanner.class);
    DatabaseAdminClient adminClient = mock(DatabaseAdminClient.class);
    when(spanner.getDatabaseAdminClient()).thenReturn(adminClient);
    when(adminClient.listDatabases(eq("my-instance"), any())).thenReturn(Pages.empty());
    StatusRuntimeException exception =
        newStatusResourceNotFoundException(
            "test-database",
            "type.googleapis.com/google.spanner.admin.database.v1.Database",
            "projects/my-project/instances/my-instance/databases/test-database");
    ResourceNotFoundException resourceNotFoundException =
        (ResourceNotFoundException) SpannerExceptionFactory.asSpannerException(exception);
    assertEquals(
        "Database projects/my-project/instances/my-instance/databases/test-database not found.\n"
            + "\n"
            + "These PostgreSQL databases are available on instance projects/my-project/instances/my-instance:\n",
        listDatabasesOrInstances(
            resourceNotFoundException,
            DatabaseName.of("my-project", "my-instance", "test-database"),
            spanner));
  }

  static Properties buildProperties(Map<String, String> map) {
    Properties properties = new Properties();
    properties.putAll(map);
    return properties;
  }

  void runWithSystemProperty(String property, String value, Runnable runnable) {
    String currentValue = System.getProperty(property);
    try {
      if (value == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, value);
      }
      runnable.run();
    } finally {
      if (currentValue == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, currentValue);
      }
    }
  }
}
