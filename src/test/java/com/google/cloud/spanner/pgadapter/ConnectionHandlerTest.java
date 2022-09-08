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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
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
  public void testHandleMessages_NonFatalException() throws Exception {
    ProxyServer server = mock(ProxyServer.class);
    Socket socket = mock(Socket.class);
    InetAddress address = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(address);
    DataOutputStream dataOutputStream = new DataOutputStream(new ByteArrayOutputStream());
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.peekOutputStream()).thenReturn(dataOutputStream);
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
    when(connectionMetadata.peekOutputStream()).thenReturn(dataOutputStream);
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
}
