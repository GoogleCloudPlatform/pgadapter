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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectionHandlerTest {

  @Ignore
  @Test
  public void testTerminateClosesSocket() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    SocketChannel socket = mock(SocketChannel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    when(socket.getRemoteAddress()).thenReturn(socketAddress);
    //    InetAddress address = mock(InetAddress.class);
    //    when(socketAddress.getAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    verify(socket).close();
  }

  @Ignore
  @Test
  public void testTerminateDoesNotCloseSocketTwice() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    //    Socket socket = mock(Socket.class);
    //    when(socket.isClosed()).thenReturn(false, true);
    //    InetAddress address = mock(InetAddress.class);
    //    when(socket.getInetAddress()).thenReturn(address);

    SocketChannel socket = mock(SocketChannel.class);
    when(socket.isConnected()).thenReturn(true, false);
    InetSocketAddress socketAddress = mock(InetSocketAddress.class);
    when(socket.getRemoteAddress()).thenReturn(socketAddress);
    InetAddress address = mock(InetAddress.class);
    when(socketAddress.getAddress()).thenReturn(address);

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    // Calling terminate a second time should be a no-op.
    connection.terminate();

    // Verify that close was only called once.
    verify(socket).close();
  }

  @Ignore
  @Test
  public void testTerminateHandlesCloseError() throws IOException {
    ProxyServer server = mock(ProxyServer.class);
    SocketChannel socket = mock(SocketChannel.class);
    InetSocketAddress socketAddress = mock(InetSocketAddress.class);
    when(socket.getRemoteAddress()).thenReturn(socketAddress);
    InetAddress address = mock(InetAddress.class);
    when(socketAddress.getAddress()).thenReturn(address);
    // IOException should be handled internally in terminate().
    doThrow(new IOException("test exception")).when(socket).close();

    ConnectionHandler connection = new ConnectionHandler(server, socket);

    connection.terminate();
    verify(socket).close();
  }
}
