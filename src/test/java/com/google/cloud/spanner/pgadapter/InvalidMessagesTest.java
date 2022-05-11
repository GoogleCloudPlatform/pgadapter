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

import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InvalidMessagesTest extends AbstractMockServerTest {

  @Test
  public void testConnectionWithoutMessages() throws IOException {
    try (Socket ignored = new Socket("localhost", pgServer.getLocalPort())) {
      // Do nothing, just close the socket again.
    }
  }

  @Test
  public void testGarbledStartupMessage() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      socket.getOutputStream().write("foo".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testDropConnectionAfterStartup() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Send a startup message and then quit.
        outputStream.writeInt(8); // length == 8
        outputStream.writeInt(StartupMessage.IDENTIFIER);
        outputStream.flush();
      }
    }
  }

  @Test
  public void testDropConnectionAfterRefusedSSL() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Request SSL.
        outputStream.writeInt(8); // length == 8
        outputStream.writeInt(SSLMessage.IDENTIFIER);
        outputStream.flush();

        // Verify that it is refused by the server.
        byte response = inputStream.readByte();
        assertEquals('N', response);
      }
    }
  }

  @Test
  public void testDropConnectionAfterStartupMessage() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Request startup.
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        // Verify that the server responds with auth OK.
        assertEquals('R', inputStream.readByte());
        assertEquals(8, inputStream.readInt());
        assertEquals(0, inputStream.readInt()); // 0 == success
      }
    }
  }

  @Test
  public void testSendGarbageAfterStartupMessage() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Request startup.
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        // Then send a random message with no meaning and drop the connection.
        outputStream.writeInt(20);
        outputStream.writeChar(' ');
        outputStream.flush();

        // Read until the end of the stream. The stream should be closed by the backend.
        int bytesRead = 0;
        while (inputStream.read() > -1 && bytesRead < 1 << 16) {
          bytesRead++;
        }
        assertEquals(-1, inputStream.read());
      }
    }
  }

  @Test
  public void testProtectionAgainstMessageLength() throws IOException {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Request startup.
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        // Send a message that claims to be very large.
        // This will cause the backend to terminate the connection to protect itself.
        outputStream.writeByte('Q');
        outputStream.writeInt(Integer.MAX_VALUE);
        outputStream.writeUTF("select foo from bar");
        outputStream.writeByte(0);
        outputStream.flush();

        // Read until the end of the stream. The stream should be closed by the backend.
        int bytesRead = 0;
        while (inputStream.read() > -1 && bytesRead < 1 << 16) {
          bytesRead++;
        }
        assertEquals(-1, inputStream.read());
      }
    }
  }
}
