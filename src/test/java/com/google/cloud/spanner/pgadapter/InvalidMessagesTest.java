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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
        try {
          int bytesRead = 0;
          while (inputStream.read() > -1 && bytesRead < 1 << 16) {
            bytesRead++;
          }
          assertEquals(-1, inputStream.read());
        } catch (IOException ignore) {
        }
      }
    }
  }

  @Test
  public void testFlushAndSync() throws IOException {
    // This test verifies that PGAdapter will treat a Flush directly followed by a Sync messages as
    // if it was just a Sync message. Sending Flush and then Sync directly after each other is not
    // very useful, as Sync means 'flush and commit' (i.e. it already entails Flush).

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

        // Receive key data.
        assertEquals('K', inputStream.readByte());
        assertEquals(12, inputStream.readInt());
        inputStream.readInt();
        inputStream.readInt();

        // Just skip parameter data and wait for 'Z' (ready for query)
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        // Do an extended query round-trip.
        // PARSE
        outputStream.writeByte('P');
        outputStream.writeInt(4 + 1 + "SELECT 1".getBytes(StandardCharsets.UTF_8).length + 1 + 2);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.write("SELECT 1".getBytes(StandardCharsets.UTF_8));
        outputStream.writeByte(0);
        outputStream.writeShort(0);
        // BIND
        outputStream.writeByte('B');
        outputStream.writeInt(4 + 1 + 1 + 2 + 2 + 2);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.writeByte(0); // Empty string terminator for the unnamed prepared statement
        outputStream.writeShort(0); // Zero parameter format codes
        outputStream.writeShort(0); // Zero parameter values
        outputStream.writeShort(0); // Zero result format codes
        // DESCRIBE
        outputStream.writeByte('D');
        outputStream.writeInt(4 + 1 + 1);
        outputStream.writeByte('P');
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        // EXECUTE
        outputStream.writeByte('E');
        outputStream.writeInt(4 + 1 + 4);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.writeInt(0); // Return all rows
        // FLUSH
        outputStream.writeByte('H');
        outputStream.writeInt(4);
        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);

        outputStream.flush();

        // Wait for 'Z' (ready for query)
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        // Verify that we received the messages that we sent.
        List<WireMessage> messages = getWireMessages();
        // Startup-Parse-Bind-Describe-Execute-Flush-Sync.
        assertEquals(7, messages.size());

        // Verify that PGAdapter executed the single query using a single-use read-only transaction.
        // This is achieved because we do a look-ahead in the flush message to check whether the
        // next message is a sync. Otherwise, the flush would cause the backend connection to start
        // an implicit read/write transaction, as we do not know what type of statement might follow
        // after the flush.
        assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
        assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
        ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
        assertTrue(request.getTransaction().hasSingleUse());
        assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
      }
    }
  }

  @Test
  public void testFlushFollowedByQuery() throws IOException {
    // This test verifies that PGAdapter will treat a Flush directly followed by another query as
    // a flush message (i.e. it does not treat it as a Sync).

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

        // Receive key data.
        assertEquals('K', inputStream.readByte());
        assertEquals(12, inputStream.readInt());
        inputStream.readInt();
        inputStream.readInt();

        // Just skip parameter data and wait for 'Z' (ready for query)
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        for (int i = 0; i < 2; i++) {
          // Do an extended query round-trip.
          // PARSE
          outputStream.writeByte('P');
          outputStream.writeInt(4 + 1 + "SELECT 1".getBytes(StandardCharsets.UTF_8).length + 1 + 2);
          outputStream.writeByte(0); // Empty string terminator for the unnamed portal
          outputStream.write("SELECT 1".getBytes(StandardCharsets.UTF_8));
          outputStream.writeByte(0);
          outputStream.writeShort(0);
          // BIND
          outputStream.writeByte('B');
          outputStream.writeInt(4 + 1 + 1 + 2 + 2 + 2);
          outputStream.writeByte(0); // Empty string terminator for the unnamed portal
          outputStream.writeByte(0); // Empty string terminator for the unnamed prepared statement
          outputStream.writeShort(0); // Zero parameter format codes
          outputStream.writeShort(0); // Zero parameter values
          outputStream.writeShort(0); // Zero result format codes
          // DESCRIBE
          outputStream.writeByte('D');
          outputStream.writeInt(4 + 1 + 1);
          outputStream.writeByte('P');
          outputStream.writeByte(0); // Empty string terminator for the unnamed portal
          // EXECUTE
          outputStream.writeByte('E');
          outputStream.writeInt(4 + 1 + 4);
          outputStream.writeByte(0); // Empty string terminator for the unnamed portal
          outputStream.writeInt(0); // Return all rows

          // Do a flush, but not a sync, after the first query.
          if (i == 0) {
            // FLUSH
            outputStream.writeByte('H');
            outputStream.writeInt(4);

            outputStream.flush();

            // Wait until we have received a CommandComplete message.
            while (true) {
              byte message = inputStream.readByte();
              int length = inputStream.readInt();
              inputStream.readFully(new byte[length - 4]);
              if (message == 'C') {
                break;
              }
            }

            // Verify that we received the messages that we sent.
            List<WireMessage> messages = getWireMessages();
            // Startup-Parse-Bind-Describe-Execute-Flush.
            assertEquals(6, messages.size());

            assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
            assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
            ExecuteSqlRequest request =
                mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
            // PGAdapter will start an implicit read/write transaction, because we have sent a
            // flush and more statements may follow, and we have not specified that this transaction
            // will only read.
            assertTrue(request.getTransaction().hasBegin());
            assertTrue(request.getTransaction().getBegin().hasReadWrite());
          }
        }
        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);

        outputStream.flush();

        // Wait for 'Z' (ready for query)
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        // Verify that we received the messages that we sent.
        List<WireMessage> messages = getWireMessages();
        // Startup-Parse-Bind-Describe-Execute-Flush.
        // Parse-Bind-Describe-Execute-Sync.
        assertEquals(11, messages.size());

        // Verify that PGAdapter executed the two queries using a read/write transaction.
        assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
        assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
        ExecuteSqlRequest firstRequest =
            mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
        assertTrue(firstRequest.getTransaction().hasBegin());
        assertTrue(firstRequest.getTransaction().getBegin().hasReadWrite());
        ExecuteSqlRequest secondRequest =
            mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1);
        assertTrue(secondRequest.getTransaction().hasId());
      }
    }
  }

  @Test
  public void testFlushFollowedByEmptyBuffer() throws IOException {
    // This test verifies that PGAdapter will treat a Flush without a message that is sent directly
    // after as a Flush. PGAdapter should not block to try to peek at the next message if there is
    // nothing in the buffer. This means that a Flush followed by a Sync could lead to the use of a
    // read/write transaction if there is a pause between the two.

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

        // Receive key data.
        assertEquals('K', inputStream.readByte());
        assertEquals(12, inputStream.readInt());
        inputStream.readInt();
        inputStream.readInt();

        // Just skip parameter data and wait for 'Z' (ready for query)
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        // Do an extended query round-trip.
        // PARSE
        outputStream.writeByte('P');
        outputStream.writeInt(4 + 1 + "SELECT 1".getBytes(StandardCharsets.UTF_8).length + 1 + 2);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.write("SELECT 1".getBytes(StandardCharsets.UTF_8));
        outputStream.writeByte(0);
        outputStream.writeShort(0);
        // BIND
        outputStream.writeByte('B');
        outputStream.writeInt(4 + 1 + 1 + 2 + 2 + 2);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.writeByte(0); // Empty string terminator for the unnamed prepared statement
        outputStream.writeShort(0); // Zero parameter format codes
        outputStream.writeShort(0); // Zero parameter values
        outputStream.writeShort(0); // Zero result format codes
        // DESCRIBE
        outputStream.writeByte('D');
        outputStream.writeInt(4 + 1 + 1);
        outputStream.writeByte('P');
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        // EXECUTE
        outputStream.writeByte('E');
        outputStream.writeInt(4 + 1 + 4);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.writeInt(0); // Return all rows

        // Do a flush, but not a sync, after the query.
        // FLUSH
        outputStream.writeByte('H');
        outputStream.writeInt(4);

        outputStream.flush();

        // Wait until we have received a CommandComplete message.
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'C') {
            break;
          }
        }

        // Verify that we received the messages that we sent.
        List<WireMessage> messages = getWireMessages();
        // Startup-Parse-Bind-Describe-Execute-Flush.
        assertEquals(6, messages.size());

        assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
        assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
        ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
        // PGAdapter will start an implicit read/write transaction, because we have sent a
        // flush and more statements may follow, and we have not specified that this transaction
        // will only read.
        assertTrue(request.getTransaction().hasBegin());
        assertTrue(request.getTransaction().getBegin().hasReadWrite());
        // The transaction should not yet have committed.
        assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));

        // Now send a sync. This will commit the implicit read/write transaction.
        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);

        outputStream.flush();

        // Wait for 'Z' (ready for query)
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        // Verify that we received the messages that we sent.
        messages = getWireMessages();
        // Startup-Parse-Bind-Describe-Execute-Flush-(pause)-Sync.
        assertEquals(7, messages.size());

        // Verify that PGAdapter committed the implicit transaction.
        assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
      }
    }
  }
}
