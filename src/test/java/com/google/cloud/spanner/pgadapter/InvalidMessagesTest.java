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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import io.opentelemetry.api.OpenTelemetry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InvalidMessagesTest extends AbstractMockServerTest {
  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        "d",
        configurator -> {},
        OpenTelemetry.noop());
  }

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
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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
    } catch (SocketException ignore) {
      // Sending non-sense messages sometimes fails the entire socket.
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
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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

  @Test
  public void testCreateInvalidPreparedStatement() throws IOException {
    // This test verifies that PGAdapter returns a valid error message if a client
    // 1. Creates a named prepared statement that uses an invalid SQL string and that contains at
    //    least one query parameter.
    // 2. Then tries to execute the prepared statement, and includes only values and no types for
    //    the query parameter values.
    //
    // This is a valid message flow, but one that is not really supported by any higher-level
    // clients. It is however something that can be executed by applications that use pqlib
    // directly. The above message flow would trigger a
    // 'Statement result cannot be retrieved before flush/sync' error.

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Request startup.
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
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

        String invalidSql = "select * from non_existing_table where id=$1";
        mockSpanner.putStatementResult(
            StatementResult.exception(
                Statement.of(invalidSql),
                Status.NOT_FOUND
                    .withDescription("Table non_existing_table not found")
                    .asRuntimeException()));
        String name = "my_statement";

        // Create a named prepared statement.
        // PARSE
        outputStream.writeByte('P');
        outputStream.writeInt(4 + 1 + invalidSql.getBytes(StandardCharsets.UTF_8).length + 1 + 2);
        outputStream.write(name.getBytes(StandardCharsets.UTF_8));
        outputStream.writeByte(0); // String terminator for the named statement
        outputStream.write(invalidSql.getBytes(StandardCharsets.UTF_8));
        outputStream.writeByte(0);
        outputStream.writeShort(0);
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

        // Now try to use the prepared statement with a BIND-DESCRIBE-EXECUTE flow.

        // BIND
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        outputStream.writeByte('B');
        outputStream.writeInt(4 + 1 + 1 + 2 + 2 + 2 + value.length + 1);
        outputStream.writeByte(0); // Empty string terminator for the unnamed portal
        outputStream.write(name.getBytes(StandardCharsets.UTF_8));
        outputStream.writeByte(0); // String terminator for the named statement.
        outputStream.writeShort(0); // Zero parameter format codes
        outputStream.writeShort(1); // One parameter value
        outputStream.writeInt(value.length + 1);
        outputStream.write(value);
        outputStream.writeByte(0);
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

        // Verify that we get an error.
        // Wait for 'Z' (ready for query)
        String errorMessage = "";
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          byte[] contents = new byte[length - 4];
          inputStream.readFully(contents);
          if (message == 'Z') {
            break;
          } else if (message == 'E') {
            byte[] errorMessageBytes = Arrays.copyOfRange(contents, 15, contents.length - 2);
            errorMessage = new String(errorMessageBytes, StandardCharsets.UTF_8);
          }
        }

        // Verify that we received the messages that we sent.
        List<WireMessage> messages = getWireMessages();
        // Startup-Parse-Sync-Bind-Describe-Execute-Flush-Sync.
        assertEquals(8, messages.size());
        // Verify that we received the expected error message.
        assertEquals(
            "Table non_existing_table not found - Statement: 'select * from non_existing_table where id=$1'",
            errorMessage);
      }
    }
  }

  @Test
  public void testPreparedStatementReturningWithConcurrentSchemaChange() throws IOException {
    String sql = "UPDATE t SET v = 'new_val' WHERE id = 1 RETURNING *";
    com.google.spanner.v1.ResultSet sevenColumnsResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                createMetadata(
                    ImmutableList.of(
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64),
                    ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7")))
            .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("1").build())
                    .addValues(Value.newBuilder().setStringValue("v2").build())
                    .addValues(Value.newBuilder().setStringValue("3").build())
                    .addValues(Value.newBuilder().setStringValue("v4").build())
                    .addValues(Value.newBuilder().setStringValue("5").build())
                    .addValues(Value.newBuilder().setStringValue("v6").build())
                    .addValues(Value.newBuilder().setStringValue("7").build())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), sevenColumnsResultSet));

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // 1. Startup message.
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        // Drain startup response until ReadyForQuery ('Z')
        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        String statementName = "s1";
        String portalName = "p1";
        byte[] statementNameBytes = statementName.getBytes(StandardCharsets.UTF_8);
        byte[] portalNameBytes = portalName.getBytes(StandardCharsets.UTF_8);
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);

        // 2. PARSE statement s1
        outputStream.writeByte('P');
        outputStream.writeInt(4 + statementNameBytes.length + 1 + sqlBytes.length + 1 + 2);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.write(sqlBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0); // 0 parameter types

        // DESCRIBE statement s1
        outputStream.writeByte('D');
        outputStream.writeInt(4 + 1 + statementNameBytes.length + 1);
        outputStream.writeByte('S');
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);

        // BIND portal p1 with 7 result format codes (matching initial 7 columns)
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 7 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0); // 0 parameter format codes
        outputStream.writeShort(0); // 0 parameter values
        outputStream.writeShort(7); // 7 result format codes
        for (int i = 0; i < 7; i++) {
          outputStream.writeShort(0); // text format code
        }

        // EXECUTE portal p1
        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0); // return all rows

        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        // Verify initial response:
        // '1' ParseComplete
        assertEquals('1', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        // 't' ParameterDescription
        assertEquals('t', inputStream.readByte());
        int paramDescLength = inputStream.readInt();
        inputStream.readFully(new byte[paramDescLength - 4]);

        // 'T' RowDescription (7 columns)
        assertEquals('T', inputStream.readByte());
        int rowDescLength = inputStream.readInt();
        short numFields = inputStream.readShort();
        assertEquals(7, numFields);
        inputStream.readFully(new byte[rowDescLength - 4 - 2]);

        // '2' BindComplete
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        // 'D' DataRow (7 columns)
        assertEquals('D', inputStream.readByte());
        int dataRowLength = inputStream.readInt();
        short numDataCols = inputStream.readShort();
        assertEquals(7, numDataCols);
        inputStream.readFully(new byte[dataRowLength - 4 - 2]);

        // 'C' CommandComplete ("UPDATE 1")
        assertEquals('C', inputStream.readByte());
        int cmdCompleteLength = inputStream.readInt();
        byte[] cmdCompleteBytes = new byte[cmdCompleteLength - 4];
        inputStream.readFully(cmdCompleteBytes);
        assertEquals(
            "UPDATE 1",
            new String(cmdCompleteBytes, 0, cmdCompleteBytes.length - 1, StandardCharsets.UTF_8));

        // 'Z' ReadyForQuery
        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());

        // 3. Schema change! Another session added a column 'c8'.
        // MockSpanner now returns 8 columns for this SQL:
        com.google.spanner.v1.ResultSet eightColumnsResultSet =
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING),
                        ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("v2").build())
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .addValues(Value.newBuilder().setStringValue("v4").build())
                        .addValues(Value.newBuilder().setStringValue("5").build())
                        .addValues(Value.newBuilder().setStringValue("v6").build())
                        .addValues(Value.newBuilder().setStringValue("7").build())
                        .addValues(Value.newBuilder().setStringValue("new_col_val").build())
                        .build())
                .build();
        mockSpanner.putStatementResult(
            StatementResult.query(Statement.of(sql), eightColumnsResultSet));

        // 4. Client re-executes prepared statement s1.
        // Client still specifies 7 result format codes in Bind because it cached 7 columns.
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 7 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0); // 0 parameter format codes
        outputStream.writeShort(0); // 0 parameter values
        outputStream.writeShort(7); // 7 result format codes (for 8 actual columns!)
        for (int i = 0; i < 7; i++) {
          outputStream.writeShort(0);
        }

        // EXECUTE portal p1
        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0);

        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        // 5. Verify re-execution:
        // Must succeed without throwing ArrayIndexOutOfBoundsException or returning ErrorResponse!
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        // 'D' DataRow should have 8 columns
        assertEquals('D', inputStream.readByte());
        int secondDataRowLength = inputStream.readInt();
        short secondNumDataColumns = inputStream.readShort();
        assertEquals(8, secondNumDataColumns);
        for (int col = 0; col < 8; col++) {
          int valueLength = inputStream.readInt();
          byte[] valueBytes = new byte[valueLength];
          inputStream.readFully(valueBytes);
          if (col == 7) {
            assertEquals("new_col_val", new String(valueBytes, StandardCharsets.UTF_8));
          }
        }

        // 'C' CommandComplete
        assertEquals('C', inputStream.readByte());
        int secondCommandCompleteLength = inputStream.readInt();
        byte[] secondCommandCompleteBytes = new byte[secondCommandCompleteLength - 4];
        inputStream.readFully(secondCommandCompleteBytes);
        assertEquals(
            "UPDATE 1",
            new String(
                secondCommandCompleteBytes,
                0,
                secondCommandCompleteBytes.length - 1,
                StandardCharsets.UTF_8));

        // 'Z' ReadyForQuery
        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());

        // 6. Describe statement s1 against Spanner now reflects the updated 8 columns.
        outputStream.writeByte('D');
        outputStream.writeInt(4 + 1 + statementNameBytes.length + 1);
        outputStream.writeByte('S');
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);

        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        // 't' ParameterDescription
        assertEquals('t', inputStream.readByte());
        int secondParameterDescriptionLength = inputStream.readInt();
        inputStream.readFully(new byte[secondParameterDescriptionLength - 4]);

        // 'T' RowDescription should now describe 8 columns!
        assertEquals('T', inputStream.readByte());
        int secondRowDescriptionLength = inputStream.readInt();
        short secondNumFields = inputStream.readShort();
        assertEquals(8, secondNumFields);
        inputStream.readFully(new byte[secondRowDescriptionLength - 4 - 2]);

        // 'Z' ReadyForQuery
        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());
      }
    }
  }

  @Test
  public void testPreparedStatementReturning_MixedFormatCodes_OutOfBoundsHandledSafely()
      throws IOException {
    String sql = "UPDATE t2 SET v = 'new_val' WHERE id = 1 RETURNING *";
    com.google.spanner.v1.ResultSet eightColumnsResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                createMetadata(
                    ImmutableList.of(
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING),
                    ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")))
            .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("100").build())
                    .addValues(Value.newBuilder().setStringValue("v2").build())
                    .addValues(Value.newBuilder().setStringValue("300").build())
                    .addValues(Value.newBuilder().setStringValue("v4").build())
                    .addValues(Value.newBuilder().setStringValue("500").build())
                    .addValues(Value.newBuilder().setStringValue("v6").build())
                    .addValues(Value.newBuilder().setStringValue("700").build())
                    .addValues(Value.newBuilder().setStringValue("v8").build())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), eightColumnsResultSet));

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Startup
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        String statementName = "s2";
        String portalName = "p2";
        byte[] statementNameBytes = statementName.getBytes(StandardCharsets.UTF_8);
        byte[] portalNameBytes = portalName.getBytes(StandardCharsets.UTF_8);
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);

        // PARSE
        outputStream.writeByte('P');
        outputStream.writeInt(4 + statementNameBytes.length + 1 + sqlBytes.length + 1 + 2);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.write(sqlBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);

        // BIND with 7 format codes: {1, 0, 1, 0, 1, 0, 1}
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 7 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0); // param format codes
        outputStream.writeShort(0); // param values
        outputStream.writeShort(7); // 7 result format codes
        outputStream.writeShort(1); // binary for c1 (INT64)
        outputStream.writeShort(0); // text for c2 (STRING)
        outputStream.writeShort(1); // binary for c3 (INT64)
        outputStream.writeShort(0); // text for c4 (STRING)
        outputStream.writeShort(1); // binary for c5 (INT64)
        outputStream.writeShort(0); // text for c6 (STRING)
        outputStream.writeShort(1); // binary for c7 (INT64)
        // Note: c8 has no format code specified (index 7 out of bounds of format codes)

        // EXECUTE
        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0);

        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        // Responses:
        // '1' ParseComplete
        assertEquals('1', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        // '2' BindComplete
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        // 'D' DataRow (8 columns)
        assertEquals('D', inputStream.readByte());
        int dataRowLength = inputStream.readInt();
        short numDataColumns = inputStream.readShort();
        assertEquals(8, numDataColumns);

        // Col 0 (c1): binary format -> 8 bytes, int64 100L
        int column0Length = inputStream.readInt();
        assertEquals(8, column0Length);
        assertEquals(100L, inputStream.readLong());

        // Col 1 (c2): text format -> 2 bytes, "v2"
        int column1Length = inputStream.readInt();
        byte[] column1Bytes = new byte[column1Length];
        inputStream.readFully(column1Bytes);
        assertEquals("v2", new String(column1Bytes, StandardCharsets.UTF_8));

        // Col 2 (c3): binary format -> 8 bytes, int64 300L
        int column2Length = inputStream.readInt();
        assertEquals(8, column2Length);
        assertEquals(300L, inputStream.readLong());

        // Col 3 (c4): text format -> 2 bytes, "v4"
        int column3Length = inputStream.readInt();
        byte[] column3Bytes = new byte[column3Length];
        inputStream.readFully(column3Bytes);
        assertEquals("v4", new String(column3Bytes, StandardCharsets.UTF_8));

        // Col 4 (c5): binary format -> 8 bytes, int64 500L
        int column4Length = inputStream.readInt();
        assertEquals(8, column4Length);
        assertEquals(500L, inputStream.readLong());

        // Col 5 (c6): text format -> 2 bytes, "v6"
        int column5Length = inputStream.readInt();
        byte[] column5Bytes = new byte[column5Length];
        inputStream.readFully(column5Bytes);
        assertEquals("v6", new String(column5Bytes, StandardCharsets.UTF_8));

        // Col 6 (c7): binary format -> 8 bytes, int64 700L
        int column6Length = inputStream.readInt();
        assertEquals(8, column6Length);
        assertEquals(700L, inputStream.readLong());

        // Col 7 (c8): fallback to text format (0) -> 2 bytes, "v8"
        int column7Length = inputStream.readInt();
        byte[] column7Bytes = new byte[column7Length];
        inputStream.readFully(column7Bytes);
        assertEquals("v8", new String(column7Bytes, StandardCharsets.UTF_8));

        // 'C' CommandComplete
        assertEquals('C', inputStream.readByte());
        int commandCompleteLength = inputStream.readInt();
        byte[] commandCompleteBytes = new byte[commandCompleteLength - 4];
        inputStream.readFully(commandCompleteBytes);
        assertEquals(
            "UPDATE 1",
            new String(
                commandCompleteBytes, 0, commandCompleteBytes.length - 1, StandardCharsets.UTF_8));

        // 'Z' ReadyForQuery
        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());
      }
    }
  }

  @Test
  public void testPreparedStatementSelect_SchemaExpansionHandledSafely() throws IOException {
    String sql = "SELECT * FROM users_expansion WHERE id = 1";
    com.google.spanner.v1.ResultSet sevenColumnsResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                createMetadata(
                    ImmutableList.of(
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64),
                    ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7")))
            .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("1").build())
                    .addValues(Value.newBuilder().setStringValue("v2").build())
                    .addValues(Value.newBuilder().setStringValue("3").build())
                    .addValues(Value.newBuilder().setStringValue("v4").build())
                    .addValues(Value.newBuilder().setStringValue("5").build())
                    .addValues(Value.newBuilder().setStringValue("v6").build())
                    .addValues(Value.newBuilder().setStringValue("7").build())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), sevenColumnsResultSet));

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Startup
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        String statementName = "s_select";
        String portalName = "p_select";
        byte[] statementNameBytes = statementName.getBytes(StandardCharsets.UTF_8);
        byte[] portalNameBytes = portalName.getBytes(StandardCharsets.UTF_8);
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);

        // PARSE
        outputStream.writeByte('P');
        outputStream.writeInt(4 + statementNameBytes.length + 1 + sqlBytes.length + 1 + 2);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.write(sqlBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);

        // BIND with 7 format codes (all text)
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 7 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);
        outputStream.writeShort(0);
        outputStream.writeShort(7);
        for (int index = 0; index < 7; index++) {
          outputStream.writeShort(0);
        }

        // EXECUTE
        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0);

        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        assertEquals('1', inputStream.readByte());
        assertEquals(4, inputStream.readInt());
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        // 'D' DataRow (7 columns)
        assertEquals('D', inputStream.readByte());
        int firstDataRowLength = inputStream.readInt();
        short firstColumnCount = inputStream.readShort();
        assertEquals(7, firstColumnCount);
        inputStream.readFully(new byte[firstDataRowLength - 4 - 2]);

        // 'C' CommandComplete ("SELECT 1")
        assertEquals('C', inputStream.readByte());
        int firstCommandCompleteLength = inputStream.readInt();
        byte[] firstCommandCompleteBytes = new byte[firstCommandCompleteLength - 4];
        inputStream.readFully(firstCommandCompleteBytes);
        assertEquals(
            "SELECT 1",
            new String(
                firstCommandCompleteBytes,
                0,
                firstCommandCompleteBytes.length - 1,
                StandardCharsets.UTF_8));

        // 'Z' ReadyForQuery
        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());

        // Concurrent schema change: table now has 8 columns!
        com.google.spanner.v1.ResultSet eightColumnsResultSet =
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING),
                        ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("v2").build())
                        .addValues(Value.newBuilder().setStringValue("3").build())
                        .addValues(Value.newBuilder().setStringValue("v4").build())
                        .addValues(Value.newBuilder().setStringValue("5").build())
                        .addValues(Value.newBuilder().setStringValue("v6").build())
                        .addValues(Value.newBuilder().setStringValue("7").build())
                        .addValues(Value.newBuilder().setStringValue("v8_added").build())
                        .build())
                .build();
        mockSpanner.putStatementResult(
            StatementResult.query(Statement.of(sql), eightColumnsResultSet));

        // Re-execute prepared statement with 7 result format codes (cached from prior describe)
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 7 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);
        outputStream.writeShort(0);
        outputStream.writeShort(7);
        for (int index = 0; index < 7; index++) {
          outputStream.writeShort(0);
        }

        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0);

        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        // Must succeed with 8 columns, 8th column formatted safely as text (format code 0)
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        assertEquals('D', inputStream.readByte());
        int secondDataRowLength = inputStream.readInt();
        short secondColumnCount = inputStream.readShort();
        assertEquals(8, secondColumnCount);
        for (int columnIndex = 0; columnIndex < 8; columnIndex++) {
          int columnLength = inputStream.readInt();
          byte[] columnBytes = new byte[columnLength];
          inputStream.readFully(columnBytes);
          if (columnIndex == 7) {
            assertEquals("v8_added", new String(columnBytes, StandardCharsets.UTF_8));
          }
        }

        assertEquals('C', inputStream.readByte());
        int secondCommandCompleteLength = inputStream.readInt();
        byte[] secondCommandCompleteBytes = new byte[secondCommandCompleteLength - 4];
        inputStream.readFully(secondCommandCompleteBytes);
        assertEquals(
            "SELECT 1",
            new String(
                secondCommandCompleteBytes,
                0,
                secondCommandCompleteBytes.length - 1,
                StandardCharsets.UTF_8));

        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());
      }
    }
  }

  @Test
  public void testPreparedStatement_SchemaContractionHandledSafely() throws IOException {
    String sql = "SELECT * FROM items_contraction WHERE id = 1";
    com.google.spanner.v1.ResultSet eightColumnsResultSet =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                createMetadata(
                    ImmutableList.of(
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING,
                        TypeCode.INT64,
                        TypeCode.STRING),
                    ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")))
            .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("10").build())
                    .addValues(Value.newBuilder().setStringValue("v2").build())
                    .addValues(Value.newBuilder().setStringValue("30").build())
                    .addValues(Value.newBuilder().setStringValue("v4").build())
                    .addValues(Value.newBuilder().setStringValue("50").build())
                    .addValues(Value.newBuilder().setStringValue("v6").build())
                    .addValues(Value.newBuilder().setStringValue("70").build())
                    .addValues(Value.newBuilder().setStringValue("v8").build())
                    .build())
            .build();
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), eightColumnsResultSet));

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort())) {
      try (DataInputStream inputStream = new DataInputStream(socket.getInputStream());
          DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        // Startup
        outputStream.writeInt(17);
        outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
        outputStream.writeBytes("user");
        outputStream.writeByte(0);
        outputStream.writeBytes("foo");
        outputStream.writeByte(0);
        outputStream.flush();

        while (true) {
          byte message = inputStream.readByte();
          int length = inputStream.readInt();
          inputStream.readFully(new byte[length - 4]);
          if (message == 'Z') {
            break;
          }
        }

        String statementName = "s_contract";
        String portalName = "p_contract";
        byte[] statementNameBytes = statementName.getBytes(StandardCharsets.UTF_8);
        byte[] portalNameBytes = portalName.getBytes(StandardCharsets.UTF_8);
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);

        // PARSE
        outputStream.writeByte('P');
        outputStream.writeInt(4 + statementNameBytes.length + 1 + sqlBytes.length + 1 + 2);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.write(sqlBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);

        // BIND with 8 format codes: {1, 0, 1, 0, 1, 0, 1, 0}
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 8 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);
        outputStream.writeShort(0);
        outputStream.writeShort(8);
        outputStream.writeShort(1);
        outputStream.writeShort(0);
        outputStream.writeShort(1);
        outputStream.writeShort(0);
        outputStream.writeShort(1);
        outputStream.writeShort(0);
        outputStream.writeShort(1);
        outputStream.writeShort(0);

        // EXECUTE
        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0);

        // SYNC
        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        assertEquals('1', inputStream.readByte());
        assertEquals(4, inputStream.readInt());
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        assertEquals('D', inputStream.readByte());
        int firstDataRowLength = inputStream.readInt();
        short firstColumnCount = inputStream.readShort();
        assertEquals(8, firstColumnCount);
        inputStream.readFully(new byte[firstDataRowLength - 4 - 2]);

        assertEquals('C', inputStream.readByte());
        int firstCommandCompleteLength = inputStream.readInt();
        byte[] firstCommandCompleteBytes = new byte[firstCommandCompleteLength - 4];
        inputStream.readFully(firstCommandCompleteBytes);
        assertEquals(
            "SELECT 1",
            new String(
                firstCommandCompleteBytes,
                0,
                firstCommandCompleteBytes.length - 1,
                StandardCharsets.UTF_8));

        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());

        // Schema change: Column 8 is dropped! MockSpanner now returns only 7 columns.
        com.google.spanner.v1.ResultSet sevenColumnsResultSet =
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64,
                            TypeCode.STRING,
                            TypeCode.INT64),
                        ImmutableList.of("c1", "c2", "c3", "c4", "c5", "c6", "c7")))
                .setStats(ResultSetStats.newBuilder().setRowCountExact(1L).build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("20").build())
                        .addValues(Value.newBuilder().setStringValue("v2").build())
                        .addValues(Value.newBuilder().setStringValue("40").build())
                        .addValues(Value.newBuilder().setStringValue("v4").build())
                        .addValues(Value.newBuilder().setStringValue("60").build())
                        .addValues(Value.newBuilder().setStringValue("v6").build())
                        .addValues(Value.newBuilder().setStringValue("80").build())
                        .build())
                .build();
        mockSpanner.putStatementResult(
            StatementResult.query(Statement.of(sql), sevenColumnsResultSet));

        // Re-execute prepared statement with 8 format codes (more format codes than columns in
        // result)
        outputStream.writeByte('B');
        outputStream.writeInt(
            4 + portalNameBytes.length + 1 + statementNameBytes.length + 1 + 2 + 2 + 2 + 8 * 2);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.write(statementNameBytes);
        outputStream.writeByte(0);
        outputStream.writeShort(0);
        outputStream.writeShort(0);
        outputStream.writeShort(8);
        outputStream.writeShort(1);
        outputStream.writeShort(0);
        outputStream.writeShort(1);
        outputStream.writeShort(0);
        outputStream.writeShort(1);
        outputStream.writeShort(0);
        outputStream.writeShort(1);
        outputStream.writeShort(0);

        outputStream.writeByte('E');
        outputStream.writeInt(4 + portalNameBytes.length + 1 + 4);
        outputStream.write(portalNameBytes);
        outputStream.writeByte(0);
        outputStream.writeInt(0);

        outputStream.writeByte('S');
        outputStream.writeInt(4);
        outputStream.flush();

        // Must succeed with 7 columns without any errors
        assertEquals('2', inputStream.readByte());
        assertEquals(4, inputStream.readInt());

        assertEquals('D', inputStream.readByte());
        int secondDataRowLength = inputStream.readInt();
        short secondColumnCount = inputStream.readShort();
        assertEquals(7, secondColumnCount);

        // Verify format codes for the 7 columns:
        // Col 0: binary format (int64 20L)
        assertEquals(8, inputStream.readInt());
        assertEquals(20L, inputStream.readLong());

        // Col 1: text format ("v2")
        int col1Length = inputStream.readInt();
        byte[] col1Bytes = new byte[col1Length];
        inputStream.readFully(col1Bytes);
        assertEquals("v2", new String(col1Bytes, StandardCharsets.UTF_8));

        // Col 2: binary format (int64 40L)
        assertEquals(8, inputStream.readInt());
        assertEquals(40L, inputStream.readLong());

        // Col 3: text format ("v4")
        int col3Length = inputStream.readInt();
        byte[] col3Bytes = new byte[col3Length];
        inputStream.readFully(col3Bytes);
        assertEquals("v4", new String(col3Bytes, StandardCharsets.UTF_8));

        // Col 4: binary format (int64 60L)
        assertEquals(8, inputStream.readInt());
        assertEquals(60L, inputStream.readLong());

        // Col 5: text format ("v6")
        int col5Length = inputStream.readInt();
        byte[] col5Bytes = new byte[col5Length];
        inputStream.readFully(col5Bytes);
        assertEquals("v6", new String(col5Bytes, StandardCharsets.UTF_8));

        // Col 6: binary format (int64 80L)
        assertEquals(8, inputStream.readInt());
        assertEquals(80L, inputStream.readLong());

        assertEquals('C', inputStream.readByte());
        int secondCommandCompleteLength = inputStream.readInt();
        byte[] secondCommandCompleteBytes = new byte[secondCommandCompleteLength - 4];
        inputStream.readFully(secondCommandCompleteBytes);
        assertEquals(
            "SELECT 1",
            new String(
                secondCommandCompleteBytes,
                0,
                secondCommandCompleteBytes.length - 1,
                StandardCharsets.UTF_8));

        assertEquals('Z', inputStream.readByte());
        assertEquals(5, inputStream.readInt());
        assertEquals('I', inputStream.readByte());
      }
    }
  }
}
