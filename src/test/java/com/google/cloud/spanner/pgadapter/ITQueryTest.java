// Copyright 2021 Google LLC
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

import static com.google.cloud.spanner.pgadapter.ProxyServer.ServerStatus;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public final class ITQueryTest implements IntegrationTest {
  private static final Logger logger = Logger.getLogger(ITQueryTest.class.getName());

  private ProxyServer server;
  private PgAdapterTestEnv testEnv = new PgAdapterTestEnv();

  private static final int PROTOCOL_MESSAGE_SIZE = 8;
  private static final int INIT_PROTOCOL = 80877103;
  private static final int OPTIONS_PROTOCOL = 196608;

  @Before
  public void setUp() throws Exception {
    testEnv.setUp();

    String[] args = {
      "-p",
      testEnv.getProjectId(),
      "-i",
      testEnv.getInstanceId(),
      "-d",
      testEnv.getDatabaseId(),
      "-c",
      testEnv.getCredentials(),
      "-s",
      String.valueOf(testEnv.getPort()),
      "-f",
      TextFormat.POSTGRESQL.toString(),
      "-e",
      "staging-wrenchworks.sandbox.googleapis.com"
    };
    server = new ProxyServer(new OptionsMetadata(args));
    server.startServer();
  }

  private void waitForServer() throws Exception {
    // Wait for up to 1 second if server has not started.
    for (int i = 0; i < 10; ++i) {
      if (server.getServerStatus() == ServerStatus.STARTED) {
        return;
      }
      Thread.sleep(100);
    }
    // Throw exception if server has still not started.
    throw new IllegalStateException("ProxyServer failed to start.");
  }

  private void initializeConnection(DataOutputStream out) throws Exception {
    // Send start message.
    {
      byte[] metadata =
          ByteBuffer.allocate(PROTOCOL_MESSAGE_SIZE)
              .putInt(PROTOCOL_MESSAGE_SIZE)
              .putInt(INIT_PROTOCOL)
              .array();
      out.write(metadata, 0, metadata.length);
    }

    // Send options.
    {
      String payload =
          "user\0"
              + System.getProperty("user.name")
              + "\0database\0"
              + testEnv.getDatabaseId()
              + "\0client_encoding\0UTF8\0DateStyle\0ISO\0TimeZone\0America/Los_Angeles\0extra_float_digits\0"
              + "2\0\0";
      byte[] metadata =
          ByteBuffer.allocate(PROTOCOL_MESSAGE_SIZE)
              .putInt(payload.length() + PROTOCOL_MESSAGE_SIZE)
              .putInt(OPTIONS_PROTOCOL)
              .array();
      byte[] message = Bytes.concat(metadata, payload.getBytes());
      out.write(message, 0, message.length);
    }
    logger.log(Level.INFO, "Connected to database " + testEnv.getDatabaseId());
  }

  class PGMessage {
    public PGMessage(char type, byte[] payload) {
      this.type = type;
      this.payload = payload;
    }

    public char getType() {
      return type;
    }

    public byte[] getPayload() {
      return payload;
    }

    public String toString() {
      return "Type: " + type + " Payload: " + Arrays.toString(payload);
    }

    char type;
    byte[] payload;
  }

  protected PGMessage consumePGMessage(char expectedType, DataInputStream in)
      throws java.io.IOException {
    char type = (char) in.readByte();
    assertThat(type, is(equalTo(expectedType)));
    int length = in.readInt();
    // The length of payload is total length - bytes to express length (4 bytes)
    byte[] payload = new byte[length - 4];
    in.readFully(payload);
    return new PGMessage(type, payload);
  }

  protected void consumeStartUpMessages(DataInputStream in) throws java.io.IOException {
    // Get result from initialization request. Skip first byte since it is metadata ('N' character).
    assertThat((char) in.readByte(), is(equalTo('N')));

    // Check for correct message type identifiers.
    // See https://www.postgresql.org/docs/13/protocol-message-formats.html for more details.
    consumePGMessage('R', in); // AuthenticationOk
    consumePGMessage('K', in); // BackendKeyData
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('S', in); // ParameterStatus
    consumePGMessage('Z', in); // ReadyForQuery
  }

  @Test
  public void simplePgQuery() throws Exception {
    waitForServer();

    Socket clientSocket = new Socket(InetAddress.getByName(null), testEnv.getPort());
    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
    DataInputStream in = new DataInputStream(clientSocket.getInputStream());
    initializeConnection(out);

    consumeStartUpMessages(in);
    // Run a query that is PG specific to ensure this is a PG dialect database.
    String payload = "SELECT 42::int8\0";
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) (payload.length() + 4)};
    byte[] message = Bytes.concat(messageMetadata, payload.getBytes());
    out.write(message, 0, message.length);

    // Check for correct message type identifiers. Exactly 1 row should be returned.
    // See https://www.postgresql.org/docs/13/protocol-message-formats.html for more details.
    consumePGMessage('T', in);
    PGMessage dataRow = consumePGMessage('D', in);
    consumePGMessage('C', in);
    consumePGMessage('Z', in);

    // Check data returned payload
    // see here: https://www.postgresql.org/docs/13/protocol-message-formats.html
    DataInputStream dataRowIn = new DataInputStream(new ByteArrayInputStream(dataRow.getPayload()));
    // Number of column values.
    assertThat((int) dataRowIn.readShort(), is(equalTo(1)));
    // Column value length (2 bytes expected)
    assertThat(dataRowIn.readInt(), is(equalTo(2)));
    // Value of the column: '42'
    assertThat(dataRowIn.readByte(), is(equalTo((byte) '4')));
    assertThat(dataRowIn.readByte(), is(equalTo((byte) '2')));
  }

  @Test
  public void basicSelectTest() throws Exception {
    waitForServer();

    Socket clientSocket = new Socket(InetAddress.getByName(null), testEnv.getPort());
    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
    DataInputStream in = new DataInputStream(clientSocket.getInputStream());
    initializeConnection(out);

    consumeStartUpMessages(in);
    // Send query.
    String payload = "SELECT id, name, age FROM users\0";
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) (payload.length() + 4)};

    byte[] message = Bytes.concat(messageMetadata, payload.getBytes());
    out.write(message, 0, message.length);

    // Check for correct message type identifiers. Exactly 3 rows should be returned.
    // See https://www.postgresql.org/docs/13/protocol-message-formats.html for more details.
    PGMessage rowDescription = consumePGMessage('T', in);
    PGMessage[] dataRows = {
      consumePGMessage('D', in), consumePGMessage('D', in), consumePGMessage('D', in)
    };
    PGMessage commandComplete = consumePGMessage('C', in);
    PGMessage readyForQuery = consumePGMessage('Z', in);

    // Check RowDescription payload
    // see here: https://www.postgresql.org/docs/13/protocol-message-formats.html
    DataInputStream rowDescIn =
        new DataInputStream(new ByteArrayInputStream(rowDescription.getPayload()));
    short fieldCount = rowDescIn.readShort();
    assertThat(fieldCount, is(equalTo((short) 3)));
    for (String expectedFieldName : new String[] {"id", "name", "age"}) {
      // Read a null-terminated string.
      StringBuilder builder = new StringBuilder("");
      byte b;
      while ((b = rowDescIn.readByte()) != (byte) 0) {
        builder.append((char) b);
      }
      String fieldName = builder.toString();
      assertThat(fieldName, is(equalTo(expectedFieldName)));
      byte[] unusedBytes = new byte[18];
      rowDescIn.readFully(unusedBytes);
    }

    // Check exact message results.
    byte[] rowDescriptionData = {
      0, 3, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 8, 0, 0, 0, 0, 0, 0, 110, 97, 109, 101,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, 0, 0, 0, 0, 0, 0, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 20, 0, 8, 0, 0, 0, 0, 0, 0
    };
    byte[] dataRow0 = {0, 3, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49};
    byte[] dataRow1 = {0, 3, 0, 0, 0, 1, 50, 0, 0, 0, 3, 74, 111, 101, 0, 0, 0, 2, 50, 48};
    byte[] dataRow2 = {0, 3, 0, 0, 0, 1, 51, 0, 0, 0, 4, 74, 97, 99, 107, 0, 0, 0, 2, 50, 51};
    byte[] commandCompleteData = {83, 69, 76, 69, 67, 84, 32, 51, 0};
    byte[] readyForQueryData = {73};

    assertThat(rowDescription.getPayload(), is(equalTo(rowDescriptionData)));
    assertThat(dataRows[0].getPayload(), is(equalTo(dataRow0)));
    assertThat(dataRows[1].getPayload(), is(equalTo(dataRow1)));
    assertThat(dataRows[2].getPayload(), is(equalTo(dataRow2)));
    assertThat(commandComplete.getPayload(), is(equalTo(commandCompleteData)));
    assertThat(readyForQuery.getPayload(), is(equalTo(readyForQueryData)));
  }
}
