// Copyright 2026 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WireMessageTest {

  private static class TestWireMessage extends WireMessage {
    TestWireMessage(ConnectionHandler connection, int length) {
      super(connection, length);
    }

    @Override
    protected void sendPayload() {}

    @Override
    protected String getMessageName() {
      return "Test";
    }

    @Override
    protected String getPayloadString() {
      return "";
    }

    @Override
    public String getIdentifier() {
      return "T";
    }
  }

  private ConnectionHandler createConnectionHandler(InputStream stream) {
    ConnectionHandler connection = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    when(connection.getServer()).thenReturn(server);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(server.getOptions()).thenReturn(options);
    when(server.getMessageReader()).thenReturn(mock(MessageReader.class));
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(connection.getConnectionMetadata()).thenReturn(metadata);

    DataInputStream inputStream = new DataInputStream(stream);
    DataOutputStream outputStream = new DataOutputStream(new ByteArrayOutputStream());
    when(metadata.getInputStream()).thenReturn(inputStream);
    when(metadata.getOutputStream()).thenReturn(outputStream);
    return connection;
  }

  private ConnectionHandler createConnectionHandler(byte[] inputBytes) {
    return createConnectionHandler(new ByteArrayInputStream(inputBytes));
  }

  @Test
  public void testRead_invalidLengthZero() {
    ConnectionHandler connection = createConnectionHandler(new byte[0]);
    TestWireMessage message = new TestWireMessage(connection, 4);

    IOException exception = assertThrows(IOException.class, () -> message.read(0));
    assertTrue(exception.getMessage().contains("Invalid message length"));
  }

  @Test
  public void testRead_invalidLengthNegative() {
    ConnectionHandler connection = createConnectionHandler(new byte[0]);
    TestWireMessage message = new TestWireMessage(connection, 4);

    IOException exception = assertThrows(IOException.class, () -> message.read(-1));
    assertTrue(exception.getMessage().contains("Invalid message length"));
  }

  @Test
  public void testRead_singleNullTerminator() throws IOException {
    ConnectionHandler connection = createConnectionHandler(new byte[] {0});
    TestWireMessage message = new TestWireMessage(connection, 5);

    assertEquals("", message.read(1));
  }

  @Test
  public void testRead_singleByteNotNullTerminated() {
    ConnectionHandler connection = createConnectionHandler(new byte[] {'a'});
    TestWireMessage message = new TestWireMessage(connection, 5);

    IOException exception = assertThrows(IOException.class, () -> message.read(1));
    assertTrue(exception.getMessage().contains("String was not null-terminated"));
  }

  @Test
  public void testRead_validString() throws IOException {
    byte[] bytes = "hello\0".getBytes(StandardCharsets.UTF_8);
    ConnectionHandler connection = createConnectionHandler(bytes);
    TestWireMessage message = new TestWireMessage(connection, 4 + bytes.length);

    assertEquals("hello", message.read(bytes.length));
  }

  @Test
  public void testRead_validStringMissingNullTerminator() {
    byte[] bytes = "hellox".getBytes(StandardCharsets.UTF_8);
    ConnectionHandler connection = createConnectionHandler(bytes);
    TestWireMessage message = new TestWireMessage(connection, 4 + bytes.length);

    IOException exception = assertThrows(IOException.class, () -> message.read(bytes.length));
    assertTrue(exception.getMessage().contains("String was not null-terminated"));
  }

  @Test
  public void testReadAll_emptyRemaining() throws Exception {
    ConnectionHandler connection = createConnectionHandler(new byte[0]);
    TestWireMessage message = new TestWireMessage(connection, 4);

    assertEquals("", message.readAll());
  }

  @Test
  public void testReadAll_negativeRemainingThrows() {
    ConnectionHandler connection = createConnectionHandler(new byte[0]);
    TestWireMessage message =
        new TestWireMessage(connection, 4) {
          @Override
          protected int getHeaderLength() {
            return 8;
          }
        };

    IOException exception = assertThrows(IOException.class, message::readAll);
    assertTrue(
        exception
            .getMessage()
            .contains("Invalid message length: 4 is smaller than header length 8"));
  }

  @Test
  public void testReadString_emptyString() throws IOException {
    ConnectionHandler connection = createConnectionHandler(new byte[] {0});
    TestWireMessage message = new TestWireMessage(connection, 5);

    assertEquals("", message.readString());
  }

  @Test
  public void testReadString_normalString() throws IOException {
    byte[] bytes = "hello world\0".getBytes(StandardCharsets.UTF_8);
    ConnectionHandler connection = createConnectionHandler(bytes);
    TestWireMessage message = new TestWireMessage(connection, 4 + bytes.length);

    assertEquals("hello world", message.readString());
  }

  @Test
  public void testPasswordMessage_lengthTooSmall() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    try {
      dataOutputStream.writeInt(4);
    } catch (IOException ignored) {
    }
    ConnectionHandler connection = createConnectionHandler(outputStream.toByteArray());

    PGException exception =
        assertThrows(
            PGException.class, () -> new PasswordMessage(connection, Collections.emptyMap()));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of password message"));
  }

  @Test
  public void testPasswordMessage_lengthTooLarge() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    try {
      dataOutputStream.writeInt(2_000_000_000);
    } catch (IOException ignored) {
    }
    ConnectionHandler connection = createConnectionHandler(outputStream.toByteArray());

    PGException exception =
        assertThrows(
            PGException.class, () -> new PasswordMessage(connection, Collections.emptyMap()));
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains("invalid length of password message"));
  }

  @Test
  public void testPasswordMessage_valid() throws Exception {
    byte[] passwordBytes = "secret_password\0".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(4 + passwordBytes.length);
    dataOutputStream.write(passwordBytes);

    ConnectionHandler connection = createConnectionHandler(outputStream.toByteArray());
    PasswordMessage passwordMessage = new PasswordMessage(connection, Collections.emptyMap());
    assertEquals("secret_password", passwordMessage.getPassword());
  }

  @Test
  public void testCheckLength_range() {
    WireMessage.checkLengthRange(10, "test", 8, 20);

    PGException tooSmall =
        assertThrows(PGException.class, () -> WireMessage.checkLengthRange(7, "test", 8, 20));
    assertEquals(SQLState.ProtocolViolation, tooSmall.getSQLState());
    assertEquals(Severity.FATAL, tooSmall.getSeverity());
    assertTrue(tooSmall.getMessage().contains("invalid length of test: 7"));

    PGException tooLarge =
        assertThrows(PGException.class, () -> WireMessage.checkLengthRange(21, "test", 8, 20));
    assertEquals(SQLState.ProtocolViolation, tooLarge.getSQLState());
    assertEquals(Severity.FATAL, tooLarge.getSeverity());
    assertTrue(tooLarge.getMessage().contains("invalid length of test: 21"));
  }

  @Test
  public void testCheckLength_exact() {
    WireMessage.checkLengthEquals(8, "test", 8);

    PGException mismatch =
        assertThrows(PGException.class, () -> WireMessage.checkLengthEquals(12, "test", 8));
    assertEquals(SQLState.ProtocolViolation, mismatch.getSQLState());
    assertEquals(Severity.FATAL, mismatch.getSeverity());
    assertTrue(mismatch.getMessage().contains("invalid length of test: 12"));
  }

  @Test
  public void testCheckLength_allowed() {
    WireMessage.checkLengthIn(16, "test", 16, 44);
    WireMessage.checkLengthIn(44, "test", 16, 44);

    PGException notAllowed =
        assertThrows(PGException.class, () -> WireMessage.checkLengthIn(20, "test", 16, 44));
    assertEquals(SQLState.ProtocolViolation, notAllowed.getSQLState());
    assertEquals(Severity.FATAL, notAllowed.getSeverity());
    assertTrue(notAllowed.getMessage().contains("invalid length of test: 20"));
  }

  @Test
  public void testSkipMessage_skipZeroFallbackReadsSuccessfully() throws IOException {
    byte[] payload = new byte[] {0, 0, 0, 7, 10, 20, 30};
    InputStream customInputStream =
        new InputStream() {
          private int position = 0;

          @Override
          public int read() {
            if (position < payload.length) {
              return payload[position++] & 0xFF;
            }
            return -1;
          }

          @Override
          public long skip(long n) {
            return 0;
          }
        };

    ConnectionHandler connection = createConnectionHandler(customInputStream);
    SkipMessage skipMessage = SkipMessage.createForValidStream(connection);
    assertEquals("Skip", skipMessage.getMessageName());
  }

  @Test
  public void testSkipMessage_skipZeroAndEOFThrowsEOFException() {
    byte[] payload = new byte[] {0, 0, 0, 10};
    InputStream customInputStream =
        new InputStream() {
          private int position = 0;

          @Override
          public int read() {
            if (position < payload.length) {
              return payload[position++] & 0xFF;
            }
            return -1;
          }

          @Override
          public long skip(long n) {
            return 0;
          }
        };

    ConnectionHandler connection = createConnectionHandler(customInputStream);
    assertThrows(EOFException.class, () -> SkipMessage.createForValidStream(connection));
  }

  @Test
  public void testSkipMessage_lengthTooSmall() {
    byte[] payload = new byte[] {0, 0, 0, 3};
    ConnectionHandler connection = createConnectionHandler(payload);

    assertThrows(
        IllegalArgumentException.class, () -> SkipMessage.createForValidStream(connection));
  }
}
