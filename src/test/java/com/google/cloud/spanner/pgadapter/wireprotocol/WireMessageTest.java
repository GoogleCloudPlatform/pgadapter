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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.ExtendedQueryProtocolHandler;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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
    when(connection.getMaxMessageLength()).thenReturn(OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
    when(connection.getWellKnownClient()).thenReturn(WellKnownClient.UNSPECIFIED);
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

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private ConnectionHandler createMockConnection(byte[] inputBytes) {
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.shouldAuthenticate()).thenReturn(false);
    when(server.getOptions()).thenReturn(options);
    when(server.getMessageReader()).thenReturn(new MessageReader(options));

    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(metadata.getInputStream())
        .thenReturn(new DataInputStream(new ByteArrayInputStream(inputBytes)));
    when(metadata.getOutputStream()).thenReturn(new DataOutputStream(new ByteArrayOutputStream()));

    ConnectionHandler connection = mock(ConnectionHandler.class);
    when(connection.getServer()).thenReturn(server);
    when(connection.getConnectionMetadata()).thenReturn(metadata);
    when(connection.getWellKnownClient()).thenReturn(WellKnownClient.UNSPECIFIED);
    when(connection.getMaxMessageLength()).thenReturn(OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
    return connection;
  }

  private List<String> captureWireMessageFinestLogs(ThrowingRunnable action) throws Exception {
    Logger wireMessageLogger = Logger.getLogger(WireMessage.class.getName());
    Level previousLevel = wireMessageLogger.getLevel();
    boolean previousUseParentHandlers = wireMessageLogger.getUseParentHandlers();
    List<String> loggedMessages = new ArrayList<>();
    Handler testHandler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            loggedMessages.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    try {
      wireMessageLogger.setUseParentHandlers(false);
      wireMessageLogger.addHandler(testHandler);
      wireMessageLogger.setLevel(Level.FINEST);
      action.run();
    } finally {
      wireMessageLogger.removeHandler(testHandler);
      wireMessageLogger.setLevel(previousLevel);
      wireMessageLogger.setUseParentHandlers(previousUseParentHandlers);
    }
    return loggedMessages;
  }

  @Test
  public void testPasswordMessage_redactsPasswordInLogs() throws Exception {
    String secretToken = "ya29.secret_oauth_token_value";
    byte[] passwordBytes = (secretToken + "\0").getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(4 + passwordBytes.length);
    dataOutputStream.write(passwordBytes);

    ConnectionHandler connection = createMockConnection(outputStream.toByteArray());
    PasswordMessage passwordMessage =
        new PasswordMessage(connection, Collections.singletonMap("user", "oauth2"));

    assertFalse(passwordMessage.getPayloadString().contains(secretToken));
    assertTrue(passwordMessage.getPayloadString().contains("Username: oauth2"));
    assertTrue(passwordMessage.getPayloadString().contains("<REDACTED>"));
    assertFalse(passwordMessage.toString().contains(secretToken));

    List<String> loggedMessages = captureWireMessageFinestLogs(passwordMessage::send);
    String targetMessage = null;
    for (String message : loggedMessages) {
      assertFalse(message.contains(secretToken));
      if (message.contains("Username: oauth2")) {
        targetMessage = message;
      }
    }
    assertNotNull(targetMessage);
    assertTrue(targetMessage.contains("<REDACTED>"));
  }

  @Test
  public void testCancelMessage_redactsSecretInLogs() throws Exception {
    byte[][] secrets =
        new byte[][] {
          new byte[] {17, 29, 43, 59},
          new byte[] {
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
            33, 34, 35, 36, 37, 38, 39, 40, 41, 42
          }
        };
    for (byte[] secret : secrets) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      dataOutputStream.writeInt(77);
      dataOutputStream.write(secret);

      int length = 12 + secret.length;
      ConnectionHandler connection = createMockConnection(outputStream.toByteArray());
      CancelMessage cancelMessage = new CancelMessage(connection, length);
      assertFalse(cancelMessage.getPayloadString().contains(Arrays.toString(secret)));
      assertTrue(cancelMessage.getPayloadString().contains("Connection ID: 77"));
      assertTrue(cancelMessage.getPayloadString().contains("<REDACTED>"));

      List<String> loggedMessages = captureWireMessageFinestLogs(cancelMessage::send);
      String targetMessage = null;
      for (String message : loggedMessages) {
        assertFalse(message.contains(Arrays.toString(secret)));
        if (message.contains("Connection ID: 77")) {
          targetMessage = message;
        }
      }
      assertNotNull(targetMessage);
      assertTrue(targetMessage.contains("<REDACTED>"));
    }
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

    assertProtocolViolationWithoutHint(
        assertThrows(PGException.class, () -> SkipMessage.createForValidStream(connection)),
        "invalid length of control message: 3");
  }

  @Test
  public void testRead_lengthExceedsMaxThrowsPGException() {
    ConnectionHandler connection = createConnectionHandler(new byte[0]);
    TestWireMessage message = new TestWireMessage(connection, 4);

    assertMaxMessageLengthExceeded(
        assertThrows(
            PGException.class, () -> message.read(OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH + 1)),
        "Invalid message length: " + (OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH + 1),
        OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
  }

  @Test
  public void testReadBytes_invalidLengthThrowsExpectedExceptions() {
    ConnectionHandler connection = createConnectionHandler(new byte[0]);
    TestWireMessage message = new TestWireMessage(connection, 4);

    IOException negativeException = assertThrows(IOException.class, () -> message.readBytes(-1));
    assertTrue(negativeException.getMessage().contains("Invalid message length"));

    assertMaxMessageLengthExceeded(
        assertThrows(
            PGException.class,
            () -> message.readBytes(OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH + 1)),
        "Invalid message length: " + (OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH + 1),
        OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
  }

  @Test
  public void testReadBytes_boundaries() throws IOException {
    ConnectionHandler emptyConnection = createConnectionHandler(new byte[0]);
    TestWireMessage emptyMessage = new TestWireMessage(emptyConnection, 4);
    assertArrayEquals(new byte[0], emptyMessage.readBytes(0));

    for (int length : new int[] {1, WireMessage.READ_CHUNK_SIZE, WireMessage.READ_CHUNK_SIZE + 1}) {
      byte[] expected = new byte[length];
      for (int i = 0; i < length; i++) {
        expected[i] = (byte) (i * 17 + 3);
      }
      ConnectionHandler connection = createConnectionHandler(expected);
      TestWireMessage message = new TestWireMessage(connection, 4 + length);
      assertArrayEquals(expected, message.readBytes(length));
    }
  }

  @Test
  public void testRead_incrementalLargePayloadReadsSuccessfully() throws IOException {
    int payloadLength = WireMessage.READ_CHUNK_SIZE * 3 + 17;
    byte[] payloadBytes = new byte[payloadLength + 1];
    for (int i = 0; i < payloadLength; i++) {
      payloadBytes[i] = (byte) ('a' + (i % 26));
    }
    payloadBytes[payloadLength] = 0;

    ConnectionHandler connection = createConnectionHandler(payloadBytes);
    TestWireMessage message = new TestWireMessage(connection, 4 + payloadBytes.length);

    String result = message.read(payloadBytes.length);
    assertEquals(payloadLength, result.length());
    assertEquals(new String(payloadBytes, 0, payloadLength, StandardCharsets.UTF_8), result);
  }

  @Test
  public void testRead_largeDeclaredLengthWithTruncatedStreamThrowsEOF() {
    // Only 10 bytes present even though read() is asked for 10_000_000 bytes; must not
    // pre-allocate 10_000_000 bytes and must fail with EOFException as soon as stream ends.
    ConnectionHandler connection =
        createConnectionHandler(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    TestWireMessage message = new TestWireMessage(connection, 10_000_004);

    assertThrows(EOFException.class, () -> message.read(10_000_000));
  }

  @Test
  public void testCopyDataMessage_incrementalLargePayloadReadsSuccessfully() throws Exception {
    int payloadLength = WireMessage.READ_CHUNK_SIZE * 2 + 123;
    byte[] data = new byte[payloadLength];
    for (int i = 0; i < payloadLength; i++) {
      data[i] = (byte) (i & 0xFF);
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(4 + payloadLength);
    dataOutputStream.write(data);

    ConnectionHandler connection = createConnectionHandler(outputStream.toByteArray());
    CopyDataMessage copyDataMessage = new CopyDataMessage(connection);
    assertArrayEquals(data, copyDataMessage.getPayload());
  }

  private byte[] createLengthHeaderOnly(int length) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(length);
    return outputStream.toByteArray();
  }

  private ConnectionHandler createMockConnectionWithExtendedHandler(byte[] inputBytes) {
    ConnectionHandler connection = createMockConnection(inputBytes);
    when(connection.getExtendedQueryProtocolHandler())
        .thenReturn(mock(ExtendedQueryProtocolHandler.class));
    return connection;
  }

  @Test
  public void testControlMessages_oversizedLengthRejectedBeforeAllocating() throws Exception {
    int[] oversizedLengths = {OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH + 1, Integer.MAX_VALUE};
    for (int oversizedLength : oversizedLengths) {
      byte[] header = createLengthHeaderOnly(oversizedLength);

      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new QueryMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of query message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new DescribeMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of describe message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new ExecuteMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of execute message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new CloseMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of close message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new CopyFailMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of copy fail message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new CopyDataMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of copy data message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new ParseMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of parse message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new BindMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of bind message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () -> new FunctionCallMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of function call message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
      assertMaxMessageLengthExceeded(
          assertThrows(
              PGException.class,
              () ->
                  SkipMessage.createForValidStream(
                      createMockConnectionWithExtendedHandler(header))),
          "invalid length of control message",
          OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH);
    }
  }

  @Test
  public void testControlMessages_invalidExactOrMinLengthRejected() throws Exception {
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new QueryMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(4)))),
        "invalid length of query message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new DescribeMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(5)))),
        "invalid length of describe message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new ExecuteMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(8)))),
        "invalid length of execute message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new CloseMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(5)))),
        "invalid length of close message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new CopyFailMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(4)))),
        "invalid length of copy fail message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new FlushMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(5)))),
        "invalid length of flush message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new SyncMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(5)))),
        "invalid length of sync message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new TerminateMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(5)))),
        "invalid length of terminate message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new CopyDoneMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(5)))),
        "invalid length of copy done message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new ParseMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(7)))),
        "invalid length of parse message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new BindMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(11)))),
        "invalid length of bind message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new FunctionCallMessage(
                    createMockConnectionWithExtendedHandler(createLengthHeaderOnly(13)))),
        "invalid length of function call message");
    assertProtocolViolationWithoutHint(
        assertThrows(
            PGException.class,
            () ->
                new PasswordMessage(
                    createMockConnectionWithExtendedHandler(
                        createLengthHeaderOnly(PasswordMessage.MAX_PASSWORD_LENGTH + 1)),
                    Collections.emptyMap())),
        "invalid length of password message");
  }

  @Test
  public void testControlMessages_lengthSmallerThanHeaderThrowsPGException() throws Exception {
    for (int invalidLength : new int[] {3, 0, -1}) {
      byte[] header = createLengthHeaderOnly(invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new QueryMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of query message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new CopyDataMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of copy data message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new SyncMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of sync message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new FlushMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of flush message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () ->
                  new PasswordMessage(
                      createMockConnectionWithExtendedHandler(header), Collections.emptyMap())),
          "invalid length of password message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new ParseMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of parse message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new BindMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of bind message: " + invalidLength);
      assertProtocolViolationWithoutHint(
          assertThrows(
              PGException.class,
              () -> new FunctionCallMessage(createMockConnectionWithExtendedHandler(header))),
          "invalid length of function call message: " + invalidLength);
    }
  }

  private static void assertProtocolViolation(PGException exception, String expectedMessage) {
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertTrue(exception.getMessage().contains(expectedMessage));
  }

  private static void assertProtocolViolationWithoutHint(
      PGException exception, String expectedMessage) {
    assertProtocolViolation(exception, expectedMessage);
    assertNull(exception.getHints());
  }

  private static void assertMaxMessageLengthExceeded(
      PGException exception, String expectedMessage, int maxMessageLength) {
    assertProtocolViolation(exception, expectedMessage);
    assertNotNull(exception.getHints());
    assertTrue(exception.getHints().contains(maxMessageLength + " bytes"));
    assertTrue(exception.getHints().contains("max_message_length"));
    assertFalse(exception.getHints().contains("PGAdapter"));
  }

  @Test
  public void testConfigurableMaxMessageLength() throws Exception {
    // Smaller configured max (100 bytes) rejects 101-byte QueryMessage immediately.
    byte[] oversizedHeader = createLengthHeaderOnly(101);
    ConnectionHandler smallLimitConnection =
        createMockConnectionWithExtendedHandler(oversizedHeader);
    when(smallLimitConnection.getMaxMessageLength()).thenReturn(100);
    assertMaxMessageLengthExceeded(
        assertThrows(PGException.class, () -> new QueryMessage(smallLimitConnection)),
        "invalid length of query message: 101",
        100);

    // Exact configured max (13 bytes for "SELECT 1\0" + 4-byte length) succeeds.
    byte[] sqlBytes = "SELECT 1\0".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream exactOutputStream = new ByteArrayOutputStream();
    DataOutputStream exactDataOutput = new DataOutputStream(exactOutputStream);
    exactDataOutput.writeInt(4 + sqlBytes.length);
    exactDataOutput.write(sqlBytes);
    ConnectionHandler exactLimitConnection =
        createMockConnectionWithExtendedHandler(exactOutputStream.toByteArray());
    when(exactLimitConnection.getMaxMessageLength()).thenReturn(4 + sqlBytes.length);
    QueryMessage exactQueryMessage = new QueryMessage(exactLimitConnection);
    assertEquals("SELECT 1", exactQueryMessage.getStatement().getSql());

    // Larger configured max (200 MB) allows message length > default 100 MB.
    int aboveDefaultLength = OptionsMetadata.DEFAULT_MAX_MESSAGE_LENGTH + 1;
    byte[] aboveDefaultHeader = createLengthHeaderOnly(aboveDefaultLength);
    ConnectionHandler largeLimitConnection =
        createMockConnectionWithExtendedHandler(aboveDefaultHeader);
    when(largeLimitConnection.getMaxMessageLength()).thenReturn(200_000_000);
    // Rejects on EOF while reading payload rather than rejecting the 100_000_001 header length.
    assertThrows(EOFException.class, () -> new QueryMessage(largeLimitConnection));
  }

  @Test
  public void testReadString_missingNullTerminatorExceedsLimitThrowsIOException() {
    byte[] nonTerminated = new byte[32];
    Arrays.fill(nonTerminated, (byte) 'a');
    ConnectionHandler connection = createConnectionHandler(nonTerminated);
    TestWireMessage message = new TestWireMessage(connection, 16);

    IOException exception = assertThrows(IOException.class, message::readString);
    assertTrue(exception.getMessage().contains("No null terminator found"));
  }

  @Test
  public void testReadBytes_exactCustomMaxMessageLengthAboveChunkSize() throws IOException {
    int customMaxLength = WireMessage.READ_CHUNK_SIZE + 17;
    byte[] expected = new byte[customMaxLength];
    for (int i = 0; i < customMaxLength; i++) {
      expected[i] = (byte) (i & 0xFF);
    }
    ConnectionHandler connection = createConnectionHandler(expected);
    when(connection.getMaxMessageLength()).thenReturn(customMaxLength);
    TestWireMessage message = new TestWireMessage(connection, 4 + customMaxLength);

    assertArrayEquals(expected, message.readBytes(customMaxLength));
  }

  @Test
  public void testPasswordMessage_largerThanSmallMaxMessageLengthSucceeds() throws Exception {
    byte[] passwordBytes = "0123456789abcdef_secret_password\0".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(4 + passwordBytes.length);
    dataOutputStream.write(passwordBytes);

    ConnectionHandler connection = createConnectionHandler(outputStream.toByteArray());
    when(connection.getMaxMessageLength()).thenReturn(16);
    PasswordMessage passwordMessage = new PasswordMessage(connection, Collections.emptyMap());
    assertEquals("0123456789abcdef_secret_password", passwordMessage.getPassword());
  }

  private void assertFatalProtocolViolation(ThrowingRunnable action) {
    PGException exception = assertThrows(PGException.class, action::run);
    assertEquals(Severity.FATAL, exception.getSeverity());
    assertEquals(SQLState.ProtocolViolation, exception.getSQLState());
  }

  @Test
  public void testReadString_longStringWithoutMarkBufferExpansion() throws IOException {
    byte[] rawString = new byte[512];
    Arrays.fill(rawString, (byte) 'x');
    byte[] wireBytes = new byte[rawString.length + 1];
    System.arraycopy(rawString, 0, wireBytes, 0, rawString.length);
    ConnectionHandler connection = createConnectionHandler(wireBytes);
    TestWireMessage message = new TestWireMessage(connection, 4 + wireBytes.length);

    assertEquals(new String(rawString, StandardCharsets.UTF_8), message.readString());
  }

  @Test
  public void testGetParameters_validNullEmptyAndSmallValues() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeShort(3);
    dataOutputStream.writeInt(-1);
    dataOutputStream.writeInt(0);
    dataOutputStream.writeInt(5);
    dataOutputStream.write("hello".getBytes(StandardCharsets.UTF_8));
    byte[] payload = outputStream.toByteArray();

    byte[][] parameters =
        WireMessage.getParameters(
            new DataInputStream(new ByteArrayInputStream(payload)), payload.length, 0);
    assertEquals(3, parameters.length);
    assertNull(parameters[0]);
    assertArrayEquals(new byte[0], parameters[1]);
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), parameters[2]);
  }

  @Test
  public void testGetParameters_chunkBoundariesAndLargeChunkedParameter() throws Exception {
    for (int size :
        new int[] {
          WireMessage.READ_CHUNK_SIZE,
          WireMessage.READ_CHUNK_SIZE + 1,
          WireMessage.READ_CHUNK_SIZE * 3 + 123
        }) {
      byte[] expectedBytes = new byte[size];
      for (int index = 0; index < expectedBytes.length; index++) {
        expectedBytes[index] = (byte) (index * 31 + 7);
      }
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      dataOutputStream.writeShort(1);
      dataOutputStream.writeInt(expectedBytes.length);
      dataOutputStream.write(expectedBytes);
      byte[] payload = outputStream.toByteArray();

      byte[][] parameters =
          WireMessage.getParameters(
              new DataInputStream(new ByteArrayInputStream(payload)), payload.length, 0);
      assertEquals(1, parameters.length);
      assertArrayEquals(expectedBytes, parameters[0]);
    }
  }

  @Test
  public void testGetParameters_remainingLengthTooSmall() {
    DataInputStream emptyInput = new DataInputStream(new ByteArrayInputStream(new byte[0]));
    assertFatalProtocolViolation(() -> WireMessage.getParameters(emptyInput, 3, 2));
  }

  @Test
  public void testGetParameters_negativeParameterCount() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeShort(-1);
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));

    assertFatalProtocolViolation(() -> WireMessage.getParameters(inputStream, 100, 0));
  }

  @Test
  public void testGetParameters_negativeParameterLength() throws Exception {
    for (int invalidLength : new int[] {-2, -100, Integer.MIN_VALUE}) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      dataOutputStream.writeShort(1);
      dataOutputStream.writeInt(invalidLength);
      DataInputStream inputStream =
          new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));

      assertFatalProtocolViolation(() -> WireMessage.getParameters(inputStream, 100, 0));
    }
  }

  @Test
  public void testGetParameters_parameterCountExceedsRemainingBytes() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeShort(3);
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));

    assertFatalProtocolViolation(() -> WireMessage.getParameters(inputStream, 15, 2));
  }

  @Test
  public void testGetParameters_parameterLengthExceedsRemainingBytes() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeShort(1);
    dataOutputStream.writeInt(Integer.MAX_VALUE);
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));

    assertFatalProtocolViolation(() -> WireMessage.getParameters(inputStream, 8, 2));
  }

  @Test
  public void testGetParameters_runningTotalExceedsRemainingBytes() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeShort(2);
    dataOutputStream.writeInt(4);
    dataOutputStream.writeInt(1234);
    dataOutputStream.writeInt(5);
    byte[] payload = outputStream.toByteArray();

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(payload));
    assertFatalProtocolViolation(() -> WireMessage.getParameters(inputStream, 20, 2));
  }

  @Test
  public void testGetParameters_exactRemainingBoundarySucceedsAndPlusOneFails() throws Exception {
    ByteArrayOutputStream validOutputStream = new ByteArrayOutputStream();
    DataOutputStream validDataOutputStream = new DataOutputStream(validOutputStream);
    validDataOutputStream.writeShort(1);
    validDataOutputStream.writeInt(4);
    validDataOutputStream.writeInt(99);
    // 2 (count) + 4 (len) + 4 (data) + 2 (trailingMinBytes) = 12.
    byte[][] valid =
        WireMessage.getParameters(
            new DataInputStream(new ByteArrayInputStream(validOutputStream.toByteArray())), 12, 2);
    assertEquals(1, valid.length);
    assertEquals(4, valid[0].length);

    ByteArrayOutputStream invalidOutputStream = new ByteArrayOutputStream();
    DataOutputStream invalidDataOutputStream = new DataOutputStream(invalidOutputStream);
    invalidDataOutputStream.writeShort(1);
    invalidDataOutputStream.writeInt(5);
    DataInputStream invalidInput =
        new DataInputStream(new ByteArrayInputStream(invalidOutputStream.toByteArray()));
    assertFatalProtocolViolation(() -> WireMessage.getParameters(invalidInput, 12, 2));
  }

  @Test
  public void testGetParameters_truncatedLargeParameterFailsWithEofWithoutPreallocating()
      throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeShort(1);
    dataOutputStream.writeInt(50_000_000);
    dataOutputStream.write(new byte[64]);
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));

    assertThrows(EOFException.class, () -> WireMessage.getParameters(inputStream, 60_000_000, 0));
  }

  private ConnectionHandler createAuthenticatedConnectionHandler(byte[] inputBytes) {
    ConnectionHandler connection = createConnectionHandler(inputBytes);
    when(connection.getExtendedQueryProtocolHandler())
        .thenReturn(mock(ExtendedQueryProtocolHandler.class));
    return connection;
  }

  @Test
  public void testBindMessage_oversizedParameterLengthRejectedBeforeReading() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(16);
    dataOutputStream.writeByte(0); // portalName = ""
    dataOutputStream.writeByte(0); // statementName = ""
    dataOutputStream.writeShort(0); // formatCodes = 0
    dataOutputStream.writeShort(1); // parameters = 1
    dataOutputStream.writeInt(Integer.MAX_VALUE); // malicious 2 GB parameter length
    ConnectionHandler connection = createAuthenticatedConnectionHandler(outputStream.toByteArray());

    assertFatalProtocolViolation(() -> new BindMessage(connection));
  }

  @Test
  public void testBindMessage_unterminatedStringsExceedFrame() throws Exception {
    // Unterminated portalName at maxBytes == 1
    ByteArrayOutputStream portalOutputStream = new ByteArrayOutputStream();
    DataOutputStream portalDataOutputStream = new DataOutputStream(portalOutputStream);
    portalDataOutputStream.writeInt(12);
    portalDataOutputStream.writeBytes("too-long-portal");
    assertFatalProtocolViolation(
        () ->
            new BindMessage(
                createAuthenticatedConnectionHandler(portalOutputStream.toByteArray())));

    // Unterminated portalName at maxBytes > 1 (16 - 4 - 7 = 5 bytes max, 5 non-null bytes sent)
    ByteArrayOutputStream multiBytePortalOutputStream = new ByteArrayOutputStream();
    DataOutputStream multiBytePortalDataOutputStream =
        new DataOutputStream(multiBytePortalOutputStream);
    multiBytePortalDataOutputStream.writeInt(16);
    multiBytePortalDataOutputStream.writeBytes("abcde");
    assertFatalProtocolViolation(
        () ->
            new BindMessage(
                createAuthenticatedConnectionHandler(multiBytePortalOutputStream.toByteArray())));

    // Unterminated statementName
    ByteArrayOutputStream statementOutputStream = new ByteArrayOutputStream();
    DataOutputStream statementDataOutputStream = new DataOutputStream(statementOutputStream);
    statementDataOutputStream.writeInt(12);
    statementDataOutputStream.writeByte(0); // portalName = ""
    statementDataOutputStream.writeBytes("too-long-statement");
    assertFatalProtocolViolation(
        () ->
            new BindMessage(
                createAuthenticatedConnectionHandler(statementOutputStream.toByteArray())));
  }

  @Test
  public void testBindMessage_invalidFormatCodesAndTrailingBytes() throws Exception {
    // Negative format code count.
    ByteArrayOutputStream negativeFormatStream = new ByteArrayOutputStream();
    DataOutputStream negativeFormatOutputStream = new DataOutputStream(negativeFormatStream);
    negativeFormatOutputStream.writeInt(12);
    negativeFormatOutputStream.writeByte(0);
    negativeFormatOutputStream.writeByte(0);
    negativeFormatOutputStream.writeShort(-1);
    assertFatalProtocolViolation(
        () ->
            new BindMessage(
                createAuthenticatedConnectionHandler(negativeFormatStream.toByteArray())));

    // Result format code count exceeding remaining length.
    ByteArrayOutputStream oversizedResultFormatStream = new ByteArrayOutputStream();
    DataOutputStream oversizedResultFormatOutputStream =
        new DataOutputStream(oversizedResultFormatStream);
    oversizedResultFormatOutputStream.writeInt(12);
    oversizedResultFormatOutputStream.writeByte(0);
    oversizedResultFormatOutputStream.writeByte(0);
    oversizedResultFormatOutputStream.writeShort(0);
    oversizedResultFormatOutputStream.writeShort(0);
    oversizedResultFormatOutputStream.writeShort(1);
    assertFatalProtocolViolation(
        () ->
            new BindMessage(
                createAuthenticatedConnectionHandler(oversizedResultFormatStream.toByteArray())));

    // Trailing unused bytes in declared frame length.
    ByteArrayOutputStream trailingBytesStream = new ByteArrayOutputStream();
    DataOutputStream trailingBytesOutputStream = new DataOutputStream(trailingBytesStream);
    trailingBytesOutputStream.writeInt(14);
    trailingBytesOutputStream.writeByte(0);
    trailingBytesOutputStream.writeByte(0);
    trailingBytesOutputStream.writeShort(0);
    trailingBytesOutputStream.writeShort(0);
    trailingBytesOutputStream.writeShort(0);
    trailingBytesOutputStream.writeShort(0);
    assertFatalProtocolViolation(
        () ->
            new BindMessage(
                createAuthenticatedConnectionHandler(trailingBytesStream.toByteArray())));
  }

  @Test
  public void testBindMessage_honorsCustomMaxMessageLength() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(24);
    dataOutputStream.writeByte(0);
    dataOutputStream.writeByte(0);
    dataOutputStream.writeShort(0);
    dataOutputStream.writeShort(1);
    dataOutputStream.writeInt(8);
    dataOutputStream.writeLong(42L);
    dataOutputStream.writeShort(0);
    byte[] bindBytes = outputStream.toByteArray();

    ConnectionHandler smallLimitConnection = createAuthenticatedConnectionHandler(bindBytes);
    when(smallLimitConnection.getMaxMessageLength()).thenReturn(20);
    assertFatalProtocolViolation(() -> new BindMessage(smallLimitConnection));

    ConnectionHandler exactLimitConnection = createAuthenticatedConnectionHandler(bindBytes);
    when(exactLimitConnection.getMaxMessageLength()).thenReturn(24);
    BindMessage bindMessage = new BindMessage(exactLimitConnection);
    assertEquals(1, bindMessage.getParameters().length);
  }

  @Test
  public void testFunctionCallMessage_argumentAndFormatCodeBounds() throws Exception {
    // Oversized parameter length inside a small FunctionCall frame.
    ByteArrayOutputStream oversizedArgumentStream = new ByteArrayOutputStream();
    DataOutputStream oversizedArgumentOutputStream = new DataOutputStream(oversizedArgumentStream);
    oversizedArgumentOutputStream.writeInt(18);
    oversizedArgumentOutputStream.writeInt(1); // functionID
    oversizedArgumentOutputStream.writeShort(0); // argumentFormatCodes
    oversizedArgumentOutputStream.writeShort(1); // arguments
    oversizedArgumentOutputStream.writeInt(Integer.MAX_VALUE); // malicious 2 GB argument length
    assertFatalProtocolViolation(
        () ->
            new FunctionCallMessage(
                createConnectionHandler(oversizedArgumentStream.toByteArray())));

    // Argument format codes exceeding remaining frame length.
    ByteArrayOutputStream oversizedFormatCodesStream = new ByteArrayOutputStream();
    DataOutputStream oversizedFormatCodesOutputStream =
        new DataOutputStream(oversizedFormatCodesStream);
    oversizedFormatCodesOutputStream.writeInt(14);
    oversizedFormatCodesOutputStream.writeInt(1);
    oversizedFormatCodesOutputStream.writeShort(2);
    assertFatalProtocolViolation(
        () ->
            new FunctionCallMessage(
                createConnectionHandler(oversizedFormatCodesStream.toByteArray())));

    // Trailing bytes mismatch.
    ByteArrayOutputStream trailingStream = new ByteArrayOutputStream();
    DataOutputStream trailingOutputStream = new DataOutputStream(trailingStream);
    trailingOutputStream.writeInt(16);
    trailingOutputStream.writeInt(1);
    trailingOutputStream.writeShort(0);
    trailingOutputStream.writeShort(0);
    trailingOutputStream.writeShort(0);
    trailingOutputStream.writeShort(0);
    assertFatalProtocolViolation(
        () -> new FunctionCallMessage(createConnectionHandler(trailingStream.toByteArray())));
  }

  @Test
  public void testParseMessage_boundsAndTrailingBytes() throws Exception {
    // Negative parameter count.
    ByteArrayOutputStream negativeCountStream = new ByteArrayOutputStream();
    DataOutputStream negativeCountOutputStream = new DataOutputStream(negativeCountStream);
    negativeCountOutputStream.writeInt(8);
    negativeCountOutputStream.writeByte(0);
    negativeCountOutputStream.writeByte(0);
    negativeCountOutputStream.writeShort(-1);
    assertFatalProtocolViolation(
        () ->
            new ParseMessage(
                createAuthenticatedConnectionHandler(negativeCountStream.toByteArray())));

    // Parameter count exceeding remaining bytes.
    ByteArrayOutputStream oversizedCountStream = new ByteArrayOutputStream();
    DataOutputStream oversizedCountOutputStream = new DataOutputStream(oversizedCountStream);
    oversizedCountOutputStream.writeInt(8);
    oversizedCountOutputStream.writeByte(0);
    oversizedCountOutputStream.writeByte(0);
    oversizedCountOutputStream.writeShort(1);
    assertFatalProtocolViolation(
        () ->
            new ParseMessage(
                createAuthenticatedConnectionHandler(oversizedCountStream.toByteArray())));

    // Trailing unused bytes in declared frame length.
    ByteArrayOutputStream trailingStream = new ByteArrayOutputStream();
    DataOutputStream trailingOutputStream = new DataOutputStream(trailingStream);
    trailingOutputStream.writeInt(10);
    trailingOutputStream.writeByte(0);
    trailingOutputStream.writeByte(0);
    trailingOutputStream.writeShort(0);
    trailingOutputStream.writeShort(0);
    assertFatalProtocolViolation(
        () -> new ParseMessage(createAuthenticatedConnectionHandler(trailingStream.toByteArray())));
  }

  @Test
  public void testMessageReader_rejectsUnauthenticatedConnection() {
    ConnectionHandler connection = createConnectionHandler(new byte[] {'F', 0, 0, 0, 14});
    when(connection.getStatus()).thenReturn(ConnectionStatus.UNAUTHENTICATED);
    MessageReader reader = new MessageReader(connection.getServer().getOptions());

    assertFatalProtocolViolation(() -> reader.create(connection));
  }

  @Test
  public void testPasswordMessage_terminatesConnectionOnFailedAuth() throws Exception {
    for (boolean shouldAuthenticate : new boolean[] {true, false}) {
      byte[] passwordBytes = "invalid-password\0".getBytes(StandardCharsets.UTF_8);
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      dataOutputStream.writeInt(4 + passwordBytes.length);
      dataOutputStream.write(passwordBytes);

      ConnectionHandler connection = createConnectionHandler(outputStream.toByteArray());
      when(connection.getServer().getOptions().shouldAuthenticate()).thenReturn(shouldAuthenticate);

      PasswordMessage passwordMessage =
          new PasswordMessage(connection, Collections.singletonMap("user", "test-user"));
      passwordMessage.sendPayload();

      verify(connection).setStatus(ConnectionStatus.TERMINATED);
    }
  }

  @Test
  public void testBindMessage_portalNameExceedingInitialBufferSize() throws Exception {
    byte[] longPortalNameBytes = new byte[65];
    Arrays.fill(longPortalNameBytes, (byte) 'p');
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.writeInt(12 + longPortalNameBytes.length);
    dataOutputStream.write(longPortalNameBytes);
    dataOutputStream.writeByte(0); // portalName null terminator
    dataOutputStream.writeByte(0); // statementName = ""
    dataOutputStream.writeShort(0); // formatCodes = 0
    dataOutputStream.writeShort(0); // parameters = 0
    dataOutputStream.writeShort(0); // resultFormatCodes = 0

    ConnectionHandler connection = createAuthenticatedConnectionHandler(outputStream.toByteArray());
    BindMessage bindMessage = new BindMessage(connection);
    assertEquals(0, bindMessage.getParameters().length);
    assertEquals(0, bindMessage.getFormatCodes().length);
  }
}
