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
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
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
}
