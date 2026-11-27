// Copyright 2020 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.utils.Logging;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Generic representation for a wire message, generally executed by calling send. */
@InternalApi
public abstract class WireMessage {

  private static final Logger logger = Logger.getLogger(WireMessage.class.getName());

  /**
   * Initial chunk size (in bytes) for incremental payload reads. Keeping this small bounds the heap
   * pinned by a connection that sends a large declared length header and then stalls before sending
   * the payload.
   */
  static final int READ_CHUNK_SIZE = 256 * 1024;

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  protected final int length;
  protected final int maxMessageLength;
  protected final MessageReader messageReader;
  protected final DataInputStream inputStream;
  protected final DataOutputStream outputStream;
  protected final ConnectionHandler connection;

  public WireMessage(ConnectionHandler connection, int length) {
    Preconditions.checkArgument(length >= 4);
    this.connection = connection;
    this.maxMessageLength = connection.getMaxMessageLength();
    this.messageReader = connection.getServer().getMessageReader();
    this.inputStream = connection.getConnectionMetadata().getInputStream();
    this.outputStream = connection.getConnectionMetadata().getOutputStream();
    this.length = length;
  }

  /**
   * Extracts parameters/arguments from the user input.
   *
   * @param input The data stream containing the user request.
   * @return A byte array of user-defined parameters to be bound.
   * @throws Exception If reading fails in any way.
   */
  protected static byte[][] getParameters(DataInputStream input) throws Exception {
    int numberOfParameters = input.readShort();
    byte[][] parameters = new byte[numberOfParameters][];
    for (int i = 0; i < numberOfParameters; i++) {
      int paramLength = input.readInt();
      if (paramLength == -1) {
        parameters[i] = null;
      } else {
        parameters[i] = new byte[paramLength];
        if (paramLength > 0) {
          input.readFully(parameters[i]);
        }
      }
    }
    return parameters;
  }

  /**
   * Once the message is ready, call this to send it across the wire. Effectively a template
   * pattern.
   *
   * @throws Exception If the sending fails.
   */
  public void send() throws Exception {
    logger.log(Level.FINEST, Logging.format("Send", this::toString));
    sendPayload();
  }

  /**
   * Override this method to include post-processing and metadata in the sending process. Template
   * method for send.
   *
   * @throws Exception If any step in output message fails.
   */
  protected abstract void sendPayload() throws Exception;

  /**
   * Used for logging.
   *
   * @return The official name of the wire message.
   */
  protected abstract String getMessageName();

  /**
   * Used for logging.
   *
   * @return Payload metadata.
   */
  protected abstract String getPayloadString();

  /**
   * Used for logging.
   *
   * @return Message Identifier (int for Bootstrap, char otherwise).
   */
  public abstract String getIdentifier();

  @Override
  public String toString() {
    return new MessageFormat("> Received Message: ({0}) {1}, with Payload: '{'{2}'}'")
        .format(
            new Object[] {this.getIdentifier(), this.getMessageName(), this.getPayloadString()});
  }

  /**
   * Read the remainder of the received message. To be called once metadata for the message is read.
   * Metadata is designated as type, length, etc.
   *
   * @return The remainder of the message in String format.
   * @throws Exception If reading fails in any way.
   */
  protected String readAll() throws Exception {
    int remaining = this.length - this.getHeaderLength();
    if (remaining < 0) {
      throw new IOException(
          String.format(
              "Invalid message length: %d is smaller than header length %d",
              this.length, this.getHeaderLength()));
    }
    if (remaining == 0) {
      return "";
    }
    return read(remaining);
  }

  /**
   * Reads a fixed-length string from the saved stream. The string still needs to be
   * null-terminated, and is read incrementally for large lengths so an unfulfilled frame header
   * cannot pin a large heap buffer before bytes actually arrive.
   *
   * @param length The number of bytes to read.
   * @return the string.
   * @throws IOException if an error occurs while reading from the stream, if the length exceeds
   *     {@link #maxMessageLength}, or if no null-terminator is found at the end of the string.
   */
  public String read(int length) throws IOException {
    validateReadLength(length, 1);
    if (length == 1) {
      byte zero = inputStream.readByte();
      if (zero != (byte) 0) {
        throw new IOException("String was not null-terminated");
      }
      return "";
    }
    byte[] buffer = readBytes(length - 1);
    byte zero = inputStream.readByte();
    if (zero != (byte) 0) {
      throw new IOException("String was not null-terminated");
    }
    return new String(buffer, StandardCharsets.UTF_8);
  }

  /**
   * Reads {@code numBytes} bytes from {@link #inputStream}. For lengths larger than {@link
   * #READ_CHUNK_SIZE}, grows a single buffer exponentially starting from {@link #READ_CHUNK_SIZE}
   * as bytes arrive so an unfulfilled frame header cannot pre-allocate a large buffer while
   * avoiding intermediate chunk lists and extra final copies.
   */
  protected byte[] readBytes(int numBytes) throws IOException {
    validateReadLength(numBytes, 0);
    if (numBytes == 0) {
      return EMPTY_BYTE_ARRAY;
    }
    if (numBytes <= READ_CHUNK_SIZE) {
      byte[] buffer = new byte[numBytes];
      this.inputStream.readFully(buffer);
      return buffer;
    }
    byte[] buffer = new byte[READ_CHUNK_SIZE];
    int offset = 0;
    while (offset < numBytes) {
      int targetLength;
      if (offset == 0) {
        targetLength = READ_CHUNK_SIZE;
      } else {
        targetLength = buffer.length <= numBytes / 2 ? buffer.length * 2 : numBytes;
        buffer = Arrays.copyOf(buffer, targetLength);
      }
      this.inputStream.readFully(buffer, offset, targetLength - offset);
      offset = targetLength;
    }
    return buffer;
  }

  /**
   * Reads a null-terminated string from a {@link DataInputStream}.
   *
   * @return the string.
   * @throws IOException if an error occurs while reading from the stream, or if no null-terminator
   *     is found before the end of the stream.
   */
  public String readString() throws IOException {
    int markReadLimit = Math.min(getMaxMessageLength(), this.length);
    this.inputStream.mark(markReadLimit);
    try {
      int index = 0;
      while (index < markReadLimit) {
        byte b = this.inputStream.readByte();
        if (b == 0) {
          break;
        }
        index++;
        if (index == markReadLimit) {
          throw new IOException("No null terminator found");
        }
      }
      if (index == 0) {
        // Empty string, we don't have to ready anything.
        return "";
      }

      // Reset the stream to the mark and read the string.
      this.inputStream.reset();
      byte[] result = new byte[index];
      this.inputStream.readFully(result);
      // Read the null-terminator.
      this.inputStream.readByte();
      return new String(result, StandardCharsets.UTF_8);
    } finally {
      // Drop the mark.
      this.inputStream.mark(0);
    }
  }

  /**
   * How many bytes is taken by the payload header. Header is defined here as protocol definition +
   * length. Most common value here is four bytes, so we keep that as default. Effectively, this is
   * how much of the message you "don't" want to read from the message's total length with readAll.
   *
   * @return The remaining length to be processed once "header" information is processed.
   */
  protected int getHeaderLength() {
    return 4;
  }

  /**
   * Some messages may have some more context and require some order. This handles state machine
   * setting for {@link ConnectionHandler}.
   */
  public void nextHandler() throws Exception {
    this.connection.setMessageState(this.messageReader.create(this.connection));
  }

  protected int getMaxMessageLength() {
    return this.maxMessageLength;
  }

  protected static String getMaxMessageLengthHint(int maxMessageLength) {
    return String.format(
        "Maximum allowed message length is %d bytes. Increase the max_message_length setting if larger messages are required.",
        maxMessageLength);
  }

  private void validateReadLength(int numBytes, int min) throws IOException {
    if (numBytes < min) {
      throw new IOException("Invalid message length: " + numBytes);
    }
    int effectiveMaxLength = getMaxMessageLength();
    if (numBytes > effectiveMaxLength) {
      PGException.Builder builder =
          PGException.newBuilder("Invalid message length: " + numBytes)
              .setSeverity(Severity.FATAL)
              .setSQLState(SQLState.ProtocolViolation);
      if (effectiveMaxLength == this.maxMessageLength) {
        builder.setHints(getMaxMessageLengthHint(effectiveMaxLength));
      }
      throw builder.build();
    }
  }

  /**
   * Validates that the message length is within the expected range [min, max], inclusive.
   *
   * @param length The message length to check.
   * @param messageType The description of the message type for the error message.
   * @param min The minimum allowed length.
   * @param max The maximum allowed length.
   * @throws PGException If the length is outside the range.
   */
  protected static void checkLengthRange(int length, String messageType, int min, int max) {
    checkLengthRange(length, messageType, min, max, null);
  }

  /**
   * Validates that the message length is within the expected range [min, max], inclusive.
   *
   * @param length The message length to check.
   * @param messageType The description of the message type for the error message.
   * @param min The minimum allowed length.
   * @param max The maximum allowed length.
   * @param maxExceededHint Optional hint attached to the {@link PGException} when {@code length >
   *     max}.
   * @throws PGException If the length is outside the range.
   */
  protected static void checkLengthRange(
      int length, String messageType, int min, int max, String maxExceededHint) {
    if (length < min || length > max) {
      PGException.Builder builder =
          PGException.newBuilder(String.format("invalid length of %s: %d", messageType, length))
              .setSeverity(Severity.FATAL)
              .setSQLState(SQLState.ProtocolViolation);
      if (length > max && maxExceededHint != null) {
        builder.setHints(maxExceededHint);
      }
      throw builder.build();
    }
  }

  /**
   * Validates that the message length matches the expected length.
   *
   * @param length The message length to check.
   * @param messageType The description of the message type for the error message.
   * @param expectedLength The exact required length.
   * @throws PGException If the length does not match.
   */
  protected static void checkLengthEquals(int length, String messageType, int expectedLength) {
    if (length != expectedLength) {
      throw PGException.newBuilder(String.format("invalid length of %s: %d", messageType, length))
          .setSeverity(Severity.FATAL)
          .setSQLState(SQLState.ProtocolViolation)
          .build();
    }
  }

  /**
   * Validates that the message length matches one of the allowed lengths.
   *
   * @param length The message length to check.
   * @param messageType The description of the message type for the error message.
   * @param allowedLengths The allowed lengths.
   * @throws PGException If the length does not match any allowed length.
   */
  protected static void checkLengthIn(int length, String messageType, int... allowedLengths) {
    for (int allowedLength : allowedLengths) {
      if (length == allowedLength) {
        return;
      }
    }
    throw PGException.newBuilder(String.format("invalid length of %s: %d", messageType, length))
        .setSeverity(Severity.FATAL)
        .setSQLState(SQLState.ProtocolViolation)
        .build();
  }
}
