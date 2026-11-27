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
import com.google.common.annotations.VisibleForTesting;
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

  private static final int INITIAL_STRING_BUFFER_SIZE = 64;
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private static final byte[][] EMPTY_PARAMETERS = new byte[0][];
  private static final int[] EMPTY_PARAMETER_TYPES = new int[0];

  protected final int length;
  protected final int maxMessageLength;
  protected final MessageReader messageReader;
  protected final DataInputStream inputStream;
  protected final DataOutputStream outputStream;
  protected final ConnectionHandler connection;
  private int remainingBytes;

  public WireMessage(ConnectionHandler connection, int length) {
    Preconditions.checkArgument(length >= 4);
    this.connection = connection;
    this.maxMessageLength = connection.getMaxMessageLength();
    this.messageReader = connection.getServer().getMessageReader();
    this.inputStream = connection.getConnectionMetadata().getInputStream();
    this.outputStream = connection.getConnectionMetadata().getOutputStream();
    this.length = length;
    this.remainingBytes = length - 4;
  }

  /**
   * Extracts parameters/arguments from the user input, validating counts and per-parameter lengths
   * against the remaining bytes of the enclosing wire message.
   *
   * @param input The data stream containing the user request.
   * @param remainingBytes The number of bytes remaining in the enclosing message before reading the
   *     parameter count.
   * @param trailingMinBytes The minimum number of bytes required by fields following the parameters
   *     in the enclosing message.
   * @return A byte array of user-defined parameters to be bound.
   * @throws Exception If reading fails or the message violates the wire protocol bounds.
   */
  @VisibleForTesting
  static byte[][] getParameters(DataInputStream input, int remainingBytes, int trailingMinBytes)
      throws Exception {
    Preconditions.checkArgument(trailingMinBytes >= 0, "trailingMinBytes must be non-negative");
    if (remainingBytes < 2L + trailingMinBytes) {
      throw protocolViolation(
          String.format("invalid remaining length for parameters: %d", remainingBytes));
    }
    int numberOfParameters = input.readShort();
    remainingBytes -= 2;
    if (numberOfParameters < 0) {
      throw protocolViolation(
          String.format("invalid number of parameters: %d", numberOfParameters));
    }
    long availableForParameters = (long) remainingBytes - trailingMinBytes;
    if (4L * numberOfParameters > availableForParameters) {
      throw protocolViolation(
          String.format(
              "number of parameters (%d) exceeds remaining message length (%d)",
              numberOfParameters, availableForParameters));
    }
    if (numberOfParameters == 0) {
      return EMPTY_PARAMETERS;
    }
    byte[][] parameters = new byte[numberOfParameters][];
    for (int index = 0; index < numberOfParameters; index++) {
      long minimumBytesAfterParameter = 4L * (numberOfParameters - index - 1) + trailingMinBytes;
      int parameterLength = input.readInt();
      remainingBytes -= 4;
      if (parameterLength < -1) {
        throw protocolViolation(String.format("invalid parameter length: %d", parameterLength));
      }
      if (parameterLength == -1) {
        parameters[index] = null;
      } else {
        long maximumAllowedLength = (long) remainingBytes - minimumBytesAfterParameter;
        if (parameterLength > maximumAllowedLength) {
          throw protocolViolation(
              String.format(
                  "parameter length (%d) exceeds remaining message length (%d)",
                  parameterLength, maximumAllowedLength));
        }
        parameters[index] = readBytes(input, parameterLength);
        remainingBytes -= parameterLength;
      }
    }
    return parameters;
  }

  private static int consumedParameterBytes(byte[][] parameters) {
    int totalBytes = 2;
    for (byte[] parameter : parameters) {
      totalBytes += 4;
      if (parameter != null) {
        totalBytes += parameter.length;
      }
    }
    return totalBytes;
  }

  protected int readInt(String messageType) throws IOException {
    if (this.remainingBytes < 4) {
      throw protocolViolation(String.format("invalid length of %s: %d", messageType, this.length));
    }
    int value = this.inputStream.readInt();
    this.remainingBytes -= 4;
    return value;
  }

  protected String readBoundedString(int trailingMinBytes, String messageType) throws IOException {
    byte[] bytes =
        readBoundedNullTerminatedBytes(this.remainingBytes - trailingMinBytes, messageType);
    this.remainingBytes -= bytes.length + 1;
    return bytes.length == 0 ? "" : new String(bytes, StandardCharsets.UTF_8);
  }

  protected short[] readFormatCodes(int trailingMinBytes) throws Exception {
    short[] formatCodes =
        MessageReader.getFormatCodes(this.inputStream, this.remainingBytes, trailingMinBytes);
    this.remainingBytes -= 2 + 2 * formatCodes.length;
    return formatCodes;
  }

  protected byte[][] readParameters(int trailingMinBytes) throws Exception {
    byte[][] parameters = getParameters(this.inputStream, this.remainingBytes, trailingMinBytes);
    this.remainingBytes -= consumedParameterBytes(parameters);
    return parameters;
  }

  protected int[] readParameterDataTypes(String messageType) throws IOException {
    if (this.remainingBytes < 2) {
      throw protocolViolation(String.format("invalid length of %s: %d", messageType, this.length));
    }
    short numberOfParameters = this.inputStream.readShort();
    this.remainingBytes -= 2;
    if (numberOfParameters < 0) {
      throw protocolViolation(
          String.format("invalid number of parameters: %d", numberOfParameters));
    }
    if (4L * numberOfParameters > this.remainingBytes) {
      throw protocolViolation(
          String.format(
              "number of parameters (%d) exceeds remaining message length (%d)",
              numberOfParameters, this.remainingBytes));
    }
    if (numberOfParameters == 0) {
      return EMPTY_PARAMETER_TYPES;
    }
    int[] parameterDataTypes = new int[numberOfParameters];
    for (int index = 0; index < numberOfParameters; index++) {
      parameterDataTypes[index] = this.inputStream.readInt();
    }
    this.remainingBytes -= 4 * numberOfParameters;
    return parameterDataTypes;
  }

  protected void checkRemainingBytes(int expectedRemaining, String messageType) {
    if (this.remainingBytes != expectedRemaining) {
      throw protocolViolation(
          String.format(
              "invalid length of %s: expected %d trailing bytes, got %d",
              messageType, expectedRemaining, this.remainingBytes));
    }
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
   * as bytes arrive so an unfulfilled frame header cannot pre-allocate a large buffer.
   */
  protected byte[] readBytes(int numBytes) throws IOException {
    validateReadLength(numBytes, 0);
    return readBytes(this.inputStream, numBytes);
  }

  private static byte[] readBytes(DataInputStream input, int numBytes) throws IOException {
    if (numBytes == 0) {
      return EMPTY_BYTE_ARRAY;
    }
    if (numBytes <= READ_CHUNK_SIZE) {
      byte[] buffer = new byte[numBytes];
      input.readFully(buffer);
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
      input.readFully(buffer, offset, targetLength - offset);
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
    int maxReadLimit = Math.min(getMaxMessageLength(), this.length);
    byte[] bytes = readNullTerminatedBytes(maxReadLimit, false, null);
    return bytes.length == 0 ? "" : new String(bytes, StandardCharsets.UTF_8);
  }

  private byte[] readBoundedNullTerminatedBytes(int maxBytes, String messageType)
      throws IOException {
    if (maxBytes < 1) {
      throw protocolViolation(String.format("invalid length of %s: %d", messageType, this.length));
    }
    return readNullTerminatedBytes(maxBytes, true, messageType);
  }

  private byte[] readNullTerminatedBytes(
      int maxBytes, boolean throwProtocolViolationOnLimit, String messageType) throws IOException {
    if (maxBytes < 1) {
      throw new IOException("Invalid message length: " + maxBytes);
    }
    byte firstByte = this.inputStream.readByte();
    if (firstByte == 0) {
      return EMPTY_BYTE_ARRAY;
    }
    if (maxBytes == 1) {
      throw stringLimitExceeded(maxBytes, throwProtocolViolationOnLimit, messageType);
    }
    int maxStringBytes = maxBytes - 1;
    byte[] buffer = new byte[Math.min(INITIAL_STRING_BUFFER_SIZE, maxStringBytes)];
    buffer[0] = firstByte;
    int index = 1;
    while (true) {
      byte currentByte = this.inputStream.readByte();
      if (currentByte == 0) {
        return index == buffer.length ? buffer : Arrays.copyOf(buffer, index);
      }
      if (index == maxStringBytes) {
        throw stringLimitExceeded(maxBytes, throwProtocolViolationOnLimit, messageType);
      }
      if (index == buffer.length) {
        int nextCapacity = buffer.length <= maxStringBytes / 2 ? buffer.length * 2 : maxStringBytes;
        buffer = Arrays.copyOf(buffer, nextCapacity);
      }
      buffer[index++] = currentByte;
    }
  }

  private static IOException stringLimitExceeded(
      int maxBytes, boolean throwProtocolViolationOnLimit, String messageType) {
    if (throwProtocolViolationOnLimit) {
      throw protocolViolation(
          String.format(
              "invalid string in %s: no null terminator found within %d bytes",
              messageType, maxBytes));
    }
    return new IOException("No null terminator found");
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

  protected static PGException protocolViolation(String message) {
    return PGException.newBuilder(message)
        .setSeverity(Severity.FATAL)
        .setSQLState(SQLState.ProtocolViolation)
        .build();
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
      throw protocolViolation(String.format("invalid length of %s: %d", messageType, length));
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
    throw protocolViolation(String.format("invalid length of %s: %d", messageType, length));
  }
}
