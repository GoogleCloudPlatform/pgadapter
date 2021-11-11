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

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Generic representation for a wire message, generally executed by calling send. */
public abstract class WireMessage {

  private static final Logger logger = Logger.getLogger(WireMessage.class.getName());

  protected int length;
  protected DataInputStream inputStream;
  protected DataOutputStream outputStream;
  protected ConnectionHandler connection;

  public WireMessage(ConnectionHandler connection, int length) {
    this.connection = connection;
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
    logger.log(Level.FINE, this.toString());
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
  protected abstract String getIdentifier();

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
    return read(this.length - this.getHeaderLength());
  }

  /**
   * Reads a fixed-length string from the saved stream. The string still needs to be
   * null-terminated, but the read is more efficient, as we can read the entire string in one go
   * instead of continuously checking for a null-terminator.
   *
   * @param length The number of bytes to read.
   * @return the string.
   * @throws IOException if an error occurs while reading from the stream, or if no null-terminator
   *     is found at the end of the string.
   */
  public String read(int length) throws IOException {
    byte[] buffer = new byte[length - 1];
    this.inputStream.readFully(buffer);
    byte zero = inputStream.readByte();
    if (zero != (byte) 0) {
      throw new IOException("String was not null-terminated");
    }
    return new String(buffer, StandardCharsets.UTF_8);
  }

  /**
   * Reads a null-terminated string from a {@link DataInputStream}. Note that though existing
   * solutions for this exist, they are either not keyed exactly for our use case, or would lead to
   * a more combersome addition to this codebase. Also note the 128 byte length is chosen from
   * profiling and determining that it exceeds the 90th percentile size for inbound messages.
   *
   * @return the string.
   * @throws IOException if an error occurs while reading from the stream, or if no null-terminator
   *     is found before the end of the stream.
   */
  public String readString() throws IOException {
    byte[] buffer = new byte[128];
    int index = 0;
    while (true) {
      byte b = this.inputStream.readByte();
      if (b == 0) {
        break;
      }
      buffer[index] = b;
      index++;
      if (index == buffer.length) {
        buffer = Arrays.copyOf(buffer, buffer.length * 2);
      }
    }
    return new String(buffer, 0, index, StandardCharsets.UTF_8);
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
    this.connection.setMessageState(ControlMessage.create(this.connection));
  }
}
