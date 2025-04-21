// Copyright 2025 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement.NO_FORMAT_CODES;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import java.io.DataInputStream;
import java.io.IOException;

/** This class reads the next control message for a connection. */
@InternalApi
public class MessageReader {
  /** Maximum number of invalid messages in a row allowed before we terminate the connection. */
  static final int MAX_INVALID_MESSAGE_COUNT = 50;

  private final OptionsMetadata options;

  public MessageReader(OptionsMetadata options) {
    this.options = options;
  }

  /**
   * Factory method to create the next control message for the given connection.
   *
   * @param connection The connection handler object setup with the ability to send/receive.
   * @return The constructed wire message given the input message.
   * @throws Exception If construction or reading fails.
   */
  public ControlMessage create(ConnectionHandler connection) throws Exception {
    boolean validMessage = true;
    char nextMsg = readNextMsgIdentifier(connection);
    try {
      if (connection.getStatus() == ConnectionStatus.COPY_IN) {
        switch (nextMsg) {
          case CopyDoneMessage.IDENTIFIER:
            return new CopyDoneMessage(connection);
          case CopyDataMessage.IDENTIFIER:
            return new CopyDataMessage(connection);
          case CopyFailMessage.IDENTIFIER:
            return new CopyFailMessage(connection);
          case SyncMessage.IDENTIFIER:
          case FlushMessage.IDENTIFIER:
            // Skip sync/flush in COPY_IN. This is consistent with real PostgreSQL which also does
            // this to accommodate clients that do not check what type of statement they sent in an
            // ExecuteMessage, and instead always blindly send a flush/sync after each execute.
            return SkipMessage.createForValidStream(connection);
          default:
            // Skip other unexpected messages and throw an exception to fail the copy operation.
            validMessage = false;
            SkipMessage.createForInvalidStream(connection);
            throw new IllegalStateException(
                String.format(
                    "Expected CopyData ('d'), CopyDone ('c') or CopyFail ('f') messages, got: '%c'",
                    nextMsg));
        }
      } else {
        switch (nextMsg) {
          case QueryMessage.IDENTIFIER:
            return new QueryMessage(connection);
          case ParseMessage.IDENTIFIER:
            return new ParseMessage(connection);
          case BindMessage.IDENTIFIER:
            return new BindMessage(connection);
          case DescribeMessage.IDENTIFIER:
            return new DescribeMessage(connection);
          case ExecuteMessage.IDENTIFIER:
            return new ExecuteMessage(connection);
          case CloseMessage.IDENTIFIER:
            return new CloseMessage(connection);
          case TerminateMessage.IDENTIFIER:
            return new TerminateMessage(connection);
          case FunctionCallMessage.IDENTIFIER:
            return new FunctionCallMessage(connection);
          case FlushMessage.IDENTIFIER:
            return new FlushMessage(connection);
          case SyncMessage.IDENTIFIER:
            return new SyncMessage(connection);
          case CopyDoneMessage.IDENTIFIER:
          case CopyDataMessage.IDENTIFIER:
          case CopyFailMessage.IDENTIFIER:
            // Silently skip COPY messages in non-COPY mode. This is consistent with the PG wire
            // protocol. If we continue to receive COPY messages while in non-COPY mode, we'll
            // terminate the connection to prevent the server from being flooded with invalid
            // messages.
            validMessage = false;
            // Note: The stream itself is still valid as we received a message that we recognized.
            return SkipMessage.createForValidStream(connection);
          default:
            throw new IllegalStateException(String.format("Unknown message: %c", nextMsg));
        }
      }
    } finally {
      if (validMessage) {
        connection.clearInvalidMessageCount();
      } else {
        connection.increaseInvalidMessageCount();
        if (connection.getInvalidMessageCount() > MAX_INVALID_MESSAGE_COUNT) {
          new ErrorResponse(
                  connection,
                  PGException.newBuilder(
                          String.format(
                              "Received %d invalid/unexpected messages. Last received message: '%c'",
                              connection.getInvalidMessageCount(), nextMsg))
                      .setSQLState(SQLState.ProtocolViolation)
                      .setSeverity(Severity.FATAL)
                      .build())
              .send();
          connection.setStatus(ConnectionStatus.TERMINATED);
        }
      }
    }
  }

  /** Reads the next control message identifier. */
  private char readNextMsgIdentifier(ConnectionHandler connection) throws IOException {
    DataInputStream inputStream = connection.getConnectionMetadata().getInputStream();
    return (char) inputStream.readUnsignedByte();
  }

  /**
   * Read the format codes from the current message (useful for both input and output format codes).
   *
   * @param input The data stream for the connection.
   * @return A list of format codes.
   * @throws Exception If reading fails in any way.
   */
  static short[] getFormatCodes(DataInputStream input) throws Exception {
    short numberOfFormatCodes = input.readShort();
    if (numberOfFormatCodes == 0) {
      return NO_FORMAT_CODES;
    }
    short[] formatCodes = new short[numberOfFormatCodes];
    for (int i = 0; i < numberOfFormatCodes; i++) {
      formatCodes[i] = input.readShort();
    }
    return formatCodes;
  }
}
