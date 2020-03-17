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
import com.google.cloud.spanner.pgadapter.PGWireProtocol;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic representation for a wire message, generally executed by calling commit.
 */
public abstract class WireMessage {

  protected int length;
  protected int remainder;
  protected ConnectionHandler connection;

  public WireMessage(ConnectionHandler connection, DataInputStream input) throws IOException {
    this.length = input.readInt();
    this.connection = connection;
  }

  /**
   * Factory method to create the message from the specific command type char.
   *
   * @param connection The connection handler object setup with the ability to send/receive.
   * @param input The data inut stream containing the remaining data.
   * @return The constructed wire message given the input message.
   * @throws Exception If construction or reading fails.
   */
  public static WireMessage create(ConnectionHandler connection, DataInputStream input)
      throws Exception {
    char nextMsg = (char) input.readUnsignedByte();
    switch (nextMsg) {
      case 'Q':
        return new QueryMessage(connection, input);
      case 'P':
        return new ParseMessage(connection, input);
      case 'B':
        return new BindMessage(connection, input);
      case 'D':
        return new DescribeMessage(connection, input);
      case 'E':
        return new ExecuteMessage(connection, input);
      case 'C':
        return new CloseMessage(connection, input);
      case 'S':
        return new SyncMessage(connection, input);
      case 'X':
        return new TerminateMessage(connection, input);
      case 'c':
        return new CopyDoneMessage(connection, input);
      case 'd':
        return new CopyDataMessage(connection, input);
      case 'f':
        return new CopyFailMessage(connection, input);
      case 'F':
        return new FunctionCallMessage(connection, input);
      case 'H':
        return new FlushMessage(connection, input);
      default:
        throw new IllegalStateException("Unknown message");
    }
  }

  /**
   * Extract format codes from message (useful for both input and output format codes).
   *
   * @param input The data stream containing the user request.
   * @return A list of format codes.
   * @throws Exception If reading fails in any way.
   */
  static protected List<Short> getFormatCodes(DataInputStream input) throws Exception {
    List<Short> formatCodes = new ArrayList<>();
    short numberOfFormatCodes = input.readShort();
    for (int i = 0; i < numberOfFormatCodes; i++) {
      formatCodes.add(input.readShort());
    }
    return formatCodes;
  }

  /**
   * Extracts parameters/arguments from the user input.
   *
   * @param input The data stream containing the user request.
   * @return A byte array of user-defined parameters to be bound.
   * @throws Exception If reading fails in any way.
   */
  static protected byte[][] getParameters(DataInputStream input) throws Exception {
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
   * Once the message is ready, call this to send it across the wire.
   *
   * @throws Exception If the sending fails.
   */
  public abstract void send() throws Exception;

  /**
   * Read the remainder of the received message. To be called once metdata for the message is read.
   * Metadata is designated as type, length, etc.
   *
   * @param input The input stream containing the remainder of the message.
   * @return The remainder of the message in String format.
   * @throws Exception If reading fails in any way.
   */
  protected String read(DataInputStream input) throws Exception {
    return PGWireProtocol.readString(input, this.length - this.remainder);
  }


  public enum PreparedType {
    Portal,
    Statement;

    static PreparedType prepareType(char type) {
      switch (type) {
        case ('P'):
          return PreparedType.Portal;
        case ('S'):
          return PreparedType.Statement;
        default:
          throw new IllegalArgumentException("Unknown Statement type!");
      }
    }
  }
}
