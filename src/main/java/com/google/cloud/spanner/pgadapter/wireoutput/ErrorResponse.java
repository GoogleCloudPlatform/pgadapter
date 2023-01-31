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

package com.google.cloud.spanner.pgadapter.wireoutput;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

/** Sends error information back to client. */
@InternalApi
public class ErrorResponse extends WireOutput {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private static final int HEADER_LENGTH = 4;
  private static final int FIELD_IDENTIFIER_LENGTH = 1;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte CODE_FLAG = 'C';
  private static final byte MESSAGE_FLAG = 'M';
  private static final byte SEVERITY_FLAG = 'S';
  private static final byte HINT_FLAG = 'H';
  private static final byte NULL_TERMINATOR = 0;

  private final PGException pgException;
  private final WellKnownClient client;

  public ErrorResponse(ConnectionHandler connection, PGException pgException) {
    super(
        connection.getConnectionMetadata().getOutputStream(),
        calculateLength(pgException, connection.getWellKnownClient()));
    this.pgException = pgException;
    this.client = connection.getWellKnownClient();
  }

  static int calculateLength(PGException pgException, WellKnownClient client) {
    int length =
        HEADER_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + convertSeverityToWireProtocol(pgException).length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + convertSQLStateToWireProtocol(pgException).length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + convertMessageToWireProtocol(pgException).length
            + NULL_TERMINATOR_LENGTH
            + NULL_TERMINATOR_LENGTH;
    byte[] hints = convertHintsToWireProtocol(pgException, client);
    if (hints.length > 0) {
      length += FIELD_IDENTIFIER_LENGTH + hints.length + NULL_TERMINATOR_LENGTH;
    }
    return length;
  }

  static byte[] convertSeverityToWireProtocol(PGException pgException) {
    return pgException.getSeverity().name().getBytes(StandardCharsets.UTF_8);
  }

  static byte[] convertSQLStateToWireProtocol(PGException pgException) {
    return pgException.getSQLState().getBytes();
  }

  static byte[] convertMessageToWireProtocol(PGException pgException) {
    return pgException.getMessage().getBytes(StandardCharsets.UTF_8);
  }

  static byte[] convertHintsToWireProtocol(PGException pgException, WellKnownClient client) {
    if (Strings.isNullOrEmpty(pgException.getHints())
        && client.getErrorHints(pgException).isEmpty()) {
      return EMPTY_BYTE_ARRAY;
    }
    String hints = "";
    if (!Strings.isNullOrEmpty(pgException.getHints())) {
      hints += pgException.getHints();
    }
    ImmutableList<String> clientHints = client.getErrorHints(pgException);
    if (!clientHints.isEmpty()) {
      if (hints.length() > 0) {
        hints += "\n";
      }
      hints += String.join("\n", clientHints);
    }
    return hints.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeByte(SEVERITY_FLAG);
    this.outputStream.write(convertSeverityToWireProtocol(pgException));
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(CODE_FLAG);
    this.outputStream.write(convertSQLStateToWireProtocol(pgException));
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(MESSAGE_FLAG);
    this.outputStream.write(convertMessageToWireProtocol(pgException));
    this.outputStream.writeByte(NULL_TERMINATOR);
    byte[] hints = convertHintsToWireProtocol(pgException, client);
    if (hints.length > 0) {
      this.outputStream.writeByte(HINT_FLAG);
      this.outputStream.write(hints);
      this.outputStream.writeByte(NULL_TERMINATOR);
    }
    this.outputStream.writeByte(NULL_TERMINATOR);
  }

  @Override
  public byte getIdentifier() {
    return 'E';
  }

  @Override
  protected String getMessageName() {
    return "Error";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, Error Message: {1}, Hints: {2}")
        .format(
            new Object[] {
              this.length,
              new String(convertMessageToWireProtocol(pgException), UTF8),
              convertHintsToWireProtocol(pgException, client).length == 0
                  ? ""
                  : new String(convertHintsToWireProtocol(pgException, client), UTF8)
            });
  }
}
