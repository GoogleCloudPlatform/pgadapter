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
import com.google.cloud.spanner.pgadapter.error.PGException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

/** Sends error information back to client. */
@InternalApi
public class ErrorResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int FIELD_IDENTIFIER_LENGTH = 1;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte CODE_FLAG = 'C';
  private static final byte MESSAGE_FLAG = 'M';
  private static final byte SEVERITY_FLAG = 'S';
  private static final byte NULL_TERMINATOR = 0;

  private final byte[] severity;
  private final byte[] errorMessage;
  private final byte[] errorState;

  public ErrorResponse(DataOutputStream output, PGException pgException) {
    super(
        output,
        HEADER_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + pgException.getSeverity().name().getBytes(StandardCharsets.UTF_8).length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + pgException.getSQLState().getBytes().length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + pgException.getMessage().getBytes(StandardCharsets.UTF_8).length
            + NULL_TERMINATOR_LENGTH
            + NULL_TERMINATOR_LENGTH);
    this.errorMessage = pgException.getMessage().getBytes(StandardCharsets.UTF_8);
    this.errorState = pgException.getSQLState().getBytes();
    this.severity = pgException.getSeverity().name().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeByte(SEVERITY_FLAG);
    this.outputStream.write(severity);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(CODE_FLAG);
    this.outputStream.write(this.errorState);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(MESSAGE_FLAG);
    this.outputStream.write(this.errorMessage);
    this.outputStream.writeByte(NULL_TERMINATOR);
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
    return new MessageFormat("Length: {0}, " + "Error Message: {1}")
        .format(new Object[] {this.length, new String(this.errorMessage, UTF8)});
  }
}
