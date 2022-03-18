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

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * Normally used to send data to the back-end. Spanner does not currently support this, so send will
 * throw a descriptive error to be sent to the user. Note that we do parse this in order for this to
 * be future proof, and to ensure the input stream is flushed of the command (in order to continue
 * receiving properly)
 */
public class CopyDataMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'd';

  private byte[] payload;
  private CopyStatement statement;

  public CopyDataMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    // Payload byte array excluding 4 bytes containing the length of message itself
    this.payload = new byte[this.length - 4];
    if (this.inputStream.read(this.payload) < 0) {
      throw new IOException("Could not read copy data.");
    }
    this.statement = (CopyStatement) connection.getActiveStatement();
  }

  @Override
  protected void sendPayload() throws Exception {
    if (statement == null) {
      throw new IllegalStateException("This connection is no (longer) in COPY IN mode.");
    }
    // If backend error occurred during copy-in mode, drop any subsequent CopyData messages.
    MutationWriter mutationWriter = this.statement.getMutationWriter();
    if (!statement.hasException()) {
      try {
        mutationWriter.addCopyData(this.payload);
      } catch (SpannerException exception) {
        mutationWriter.writeErrorFile(exception);
        statement.handleExecutionException(exception);
        throw exception;
      }
    } else {
      // If we get another CopyData message after we have already failed, then we drop the
      // connection to ensure that the client is not overflowing us with messages. Some clients do
      // not check for error messages during a COPY operation.
      connection.setStatus(ConnectionStatus.TERMINATED);
      throw statement.getException();
    }
  }

  @Override
  protected String getMessageName() {
    return "Copy Data";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[] {this.length});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public byte[] getPayload() {
    return this.payload;
  }
}
