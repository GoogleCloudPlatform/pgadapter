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
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import java.text.MessageFormat;

/**
 * Normally used to send data to the back-end. Spanner does not currently support this, so send will
 * throw a descriptive error to be sent to the user. Note that we do parse this in order for this to
 * be future proof, and to ensure the input stream is flushed of the command (in order to continue
 * receiving properly)
 */
@InternalApi
public class CopyDataMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'd';

  private final byte[] payload;
  private final CopyStatement statement;

  public CopyDataMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    // Payload byte array excluding 4 bytes containing the length of message itself
    int dataLength = this.length - 4;
    this.payload = new byte[dataLength];
    this.inputStream.readFully(this.payload);
    this.statement = connection.getActiveCopyStatement();
  }

  @Override
  protected void sendPayload() throws Exception {
    if (statement == null) {
      // Do not handle this message if there is no CopyStatement available anymore. This means that
      // the copy operation failed and stopped while the client was still sending data to the
      // server.
      return;
    }
    // If backend error occurred during copy-in mode, drop any subsequent CopyData messages.
    MutationWriter mutationWriter = this.statement.getMutationWriter();
    if (!statement.hasException()) {
      try {
        mutationWriter.addCopyData(this.payload);
      } catch (PGException exception) {
        statement.handleExecutionException(exception);
        throw exception;
      }
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
