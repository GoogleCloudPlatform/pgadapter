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
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import java.text.MessageFormat;

/**
 * Normally used to signal a copy command is done. Spanner does not currently support copies, so
 * send will throw a descriptive error to be sent to the user. Note that we do parse this in order
 * for this to be future proof, and to ensure the input stream is flushed of the command (in order
 * to continue receiving properly)
 */
@InternalApi
public class CopyDoneMessage extends ControlMessage {
  protected static final char IDENTIFIER = 'c';
  private final CopyStatement statement;

  public CopyDoneMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.statement = (CopyStatement) connection.getActiveStatement();
  }

  @Override
  protected void sendPayload() throws Exception {
    // If backend error occurred during copy-in mode, drop any subsequent CopyDone messages.
    if (this.statement != null) {
      statement.getMutationWriter().commit();
      statement.close();
    }
    // Clear the COPY_IN status to indicate that we finished successfully. This will cause the
    // inline handling of incoming (copy) messages to stop and the server to resume normal
    // operation.
    this.connection.setStatus(ConnectionStatus.COPY_DONE);
  }

  @Override
  protected String getMessageName() {
    return "Copy Done";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[] {this.length});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }
}
