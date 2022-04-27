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
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import java.text.MessageFormat;

/**
 * Handles a sync command from the user. JDBC does not require this step, so server-side this is a
 * noop.
 */
@InternalApi
public class SyncMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'S';

  public SyncMessage(ConnectionHandler connection) throws Exception {
    super(connection);
  }

  @Override
  protected void sendPayload() throws Exception {
    new ReadyResponse(this.outputStream, connection.getStatus().getReadyResponseStatus()).send();
  }

  @Override
  protected String getMessageName() {
    return "Sync";
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
