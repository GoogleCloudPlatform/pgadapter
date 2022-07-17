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
import java.text.MessageFormat;

/**
 * Handles a flush command from the user. This flushes any pending changes to Cloud Spanner and then
 * sends any pending messages to the client.
 */
@InternalApi
public class FlushMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'H';

  public FlushMessage(ConnectionHandler connection) throws Exception {
    super(connection);
  }

  public FlushMessage(ConnectionHandler connection, ManuallyCreatedToken manuallyCreatedToken) {
    super(connection, 4, manuallyCreatedToken);
  }

  @Override
  protected void sendPayload() throws Exception {
    // Pretend that this message is a sync if it is followed directly by a sync message.
    // This allows us to use a more efficient transaction type for single queries, as we know that
    // no other query will be following after the flush.
    char nextMessage = connection.getConnectionMetadata().peekNextByte();
    if (nextMessage == SyncMessage.IDENTIFIER) {
      connection.getExtendedQueryProtocolHandler().sync();
    } else {
      connection.getExtendedQueryProtocolHandler().flush();
    }
  }

  @Override
  protected String getMessageName() {
    return "Flush";
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
