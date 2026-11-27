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
 * Handles a sync command from the user. JDBC does not require this step, so server-side this is a
 * noop.
 */
@InternalApi
public class SyncMessage extends ControlMessage {

  public static final char IDENTIFIER = 'S';
  private static final String MESSAGE_NAME = "Sync";
  // 4-byte length.
  private static final int MESSAGE_LENGTH = 4;

  public SyncMessage(ConnectionHandler connection) throws Exception {
    super(connection, MESSAGE_LENGTH, MESSAGE_LENGTH, MESSAGE_NAME);
  }

  public SyncMessage(ConnectionHandler connection, ManuallyCreatedToken manuallyCreatedToken) {
    super(connection, MESSAGE_LENGTH, manuallyCreatedToken);
  }

  @Override
  protected void sendPayload() throws Exception {
    connection.getExtendedQueryProtocolHandler().sync(true);
  }

  @Override
  protected String getMessageName() {
    return MESSAGE_NAME;
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[] {this.length});
  }

  @Override
  public String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }
}
