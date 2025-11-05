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
import java.util.Arrays;

/**
 * This message handles the imperative cancellation, as issues in a new connection by the PG wire
 * protocol. We expect that this message contains an ID for the connection which issues the original
 * query, as well as an auth secret.
 *
 * <p>In protocol 3.0, the secret is a 4-byte integer.
 *
 * <p>In protocol 3.2, the secret is a 32-byte array.
 */
@InternalApi
public class CancelMessage extends BootstrapMessage {

  private static final int PROTOCOL_3_0_MESSAGE_LENGTH = 16;
  public static final int IDENTIFIER = 80877102; // First Hextet: 1234, Second Hextet: 5678

  private final int connectionId;
  private final int secret; // For protocol 3.0
  private final byte[] secretBytes; // For protocol 3.2

  public CancelMessage(ConnectionHandler connection, int length) throws Exception {
    super(connection, length);
    this.connectionId = this.inputStream.readInt();
    if (length == PROTOCOL_3_0_MESSAGE_LENGTH) {
      this.secret = this.inputStream.readInt();
      this.secretBytes = null;
    } else {
      this.secretBytes = new byte[ConnectionHandler.DEFAULT_KEY_LENGTH_BYTES];
      this.inputStream.readFully(secretBytes);
      this.secret = -1;
    }
  }

  @Override
  protected void sendPayload() throws Exception {
    if (this.secretBytes != null) {
      this.connection.cancelActiveStatement(this.connectionId, this.secretBytes);
    } else {
      this.connection.cancelActiveStatement(this.connectionId, this.secret);
    }
    this.connection.handleTerminate();
  }

  @Override
  protected String getMessageName() {
    return "Cancel";
  }

  @Override
  protected String getPayloadString() {
    if (this.secretBytes != null) {
      return new MessageFormat("Length: {0}, Connection ID: {1}, Secret: {2}")
          .format(new Object[] {this.length, this.connectionId, Arrays.toString(this.secretBytes)});
    } else {
      return new MessageFormat("Length: {0}, Connection ID: {1}, Secret: {2}")
          .format(new Object[] {this.length, this.connectionId, this.secret});
    }
  }

  @Override
  public String getIdentifier() {
    return Integer.toString(IDENTIFIER);
  }

  public int getConnectionId() {
    return connectionId;
  }

  public int getSecret() {
    return secret;
  }

  public byte[] getSecretBytes() {
    return secretBytes;
  }
}
