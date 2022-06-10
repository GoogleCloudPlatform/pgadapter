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
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.wireoutput.AuthenticationCleartextPasswordResponse;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The first (non-encryption, non-admin) message expected in from a client in a connection loop.
 * Here we handle metadata and authentication if any.
 */
@InternalApi
public class StartupMessage extends BootstrapMessage {

  static final String DATABASE_KEY = "database";
  public static final int IDENTIFIER = 196608; // First Hextet: 3 (version), Second Hextet: 0

  private final boolean authenticate;
  private final Map<String, String> parameters;

  public StartupMessage(ConnectionHandler connection, int length) throws Exception {
    super(connection, length);
    this.authenticate = connection.getServer().getOptions().shouldAuthenticate();
    this.parameters = this.parseParameters(this.readAll());
  }

  @Override
  protected void sendPayload() throws Exception {
    if (!authenticate) {
      createConnectionAndSendStartupMessage(
          this.connection, this.parameters.get(DATABASE_KEY), null);
    } else {
      new AuthenticationCleartextPasswordResponse(this.outputStream).send();
    }
  }

  static void createConnectionAndSendStartupMessage(
      ConnectionHandler connection, String database, @Nullable GoogleCredentials credentials)
      throws Exception {
    connection.connectToSpanner(database, credentials);
    sendStartupMessage(
        connection.getConnectionMetadata().getOutputStream(),
        connection.getConnectionId(),
        connection.getSecret(),
        connection.getServer().getOptions());
    connection.setStatus(ConnectionStatus.IDLE);
  }

  /**
   * Here we expect the nextHandler to be {@link PasswordMessage} if we authenticate. Otherwise
   * default.
   *
   * @return PasswordMessage if auth is set to true, else default.
   * @throws Exception
   */
  @Override
  public void nextHandler() throws Exception {
    if (authenticate) {
      char protocol = (char) inputStream.readUnsignedByte();
      if (protocol != PasswordMessage.IDENTIFIER) {
        throw new IOException(
            "Unexpected response, expected '"
                + PasswordMessage.IDENTIFIER
                + "', but got: "
                + protocol);
      }
      this.connection.setMessageState(new PasswordMessage(this.connection, this.parameters));
    } else {
      super.nextHandler();
    }
  }

  @Override
  protected int getHeaderLength() {
    return 8;
  }

  @Override
  protected String getMessageName() {
    return "Start-Up";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Parameters: {1}")
        .format(new Object[] {this.length, this.parameters.toString()});
  }

  @Override
  protected String getIdentifier() {
    return Integer.toString(IDENTIFIER);
  }

  public Map<String, String> getParameters() {
    return parameters;
  }
}
