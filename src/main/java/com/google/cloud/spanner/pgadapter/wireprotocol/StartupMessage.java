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
import com.google.auth.Credentials;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.wireoutput.AuthenticationCleartextPasswordResponse;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * The first (non-encryption, non-admin) message expected in from a client in a connection loop.
 * Here we handle metadata and authentication if any.
 */
@InternalApi
public class StartupMessage extends BootstrapMessage {
  private static final Logger logger = Logger.getLogger(StartupMessage.class.getName());
  static final String DATABASE_KEY = "database";
  private static final String USER_KEY = "user";
  public static final int IDENTIFIER = 196608; // First Hextet: 3 (version), Second Hextet: 0

  private final boolean authenticate;
  private final Map<String, String> parameters;

  public StartupMessage(ConnectionHandler connection, int length) throws Exception {
    super(connection, length);
    this.authenticate = connection.getServer().getOptions().shouldAuthenticate();
    String rawParameters = this.readAll();
    this.parameters = this.parseParameters(rawParameters);
    if (connection.getServer().getOptions().shouldAutoDetectClient()) {
      WellKnownClient wellKnownClient =
          ClientAutoDetector.detectClient(this.parseParameterKeys(rawParameters), this.parameters);
      connection.setWellKnownClient(wellKnownClient);
    } else {
      connection.setWellKnownClient(WellKnownClient.UNSPECIFIED);
    }
  }

  @Override
  protected void sendPayload() throws Exception {
    if (!authenticate) {
      createConnectionAndSendStartupMessage(
          this.connection, this.parameters.get(DATABASE_KEY), this.parameters, null);
    } else {
      new AuthenticationCleartextPasswordResponse(this.outputStream).send();
    }
  }

  static void createConnectionAndSendStartupMessage(
      ConnectionHandler connection,
      String database,
      Map<String, String> parameters,
      @Nullable Credentials credentials)
      throws Exception {
    connection.connectToSpanner(database, credentials);
    for (Entry<String, String> parameter :
        Iterables.concat(
            connection.getWellKnownClient().getDefaultParameters(parameters).entrySet(),
            parameters.entrySet())) {
      connection
          .getExtendedQueryProtocolHandler()
          .getBackendConnection()
          .initSessionSetting(parameter.getKey(), parameter.getValue());
    }
    sendStartupMessage(
        connection.getConnectionMetadata().getOutputStream(),
        connection.getConnectionId(),
        connection.getSecret(),
        connection.getExtendedQueryProtocolHandler().getBackendConnection().getSessionState(),
        connection.getWellKnownClient().createStartupNoticeResponses(connection));
    connection.setStatus(ConnectionStatus.AUTHENTICATED);
  }

  /** Here we expect the nextHandler to be {@link PasswordMessage} if we authenticate. */
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
