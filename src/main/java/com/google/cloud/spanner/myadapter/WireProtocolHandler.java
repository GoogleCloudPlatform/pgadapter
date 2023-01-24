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

package com.google.cloud.spanner.myadapter;

import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.myadapter.command.CommandHandler;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.session.ProtocolStatus;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.wireinput.ClientHandshakeMessage;
import com.google.cloud.spanner.myadapter.wireinput.HeaderMessage;
import com.google.cloud.spanner.myadapter.wireinput.PingMessage;
import com.google.cloud.spanner.myadapter.wireinput.QueryMessage;
import com.google.cloud.spanner.myadapter.wireinput.ServerHandshakeMessage;
import com.google.cloud.spanner.myadapter.wireinput.TerminateMessage;
import java.io.EOFException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles wire messages from the client, and initiates the correct workflow based on the current
 * protocol status and the incoming packet.
 */
public class WireProtocolHandler {

  private static final Logger logger = Logger.getLogger(WireProtocolHandler.class.getName());

  private final ConnectionMetadata connectionMetadata;
  private final CommandHandler commandHandler;
  private final SessionState sessionState;
  private final BackendConnection backendConnection;

  public WireProtocolHandler(
      ConnectionMetadata connectionMetadata,
      SessionState sessionState,
      BackendConnection backendConnection,
      OptionsMetadata optionsMetadata) {
    this.backendConnection = backendConnection;
    this.commandHandler =
        new CommandHandler(connectionMetadata, sessionState, backendConnection, optionsMetadata);
    this.connectionMetadata = connectionMetadata;
    this.sessionState = sessionState;
  }

  public void run() throws Exception {
    try {
      commandHandler.processMessage(ServerHandshakeMessage.getInstance());
      while (sessionState.getProtocolStatus() != ProtocolStatus.TERMINATED) {
        processNextMessage();
        if (sessionState.getProtocolStatus() == ProtocolStatus.AUTHENTICATED) {
          logger.log(Level.INFO, "Client authentication established!");
          backendConnection.connectToSpanner("test", null);
          sessionState.setProtocolStatus(ProtocolStatus.QUERY_WAIT);
        }
      }
    } finally {
      terminate();
    }
  }

  private void processNextMessage() throws Exception {
    HeaderMessage headerMessage;
    try {
      headerMessage = parseHeaderMessage(connectionMetadata);
    } catch (EOFException e) {
      sessionState.setProtocolStatus(ProtocolStatus.TERMINATED);
      return;
    }
    switch (sessionState.getProtocolStatus()) {
      case SERVER_GREETINGS_SENT:
        logger.log(Level.FINE, "Processing client handshake!");
        ClientHandshakeMessage clientHandshakeMessage = new ClientHandshakeMessage(headerMessage);
        commandHandler.processMessage(clientHandshakeMessage);
        break;
      case QUERY_WAIT:
        logger.log(Level.FINE, "Processing next command!");
        nextCommandMessage(headerMessage);
        break;
      default:
        throw new Exception("Illegal protocol message state");
    }
  }

  private void nextCommandMessage(HeaderMessage headerMessage) throws Exception {
    int nextCommand = headerMessage.getBufferedInputStream().read();
    switch (nextCommand) {
      case QueryMessage.IDENTIFIER:
        logger.log(Level.FINE, "Query received!");
        QueryMessage queryMessage = new QueryMessage(headerMessage);
        commandHandler.processMessage(queryMessage);
        break;
      case PingMessage.IDENTIFIER:
        logger.log(Level.FINE, "Ping received!");
        PingMessage pingMessage = new PingMessage(headerMessage);
        commandHandler.processMessage(pingMessage);
        break;
      case TerminateMessage.IDENTIFIER:
        logger.log(Level.INFO, "Terminate message received.");
        TerminateMessage terminateMessage = new TerminateMessage(headerMessage);
        commandHandler.processMessage(terminateMessage);
        break;
      default:
        throw new Exception("Unknown command");
    }
  }

  private void terminate() {
    // TO-DO Destroy any thread.
    commandHandler.terminate();
  }

  private HeaderMessage parseHeaderMessage(ConnectionMetadata connectionMetadata)
      throws IOException {
    return HeaderMessage.create(connectionMetadata.getInputStream());
  }
}
