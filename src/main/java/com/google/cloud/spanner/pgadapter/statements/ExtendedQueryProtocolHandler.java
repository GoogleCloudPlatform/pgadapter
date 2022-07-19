// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.wireprotocol.AbstractQueryProtocolMessage;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Handles the message flow for the extended query protocol. Wire-protocol messages are buffered in
 * memory until a flush/sync is received.
 */
public class ExtendedQueryProtocolHandler {
  private final LinkedList<AbstractQueryProtocolMessage> messages = new LinkedList<>();
  private final BackendConnection backendConnection;

  /** Creates an {@link ExtendedQueryProtocolHandler} for the given connection. */
  public ExtendedQueryProtocolHandler(ConnectionHandler connectionHandler) {
    this.backendConnection =
        new BackendConnection(
            connectionHandler.getDatabaseId(),
            connectionHandler.getSpannerConnection(),
            connectionHandler.getServer().getOptions(),
            connectionHandler.getWellKnownClient().getLocalStatements(connectionHandler));
  }

  /** Constructor only intended for testing. */
  @VisibleForTesting
  public ExtendedQueryProtocolHandler(BackendConnection backendConnection) {
    this.backendConnection = backendConnection;
  }

  /** Returns the backend PG connection for this query handler. */
  public BackendConnection getBackendConnection() {
    return backendConnection;
  }

  /** Returns a copy of the currently buffered messages in this handler. */
  @VisibleForTesting
  List<AbstractQueryProtocolMessage> getMessages() {
    return new ArrayList<>(messages);
  }

  /**
   * Buffer an extended query protocol message for execution when the next flush/sync message is
   * received.
   */
  public void buffer(AbstractQueryProtocolMessage message) {
    messages.add(message);
  }

  /**
   * Flushes the current queue of messages. Any pending database statements are first executed,
   * before sending the wire-protocol responses to the frontend. A flush does not commit the
   * implicit transaction (if any).
   */
  public void flush() throws Exception {
    backendConnection.flush();
    flushMessages();
  }

  /**
   * Flushes the current queue of messages and commits the implicit transaction (if any). Any
   * pending database statements are first executed, before sending the wire-protocol responses to
   * the frontend.
   */
  public void sync() throws Exception {
    backendConnection.sync();
    flushMessages();
  }

  /** Flushes the wire-protocol messages to the frontend. */
  private void flushMessages() throws Exception {
    try {
      for (AbstractQueryProtocolMessage message : messages) {
        message.flush();
        if (message.isReturnedErrorResponse()) {
          break;
        }
      }
    } finally {
      messages.clear();
    }
  }
}
