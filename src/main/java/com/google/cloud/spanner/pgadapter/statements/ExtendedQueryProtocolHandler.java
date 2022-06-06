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
import java.util.LinkedList;

public class ExtendedQueryProtocolHandler {
  private final LinkedList<AbstractQueryProtocolMessage> messages = new LinkedList<>();
  private final BackendConnection backendConnection;

  public ExtendedQueryProtocolHandler(ConnectionHandler connectionHandler) {
    this.backendConnection =
        new BackendConnection(
            connectionHandler.getSpannerConnection(),
            connectionHandler.getServer().getOptions().getDdlTransactionMode());
  }

  @VisibleForTesting
  public ExtendedQueryProtocolHandler(BackendConnection backendConnection) {
    this.backendConnection = backendConnection;
  }

  public BackendConnection getBackendConnection() {
    return backendConnection;
  }

  public void buffer(AbstractQueryProtocolMessage message) {
    messages.add(message);
  }

  public void flush() throws Exception {
    backendConnection.flush();
    flushMessages();
  }

  public void sync() throws Exception {
    backendConnection.sync();
    flushMessages();
  }

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
