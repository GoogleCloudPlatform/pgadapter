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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.ExtendedQueryProtocolHandler;
import java.io.IOException;

/**
 * Abstract base class for extended query protocol messages. These can be buffered in memory locally
 * until a flush or sync message is received. Any corresponding database statement will only be
 * executed once a flush/sync is received.
 */
@InternalApi
public abstract class AbstractQueryProtocolMessage extends ControlMessage {
  private final ExtendedQueryProtocolHandler handler;
  protected final QueryMode queryMode;
  private boolean returnedErrorResponse;

  AbstractQueryProtocolMessage(ConnectionHandler connection) throws IOException {
    super(connection);
    this.handler = connection.getExtendedQueryProtocolHandler();
    this.queryMode = QueryMode.EXTENDED;
  }

  AbstractQueryProtocolMessage(
      ConnectionHandler connection, int length, ManuallyCreatedToken manuallyCreatedToken) {
    super(connection, length, manuallyCreatedToken);
    this.handler = connection.getExtendedQueryProtocolHandler();
    this.queryMode = QueryMode.SIMPLE;
  }

  @Override
  protected final void sendPayload() throws Exception {
    this.buffer(handler.getBackendConnection());
    handler.buffer(this);
  }

  abstract void buffer(BackendConnection backendConnection) throws Exception;

  public abstract void flush() throws Exception;

  public abstract String getSql();

  public boolean isReturnedErrorResponse() {
    return returnedErrorResponse;
  }

  protected void handleError(Exception exception) throws Exception {
    super.handleError(exception);
    this.returnedErrorResponse = true;
  }
}
