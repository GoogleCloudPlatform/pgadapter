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

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.text.MessageFormat;

/** Executes a portal. */
public class ExecuteMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'E';

  private String name;
  private int maxRows;
  private IntermediateStatement statement;

  public ExecuteMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.name = this.readAll();
    this.maxRows = this.inputStream.readInt();
    this.statement = this.connection.getPortal(this.name);
  }

  @Override
  protected void sendPayload() throws Exception {
    this.statement.execute();
    this.handleExecute();
  }

  @Override
  protected String getMessageName() {
    return "Execute";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Name: {1}, " + "Max Rows: {2}")
        .format(new Object[] {this.length, this.name, this.maxRows});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getName() {
    return this.name;
  }

  public int getMaxRows() {
    return this.maxRows;
  }

  @Override
  protected int getHeaderLength() {
    return 8;
  }

  /**
   * Called when an execute message is received.
   *
   * @throws Exception if sending the message back to the client causes an error.
   */
  private void handleExecute() throws Exception {
    if (this.statement.hasException(0)) {
      this.handleError(this.statement.getException(0));
    } else {
      this.sendSpannerResult(0, this.statement, QueryMode.EXTENDED, this.maxRows);
      this.outputStream.flush();
    }
    this.connection.cleanUp(this.statement);
  }
}
