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
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.DataInputStream;

/**
 * Executes a portal.
 */
public class ExecuteMessage extends WireMessage {

  private String name;
  private int maxRows;
  private IntermediateStatement statement;

  public ExecuteMessage(ConnectionHandler connection, DataInputStream input) throws Exception {
    super(connection, input);
    this.remainder = 8;
    this.name = this.read(input);
    this.maxRows = input.readInt();
    this.statement = this.connection.getPortal(this.name);
  }

  @Override
  public void send() throws Exception {
    this.statement.execute();
    this.connection.handleExecute(this.statement, this.maxRows);
  }

  public String getName() {
    return this.name;
  }

  public int getMaxRows() {
    return this.maxRows;
  }

}
