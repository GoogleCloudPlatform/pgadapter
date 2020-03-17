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
import com.google.cloud.spanner.pgadapter.PGWireProtocol;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import java.io.DataInputStream;
import java.util.List;

/**
 * Message of type bind (meaning that it is a message called to a prepared statement to complete it
 * with missing data to prepare for execution). A bound prepared statement always yields a portal
 * unless it fails.
 */
public class BindMessage extends WireMessage {

  private String portalName;
  private String statementName;
  private List<Short> formatCodes;
  private List<Short> resultFormatCodes;
  private byte[][] parameters;
  private IntermediatePreparedStatement statement;

  public BindMessage(ConnectionHandler connection, DataInputStream input) throws Exception {
    super(connection, input);
    this.portalName = PGWireProtocol.readString(input);
    this.statementName = PGWireProtocol.readString(input);
    this.formatCodes = getFormatCodes(input);
    this.parameters = getParameters(input);
    this.resultFormatCodes = getFormatCodes(input);
    this.statement = connection.getStatement(this.statementName);
  }

  /**
   * Given the prepared statement, bind it and save it locally.
   *
   * @throws Exception If the binding fails.
   */
  @Override
  public void send() throws Exception {
    this.connection.registerPortal(
        this.portalName,
        this.statement.bind(this.parameters, this.formatCodes, this.resultFormatCodes));
    this.connection.handleBind();
  }

  public String getPortalName() {
    return this.portalName;
  }

  public String getStatementName() {
    return this.statementName;
  }

  public byte[][] getParameters() {
    return this.parameters;
  }

  public List<Short> getFormatCodes() {
    return this.formatCodes;
  }

  public List<Short> getResultFormatCodes() {
    return this.resultFormatCodes;
  }
}
