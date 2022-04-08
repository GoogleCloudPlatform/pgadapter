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
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.NoDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ParameterDescriptionResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.RowDescriptionResponse;
import java.text.MessageFormat;

/** Calls describe on a portal or prepared statement. */
public class DescribeMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'D';

  private PreparedType type;
  private String name;
  private IntermediateStatement statement;

  public DescribeMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.type = PreparedType.prepareType((char) this.inputStream.readUnsignedByte());
    this.name = this.readAll();
    if (this.type == PreparedType.Portal) {
      this.statement = this.connection.getPortal(this.name);
    } else {
      this.statement = this.connection.getStatement(this.name);
    }
  }

  @Override
  protected void sendPayload() throws Exception {
    if (this.type == PreparedType.Portal) {
      this.handleDescribePortal();
    } else {
      this.handleDescribeStatement();
    }
  }

  @Override
  protected String getMessageName() {
    return "Describe";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Type: {1}, " + "Name: {2}")
        .format(new Object[] {this.length, this.type.toString(), this.name});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getName() {
    return this.name;
  }

  @Override
  protected int getHeaderLength() {
    return 5;
  }

  /**
   * Called when a describe message of type 'P' is received.
   *
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleDescribePortal() throws Exception {
    if (this.statement.hasException(0)) {
      this.handleError(this.statement.getException(0));
    } else {
      switch (this.statement.getStatementType(0)) {
        case UPDATE:
        case DDL:
        case CLIENT_SIDE:
          new NoDataResponse(this.outputStream).send();
          break;
        case QUERY:
          new RowDescriptionResponse(
                  this.outputStream,
                  this.statement,
                  ((DescribePortalMetadata) this.statement.describe()).getMetadata(),
                  this.connection.getServer().getOptions(),
                  QueryMode.EXTENDED)
              .send();
          break;
      }
    }
  }

  /**
   * Called when a describe message of type 'S' is received.
   *
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleDescribeStatement() throws Exception {
    DescribeStatementMetadata metadata = (DescribeStatementMetadata) this.statement.describe();
    new ParameterDescriptionResponse(this.outputStream, metadata.getParameters()).send();
    if (metadata.getResultSet() != null) {
      new RowDescriptionResponse(
              this.outputStream,
              this.statement,
              metadata.getResultSet(),
              this.connection.getServer().getOptions(),
              QueryMode.EXTENDED)
          .send();
    } else {
      new NoDataResponse(this.outputStream).send();
    }
  }
}
