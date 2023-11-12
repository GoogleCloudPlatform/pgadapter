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
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.CloseCompleteResponse;
import java.text.MessageFormat;

/** Close the designated statement. */
@InternalApi
public class CloseMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'C';

  private PreparedType type;
  private String name;
  private IntermediateStatement statement;

  public CloseMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.type = PreparedType.prepareType((char) this.inputStream.readUnsignedByte());
    this.name = this.readAll();
    if (this.type == PreparedType.Statement) {
      this.statement = this.connection.getStatement(this.name);
    } else {
      this.statement = this.connection.getPortal(this.name);
    }
  }

  /** Close the statement server-side and clean up by deleting their metdata locally. */
  @Override
  protected void sendPayload() throws Exception {
    if (this.type == PreparedType.Portal) {
      this.statement.close(); // Only portals need to be closed server side, since PS is not bound
      this.connection.closePortal(this.name);
    } else {
      this.connection.closeStatement(this.name);
    }
    new CloseCompleteResponse(this.outputStream).send();
  }

  @Override
  protected String getMessageName() {
    return "Close";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Name: {1}, " + "Type: {2}")
        .format(new Object[] {this.length, this.name, this.type.toString()});
  }

  @Override
  public String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public String getName() {
    return this.name;
  }

  public PreparedType getType() {
    return this.type;
  }

  @Override
  protected int getHeaderLength() {
    return 5;
  }
}
