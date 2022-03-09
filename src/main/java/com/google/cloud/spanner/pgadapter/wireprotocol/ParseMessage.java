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
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import com.google.cloud.spanner.pgadapter.wireoutput.ParseCompleteResponse;
import com.google.common.base.Strings;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/** Creates a prepared statement. */
public class ParseMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'P';

  private String name;
  private IntermediatePreparedStatement statement;
  private List<Integer> parameterDataTypes;

  public ParseMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.name = this.readString();
    String queryString = StatementParser.removeCommentsAndTrim(this.readString());
    this.parameterDataTypes = new ArrayList<>();
    short numberOfParameters = this.inputStream.readShort();
    for (int i = 0; i < numberOfParameters; i++) {
      int type = this.inputStream.readInt();
      this.parameterDataTypes.add(type);
    }
    this.statement =
        new IntermediatePreparedStatement(
            connection.getServer().getOptions(), queryString, connection.getSpannerConnection());
    this.statement.setParameterDataTypes(this.parameterDataTypes);
  }

  @Override
  protected void sendPayload() throws Exception {
    if (!Strings.isNullOrEmpty(this.name) && this.connection.hasStatement(this.name)) {
      throw new IllegalStateException("Must close statement before reusing name.");
    }
    this.connection.registerStatement(this.name, this.statement);
    new ParseCompleteResponse(this.outputStream).send();
  }

  @Override
  protected String getMessageName() {
    return "Parse";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, Name: {1},  SQL: {2}, Parameters: {3}")
        .format(
            new Object[] {
              this.length, this.name, this.statement.getSql(), this.parameterDataTypes
            });
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public IntermediatePreparedStatement getStatement() {
    return this.statement;
  }

  public String getName() {
    return this.name;
  }
}
