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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.ParseCompleteResponse;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.text.MessageFormat;

/** Creates a prepared statement. */
@InternalApi
public class ParseMessage extends ControlMessage {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
  protected static final char IDENTIFIER = 'P';

  private final String name;
  private final IntermediatePreparedStatement statement;
  private final int[] parameterDataTypes;

  public ParseMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.name = this.readString();
    ParsedStatement parsedStatement = PARSER.parse(Statement.of(this.readString()));
    ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    short numberOfParameters = this.inputStream.readShort();
    this.parameterDataTypes = new int[numberOfParameters];
    for (int i = 0; i < numberOfParameters; i++) {
      parameterDataTypes[i] = this.inputStream.readInt();
    }
    this.statement =
        new IntermediatePreparedStatement(
            connection, connection.getServer().getOptions(), parsedStatement);
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
