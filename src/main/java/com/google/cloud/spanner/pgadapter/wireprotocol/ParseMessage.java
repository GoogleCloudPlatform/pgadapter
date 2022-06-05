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
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.ParseCompleteResponse;
import com.google.common.base.Strings;
import java.text.MessageFormat;

/** Creates a prepared statement. */
@InternalApi
public class ParseMessage extends AbstractQueryProtocolMessage {
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
    short numberOfParameters = this.inputStream.readShort();
    this.parameterDataTypes = new int[numberOfParameters];
    for (int i = 0; i < numberOfParameters; i++) {
      parameterDataTypes[i] = this.inputStream.readInt();
    }
    this.statement =
        new IntermediatePreparedStatement(
            connection, connection.getServer().getOptions(), name, parsedStatement);
    this.statement.setParameterDataTypes(this.parameterDataTypes);
  }

  /**
   * Constructor for manually created Parse messages that originate from the simple query protocol.
   */
  public ParseMessage(ConnectionHandler connection, ParsedStatement parsedStatement) {
    super(
        connection,
        5 + parsedStatement.getSqlWithoutComments().length(),
        ManuallyCreatedToken.MANUALLY_CREATED_TOKEN);
    this.name = "";
    this.parameterDataTypes = new int[0];
    this.statement =
        new IntermediatePreparedStatement(
            connection, connection.getServer().getOptions(), name, parsedStatement);
    this.statement.setParameterDataTypes(this.parameterDataTypes);
  }

  @Override
  void buffer(BackendConnection backendConnection) {
    if (!Strings.isNullOrEmpty(this.name) && this.connection.hasStatement(this.name)) {
      throw new IllegalStateException("Must close statement before reusing name.");
    }
    this.connection.registerStatement(this.name, this.statement);
  }

  @Override
  public void flush() throws Exception {
    // The simple query protocol does not need the ParseComplete response.
    if (!isManuallyCreated()) {
      new ParseCompleteResponse(this.outputStream).send();
    }
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
