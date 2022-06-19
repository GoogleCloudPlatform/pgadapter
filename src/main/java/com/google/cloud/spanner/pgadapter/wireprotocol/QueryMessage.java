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
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleQueryStatement;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyInResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import java.text.MessageFormat;

/** Executes a simple statement. */
@InternalApi
public class QueryMessage extends ControlMessage {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
  protected static final char IDENTIFIER = 'Q';
  public static final String COPY = "COPY";

  private final Statement originalStatement;
//  private final CopyStatement copyStatement;
  private final SimpleQueryStatement simpleQueryStatement;
//  private final boolean isCopy;

  public QueryMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.originalStatement = Statement.of(this.readAll());
//    ParsedStatement parsedStatement = PARSER.parse(originalStatement);
//    this.isCopy = StatementParser.isCommand(COPY, parsedStatement.getSqlWithoutComments());
//    if (isCopy) {
//      this.copyStatement =
//          new CopyStatement(
//              connection,
//              connection.getServer().getOptions(),
//              parsedStatement,
//              this.originalStatement);
//      this.simpleQueryStatement = null;
//      this.connection.addActiveStatement(this.copyStatement);
//    } else {
      this.simpleQueryStatement =
          new SimpleQueryStatement(
              connection.getServer().getOptions(), this.originalStatement, this.connection);
//      this.copyStatement = null;
//    }
  }

  @Override
  protected void sendPayload() throws Exception {
//    if (this.isCopy) {
//      this.copyStatement.execute();
//      handleCopy();
//    } else {
      this.simpleQueryStatement.execute();
//    }
  }

  @Override
  protected String getMessageName() {
    return "Query";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, SQL: {1}")
        .format(new Object[] {this.length, this.originalStatement.getSql()});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public Statement getStatement() {
    return this.originalStatement;
  }

  public SimpleQueryStatement getSimpleQueryStatement() {
    return this.simpleQueryStatement;
  }

//  private void handleCopy() throws Exception {
//    if (this.copyStatement.hasException()) {
//      handleError(this.copyStatement.getException());
//      new ReadyResponse(
//              this.outputStream,
//              connection
//                  .getExtendedQueryProtocolHandler()
//                  .getBackendConnection()
//                  .getConnectionState()
//                  .getReadyResponseStatus())
//          .send();
//    } else {
//      new CopyInResponse(
//              this.outputStream,
//              copyStatement.getTableColumns().size(),
//              copyStatement.getFormatCode())
//          .send();
//      this.connection.setStatus(ConnectionStatus.COPY_IN);
//    }
//  }
}
