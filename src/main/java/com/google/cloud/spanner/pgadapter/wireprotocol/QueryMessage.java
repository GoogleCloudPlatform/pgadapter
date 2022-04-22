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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.MatcherStatement;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyInResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.State;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import com.google.cloud.spanner.pgadapter.wireoutput.RowDescriptionResponse;
import java.text.MessageFormat;

/** Executes a simple statement. */
public class QueryMessage extends ControlMessage {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
  protected static final char IDENTIFIER = 'Q';
  public static final String COPY = "COPY";

  private final boolean isCopy;
  private final IntermediateStatement statement;

  public QueryMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    ParsedStatement parsedStatement = PARSER.parse(Statement.of(this.readAll()));
    this.isCopy = StatementParser.isCommand(COPY, parsedStatement.getSqlWithoutComments());
    if (isCopy) {
      this.statement =
          new CopyStatement(connection, connection.getServer().getOptions(), parsedStatement);
    } else if (!connection.getServer().getOptions().requiresMatcher()) {
      this.statement =
          new IntermediateStatement(
              connection.getServer().getOptions(), parsedStatement, this.connection);
    } else {
      this.statement =
          new MatcherStatement(
              connection.getServer().getOptions(), parsedStatement, this.connection);
    }
    this.connection.addActiveStatement(this.statement);
  }

  @Override
  protected void sendPayload() throws Exception {
    this.statement.execute();
    this.handleQuery();
    if (!this.statement.getCommand(0).equalsIgnoreCase(COPY)) {
      this.connection.removeActiveStatement(this.statement);
    }
  }

  @Override
  protected String getMessageName() {
    return "Query";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, SQL: {1}")
        .format(new Object[] {this.length, this.statement.getSql()});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public IntermediateStatement getStatement() {
    return this.statement;
  }

  /**
   * Simple Query handler, which examined the state of the statement and processes accordingly (if
   * error, handle error, otherwise sends the result and if contains result set, send row
   * description)
   *
   * @throws Exception if handling the query fails
   */
  public void handleQuery() throws Exception {
    // Skip unexecuted statements, as no response needs be returned
    for (int index = 0; index < statement.getStatements().size(); index++) {
      if (this.statement.hasException(index)) {
        new ErrorResponse(
                this.outputStream, this.statement.getException(index), State.InternalError)
            .send();
      } else if (this.statement.getCommand(index).equalsIgnoreCase(COPY)) {
        CopyStatement copyStatement = (CopyStatement) this.statement;
        new CopyInResponse(
                this.outputStream,
                copyStatement.getTableColumns().size(),
                copyStatement.getFormatCode())
            .send();
        this.connection.setStatus(ConnectionStatus.COPY_IN);

        // Return early as we do not respond with CommandComplete after a COPY command.
        return;
      } else {
        if (this.statement.containsResultSet(index)) {
          new RowDescriptionResponse(
                  this.outputStream,
                  this.statement,
                  this.statement.getStatementResult(index),
                  this.connection.getServer().getOptions(),
                  QueryMode.SIMPLE)
              .send(false);
        }
        this.sendSpannerResult(index, this.statement, QueryMode.SIMPLE, 0L);
      }
    }
    boolean inTransaction = connection.getSpannerConnection().isInTransaction();
    Status transactionStatus = Status.IDLE;
    if (inTransaction) {
      if (connection.getStatus() == ConnectionStatus.TRANSACTION_ABORTED) {
        transactionStatus = Status.FAILED;
        // Actively rollback the aborted transaction but still block clients
        connection.getSpannerConnection().rollback();
      } else {
        transactionStatus = Status.TRANSACTION;
      }
    }
    new ReadyResponse(this.outputStream, transactionStatus).send();
    this.connection.cleanUp(this.statement);
  }
}
