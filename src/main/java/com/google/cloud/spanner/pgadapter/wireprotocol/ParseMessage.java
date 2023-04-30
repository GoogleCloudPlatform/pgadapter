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

import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.isCommand;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.CLOSE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.COPY;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.DEALLOCATE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.DECLARE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.EXECUTE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.FETCH;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.MOVE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.PREPARE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.RELEASE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.ROLLBACK;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.SAVEPOINT;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.TRUNCATE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.VACUUM;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.CloseStatement;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.DeallocateStatement;
import com.google.cloud.spanner.pgadapter.statements.DeclareStatement;
import com.google.cloud.spanner.pgadapter.statements.ExecuteStatement;
import com.google.cloud.spanner.pgadapter.statements.FetchStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.InvalidStatement;
import com.google.cloud.spanner.pgadapter.statements.MoveStatement;
import com.google.cloud.spanner.pgadapter.statements.PrepareStatement;
import com.google.cloud.spanner.pgadapter.statements.ReleaseStatement;
import com.google.cloud.spanner.pgadapter.statements.RollbackToStatement;
import com.google.cloud.spanner.pgadapter.statements.SavepointStatement;
import com.google.cloud.spanner.pgadapter.statements.TruncateStatement;
import com.google.cloud.spanner.pgadapter.statements.VacuumStatement;
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
    Statement originalStatement = Statement.of(this.readString());
    ParsedStatement parsedStatement = PARSER.parse(originalStatement);
    short numberOfParameters = this.inputStream.readShort();
    this.parameterDataTypes = new int[numberOfParameters];
    for (int i = 0; i < numberOfParameters; i++) {
      parameterDataTypes[i] = this.inputStream.readInt();
    }
    this.statement =
        createStatement(connection, name, parsedStatement, originalStatement, parameterDataTypes);
    connection.maybeDetermineWellKnownClient(this);
  }

  /**
   * Constructor for manually created Parse messages that originate from the simple query protocol.
   */
  public ParseMessage(
      ConnectionHandler connection, ParsedStatement parsedStatement, Statement originalStatement) {
    this(connection, "", new int[0], parsedStatement, originalStatement);
  }

  /** Constructor for manually created Parse messages that originate from a PREPARE statement. */
  public ParseMessage(
      ConnectionHandler connection,
      String name,
      int[] parameterDataTypes,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(
        connection,
        5 + parsedStatement.getSqlWithoutComments().length(),
        ManuallyCreatedToken.MANUALLY_CREATED_TOKEN);
    this.name = name;
    this.parameterDataTypes = parameterDataTypes;
    this.statement =
        createStatement(connection, name, parsedStatement, originalStatement, parameterDataTypes);
  }

  static IntermediatePreparedStatement createStatement(
      ConnectionHandler connectionHandler,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement,
      int[] parameterDataTypes) {
    try {
      if (isCommand(COPY, originalStatement.getSql())) {
        return CopyStatement.create(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(PREPARE, originalStatement.getSql())) {
        return new PrepareStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(EXECUTE, originalStatement.getSql())) {
        return new ExecuteStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(DEALLOCATE, originalStatement.getSql())) {
        return new DeallocateStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(DECLARE, originalStatement.getSql())) {
        return new DeclareStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
//      } else if (isCommand(FETCH, originalStatement.getSql())) {
//        return new FetchStatement(
//            connectionHandler,
//            connectionHandler.getServer().getOptions(),
//            name,
//            parsedStatement,
//            originalStatement);
//      } else if (isCommand(MOVE, originalStatement.getSql())) {
//        return new MoveStatement(
//            connectionHandler,
//            connectionHandler.getServer().getOptions(),
//            name,
//            parsedStatement,
//            originalStatement);
      } else if (isCommand(CLOSE, originalStatement.getSql())) {
        return new CloseStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(VACUUM, originalStatement.getSql())) {
        return new VacuumStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(TRUNCATE, originalStatement.getSql())) {
        return new TruncateStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(SAVEPOINT, originalStatement.getSql())) {
        return new SavepointStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(RELEASE, originalStatement.getSql())) {
        return new ReleaseStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else if (isCommand(ROLLBACK, originalStatement.getSql())
          && parsedStatement.getType() == StatementType.UNKNOWN) {
        // ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] savepoint_name is not recognized by the
        // Connection API as any known statement.
        return new RollbackToStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parsedStatement,
            originalStatement);
      } else {
        return new IntermediatePreparedStatement(
            connectionHandler,
            connectionHandler.getServer().getOptions(),
            name,
            parameterDataTypes,
            parsedStatement,
            originalStatement);
      }
    } catch (Exception exception) {
      return new InvalidStatement(
          connectionHandler,
          connectionHandler.getServer().getOptions(),
          name,
          parsedStatement,
          originalStatement,
          exception);
    }
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
    if (statement.hasException()) {
      handleError(statement.getException());
    } else if (isExtendedProtocol()) {
      // The simple query protocol does not need the ParseComplete response.
      new ParseCompleteResponse(this.outputStream).send(false);
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

  @Override
  public String getSql() {
    return this.statement.getSql();
  }

  public IntermediatePreparedStatement getStatement() {
    return this.statement;
  }

  public String getName() {
    return this.name;
  }
}
