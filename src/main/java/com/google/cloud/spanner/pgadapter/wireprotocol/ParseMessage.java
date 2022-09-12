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

import static com.google.cloud.spanner.pgadapter.parsers.copy.Copy.parse;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.isCommand;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.COPY;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.DEALLOCATE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.EXECUTE;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.PREPARE;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions.FromTo;
import com.google.cloud.spanner.pgadapter.parsers.copy.TokenMgrError;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.CopyToStatement;
import com.google.cloud.spanner.pgadapter.statements.DeallocateStatement;
import com.google.cloud.spanner.pgadapter.statements.ExecuteStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.InvalidStatement;
import com.google.cloud.spanner.pgadapter.statements.PrepareStatement;
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
        CopyOptions copyOptions = parseCopyStatement(parsedStatement);
        if (copyOptions.getFromTo() == FromTo.FROM) {
          return new CopyStatement(
              connectionHandler,
              connectionHandler.getServer().getOptions(),
              name,
              parsedStatement,
              originalStatement);
        } else if (copyOptions.getFromTo() == FromTo.TO) {
          return new CopyToStatement(
              connectionHandler, connectionHandler.getServer().getOptions(), name, copyOptions);
        } else {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT, "Unsupported COPY direction: " + copyOptions.getFromTo());
        }
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
      } else {
        IntermediatePreparedStatement statement =
            new IntermediatePreparedStatement(
                connectionHandler,
                connectionHandler.getServer().getOptions(),
                name,
                parsedStatement,
                originalStatement);
        statement.setParameterDataTypes(parameterDataTypes);
        return statement;
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

  static CopyOptions parseCopyStatement(ParsedStatement parsedStatement) {
    CopyTreeParser.CopyOptions copyOptions = new CopyOptions();
    try {
      parse(parsedStatement.getSqlWithoutComments(), copyOptions);
      return copyOptions;
    } catch (Exception | TokenMgrError e) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid COPY statement syntax: " + e);
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
