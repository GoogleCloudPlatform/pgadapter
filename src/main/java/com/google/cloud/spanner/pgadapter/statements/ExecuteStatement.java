// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.statements;

import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.unquoteOrFoldIdentifier;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@InternalApi
public class ExecuteStatement extends IntermediatePortalStatement {
  static final class ParsedExecuteStatement {
    final String name;
    final byte[][] parameters;

    private ParsedExecuteStatement(String name, byte[][] parameters) {
      this.name = name;
      this.parameters = parameters;
    }
  }

  private final ParsedExecuteStatement executeStatement;

  public ExecuteStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
    this.executeStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "EXECUTE";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    try {
      new BindMessage(
              connectionHandler,
              executeStatement.name,
              executeStatement.parameters,
              ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
          .send();
      new DescribeMessage(
              connectionHandler,
              PreparedType.Portal,
              /* name = */ "",
              ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
          .send();
      new ExecuteMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
    } catch (Exception exception) {
      setFutureStatementResult(Futures.immediateFailedFuture(exception));
      return;
    }
    // Set a null result to indicate that this statement should not return any result.
    setFutureStatementResult(Futures.immediateFuture(null));
  }

  @Override
  public Future<DescribePortalMetadata> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this EXECUTE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement bind(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // EXECUTE does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedExecuteStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("execute")) {
      throw PGExceptionFactory.newPGException("not a valid EXECUTE statement: " + sql);
    }
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null || name.schema != null) {
      throw PGExceptionFactory.newPGException("invalid prepared statement name");
    }
    String statementName = unquoteOrFoldIdentifier(name.name);

    List<String> parameters;
    if (parser.eatToken("(")) {
      List<String> parametersList = parser.parseExpressionList();
      if (parametersList == null || parametersList.isEmpty()) {
        throw PGExceptionFactory.newPGException("invalid parameter list");
      }
      if (!parser.eatToken(")")) {
        throw PGExceptionFactory.newPGException("missing closing parentheses in parameters list");
      }
      parameters =
          parametersList.stream().map(ExecuteStatement::unquoteString).collect(Collectors.toList());
    } else {
      parameters = Collections.emptyList();
    }
    parser.skipWhitespaces();
    if (parser.getPos() < parser.getSql().length()) {
      throw PGExceptionFactory.newPGException(
          "Syntax error. Unexpected tokens: " + parser.getSql().substring(parser.getPos()));
    }
    return new ParsedExecuteStatement(
        statementName,
        parameters.stream()
            .map(p -> p == null ? null : p.getBytes(StandardCharsets.UTF_8))
            .toArray(byte[][]::new));
  }

  static String unquoteString(String parameter) {
    if (parameter != null
        && parameter.length() > 1
        && parameter.charAt(0) == '\''
        && parameter.charAt(parameter.length() - 1) == '\'') {
      return parameter.substring(1, parameter.length() - 1);
    }
    if (parameter != null && parameter.trim().equalsIgnoreCase("null")) {
      return null;
    }
    return parameter;
  }
}
