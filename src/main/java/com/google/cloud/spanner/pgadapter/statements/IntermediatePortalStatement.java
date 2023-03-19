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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import java.util.List;
import java.util.concurrent.Future;

/**
 * An intermediate representation of a portal statement (that is, a prepared statement which
 * contains all relevant information for execution.
 */
@InternalApi
public class IntermediatePortalStatement extends IntermediatePreparedStatement {
  static final byte[][] NO_PARAMS = new byte[0][];

  private final IntermediatePreparedStatement preparedStatement;
  private final byte[][] parameters;
  protected final List<Short> parameterFormatCodes;
  protected final List<Short> resultFormatCodes;

  public IntermediatePortalStatement(
      String name,
      IntermediatePreparedStatement preparedStatement,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    super(
        preparedStatement.connectionHandler,
        preparedStatement.options,
        name,
        preparedStatement.givenParameterDataTypes,
        preparedStatement.parsedStatement,
        preparedStatement.originalStatement);
    this.preparedStatement = preparedStatement;
    this.parameters = parameters;
    this.parameterFormatCodes = parameterFormatCodes;
    this.resultFormatCodes = resultFormatCodes;
  }

  public IntermediatePreparedStatement getPreparedStatement() {
    return this.preparedStatement;
  }

  public short getParameterFormatCode(int index) {
    if (this.parameterFormatCodes.size() == 0) {
      return 0;
    } else if (index >= this.parameterFormatCodes.size()) {
      return this.parameterFormatCodes.get(0);
    } else {
      return this.parameterFormatCodes.get(index);
    }
  }

  @Override
  public short getResultFormatCode(int index) {
    if (this.resultFormatCodes == null || this.resultFormatCodes.isEmpty()) {
      return super.getResultFormatCode(index);
    } else if (this.resultFormatCodes.size() == 1) {
      return this.resultFormatCodes.get(0);
    } else {
      return this.resultFormatCodes.get(index);
    }
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    // If the portal has already been described, the statement has already been executed, and we
    // don't need to do that once more.
    if (futureStatementResult == null && getStatementResult() == null) {
      this.executed = true;
      setFutureStatementResult(backendConnection.execute(parsedStatement, statement, this::bind));
    }
  }

  public void setStatementResult(StatementResult statementResult) {
    super.setStatementResult(statementResult);
    if (statementResult != null) {
      if (statementResult.getResultType() == ResultType.RESULT_SET) {
        this.hasMoreData = statementResult.getResultSet().next();
      } else if (statementResult instanceof NoResult
          && ((NoResult) statementResult).hasCommandTag()) {
        this.commandTag = ((NoResult) statementResult).getCommandTag();
      }
    }
  }

  /** Binds this portal to a set of parameter values. */
  public Statement bind(Statement statement) {
    // Make sure the results from any Describe message are propagated to the prepared statement
    // before using it to bind the parameter values.
    preparedStatement.describe();
    Statement.Builder builder = statement.toBuilder();
    for (int index = 0; index < parameters.length; index++) {
      short formatCode = getParameterFormatCode(index);
      int type = preparedStatement.getParameterDataType(index);
      Parser<?> parser =
          Parser.create(
              connectionHandler
                  .getExtendedQueryProtocolHandler()
                  .getBackendConnection()
                  .getSessionState(),
              parameters[index],
              type,
              FormatCode.of(formatCode));
      parser.bind(builder, "p" + (index + 1));
    }
    return builder.build();
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Pre-emptively execute the statement, even though it is only asked to be described. This is
    // a lot more efficient than taking two round trips to the server, and getting a
    // DescribePortal message without a following Execute message is extremely rare, as that would
    // only happen if the client is ill-behaved, or if the client crashes between the
    // DescribePortal and Execute.
    Future<StatementResult> statementResultFuture =
        backendConnection.execute(this.parsedStatement, this.statement, this::bind);
    setFutureStatementResult(statementResultFuture);
    this.executed = true;
    return statementResultFuture;
  }
}
