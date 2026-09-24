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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribeResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.util.concurrent.Futures;
import java.util.concurrent.Future;

public class InvalidStatement extends IntermediatePortalStatement {
  private static final Statement EMPTY_STATEMENT = Statement.of("");
  private static final ParsedStatement EMPTY_PARSED_STATEMENT =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(EMPTY_STATEMENT);

  public InvalidStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      Exception exception) {
    this(connectionHandler, options, name, EMPTY_PARSED_STATEMENT, EMPTY_STATEMENT, exception);
  }

  public InvalidStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      Statement originalStatement,
      Exception exception) {
    this(connectionHandler, options, "", parsedStatement, originalStatement, exception);
  }

  public InvalidStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement,
      Exception exception) {
    super(
        name,
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            name,
            NO_PARAMETER_TYPES,
            parsedStatement,
            originalStatement),
        NO_PARAMS,
        NO_FORMAT_CODES,
        NO_FORMAT_CODES);
    setException(PGExceptionFactory.toPGException(exception));
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name, byte[][] parameters, short[] parameterFormatCodes, short[] resultFormatCodes) {
    return this;
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    return Futures.immediateFailedFuture(getException());
  }

  @Override
  public DescribeResult describe() {
    throw getException();
  }

  @Override
  public void autoDescribeParameters(
      byte[][] parameterValues, BackendConnection backendConnection) {
    // Do nothing for invalid statements.
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
  }
}
