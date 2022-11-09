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
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.DescribeResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * An intermediate representation of a portal statement (that is, a prepared statement which
 * contains all relevant information for execution.
 */
@InternalApi
public class IntermediatePortalStatement extends IntermediatePreparedStatement {
  protected List<Short> parameterFormatCodes;
  protected List<Short> resultFormatCodes;

  public IntermediatePortalStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
    this.statement = originalStatement;
    this.parameterFormatCodes = new ArrayList<>();
    this.resultFormatCodes = new ArrayList<>();
  }

  void setBoundStatement(Statement statement) {
    this.statement = statement;
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

  public void setParameterFormatCodes(List<Short> parameterFormatCodes) {
    this.parameterFormatCodes = parameterFormatCodes;
  }

  public void setResultFormatCodes(List<Short> resultFormatCodes) {
    this.resultFormatCodes = resultFormatCodes;
  }

  @Override
  public Future<DescribeResult> describeAsync(BackendConnection backendConnection) {
    // Pre-emptively execute the statement, even though it is only asked to be described. This is
    // a lot more efficient than taking two round trips to the server, and getting a
    // DescribePortal message without a following Execute message is extremely rare, as that would
    // only happen if the client is ill-behaved, or if the client crashes between the
    // DescribePortal and Execute.
    Future<StatementResult> statementResultFuture =
        backendConnection.execute(this.parsedStatement, this.statement);
    setFutureStatementResult(statementResultFuture);
    return Futures.lazyTransform(
        statementResultFuture,
        statementResult -> new DescribeResult(statementResult.getResultSet()));
  }
}
