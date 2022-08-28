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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

public class DeallocateStatement extends IntermediatePortalStatement {
  static final class ParsedDeallocateStatement {
    private final String name;

    private ParsedDeallocateStatement(String name) {
      this.name = name;
    }
  }

  private final ParsedDeallocateStatement deallocateStatement;

  public DeallocateStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
    this.deallocateStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "DEALLOCATE";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    try {
      if (deallocateStatement.name == null) {
        connectionHandler.closeAllStatements();
      } else {
        connectionHandler.closeStatement(deallocateStatement.name);
      }
    } catch (Exception exception) {
      setFutureStatementResult(Futures.immediateFailedFuture(exception));
      return;
    }
    setFutureStatementResult(Futures.immediateFuture(new NoResult()));
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

  static ParsedDeallocateStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("deallocate")) {
      throw PGExceptionFactory.newPGException("not a valid DEALLOCATE statement: " + sql);
    }
    parser.eatKeyword("prepare");
    String statementName = null;
    if (!parser.eatKeyword("all")) {
      TableOrIndexName name = parser.readTableOrIndexName();
      if (name == null || name.schema != null) {
        throw PGExceptionFactory.newPGException("invalid prepared statement name");
      }
      statementName = name.name;
    }
    parser.skipWhitespaces();
    if (parser.getPos() < parser.getSql().length()) {
      throw PGExceptionFactory.newPGException(
          "Syntax error. Unexpected tokens: " + parser.getSql().substring(parser.getPos()));
    }
    return new ParsedDeallocateStatement(statementName);
  }
}
