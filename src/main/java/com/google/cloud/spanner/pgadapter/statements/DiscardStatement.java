// Copyright 2024 Google LLC
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
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.ConnectionState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.DiscardStatement.ParsedDiscardStatement.DiscardType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

public class DiscardStatement extends IntermediatePortalStatement {
  static final class ParsedDiscardStatement {
    enum DiscardType {
      PLANS,
      SEQUENCES,
      TEMPORARY,
      ALL,
    }

    final DiscardType type;

    private ParsedDiscardStatement(DiscardType type) {
      this.type = type;
    }
  }

  private final ParsedDiscardStatement discardStatement;

  public DiscardStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
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
        ImmutableList.of(),
        ImmutableList.of());
    this.discardStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "DISCARD";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    try {
      switch (discardStatement.type) {
        case ALL:
          if (backendConnection.getConnectionState() == ConnectionState.TRANSACTION) {
            setFutureStatementResult(
                Futures.immediateFailedFuture(
                    PGExceptionFactory.newPGException(
                        "DISCARD ALL cannot run inside a transaction block",
                        SQLState.ActiveSqlTransaction)));
            return;
          }
          backendConnection.getSessionState().resetAll();
          connectionHandler.closeAllStatements();
          break;
        case PLANS:
        case SEQUENCES:
        case TEMPORARY:
        default:
          // The default is no-op as PGAdapter does not support clearing plans, sequences or
          // temporary tables.
      }
    } catch (Exception exception) {
      setFutureStatementResult(Futures.immediateFailedFuture(exception));
      return;
    }
    setFutureStatementResult(Futures.immediateFuture(new NoResult()));
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this DISCARD statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // DISCARD does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedDiscardStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("discard")) {
      throw PGExceptionFactory.newPGException("not a valid DISCARD statement: " + sql);
    }
    DiscardType type;
    if (parser.eatKeyword("all")) {
      type = DiscardType.ALL;
    } else if (parser.eatKeyword("plans")) {
      type = DiscardType.PLANS;
    } else if (parser.eatKeyword("sequences")) {
      type = DiscardType.SEQUENCES;
    } else if (parser.eatKeyword("temp") || parser.eatKeyword("temporary")) {
      type = DiscardType.TEMPORARY;
    } else {
      throw PGExceptionFactory.newPGException("Invalid DISCARD statement: " + parser.getSql());
    }
    parser.throwIfHasMoreTokens();
    return new ParsedDiscardStatement(type);
  }
}
