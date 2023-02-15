// Copyright 2023 Google LLC
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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

public class RollbackToStatement extends IntermediatePortalStatement {
  static class ParsedRollbackToStatement {
    final String name;

    ParsedRollbackToStatement(String name) {
      this.name = name;
    }
  }

  private final ParsedRollbackToStatement parsedRollbackToStatement;

  public RollbackToStatement(
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
    this.parsedRollbackToStatement = parse(originalStatement.getSql());
  }

  static ParsedRollbackToStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    // https://www.postgresql.org/docs/current/sql-rollback-to.html
    // ROLLBACK [ WORK | TRANSACTION ] TO [ SAVEPOINT ] savepoint_name
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("rollback")) {
      throw PGExceptionFactory.newPGException(
          "not a valid ROLLBACK [WORK | TRANSACTION] TO statement: " + sql);
    }
    if (parser.peekKeyword("work")) {
      parser.eatKeyword("work");
    } else if (parser.peekKeyword("transaction")) {
      parser.eatKeyword("transaction");
    }
    if (!parser.eatKeyword("to")) {
      throw PGExceptionFactory.newPGException(
          "missing 'TO' keyword in ROLLBACK [WORK | TRANSACTION] TO statement: " + sql);
    }
    parser.eatKeyword("savepoint");
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null || name.schema != null) {
      throw PGExceptionFactory.newPGException("invalid savepoint name", SQLState.SyntaxError);
    }
    String savepointName = unquoteOrFoldIdentifier(name.name);
    parser.throwIfHasMoreTokens();

    return new ParsedRollbackToStatement(savepointName);
  }

  public String getSavepointName() {
    return parsedRollbackToStatement.name;
  }

  @Override
  public String getCommandTag() {
    // The command tag for 'ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] is just 'ROLLBACK'.
    return "ROLLBACK";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    setFutureStatementResult(
        Futures.immediateFailedFuture(
            PGExceptionFactory.newPGException(
                "Statement 'ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] savepoint_name' is not supported",
                SQLState.FeatureNotSupported)));
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this ROLLBACK statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // ROLLBACK does not support binding any parameters, so we just return the same statement.
    return this;
  }
}
