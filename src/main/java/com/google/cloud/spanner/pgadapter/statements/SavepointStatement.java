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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

public class SavepointStatement extends IntermediatePortalStatement {
  static class ParsedSavepointStatement {
    private final String name;

    ParsedSavepointStatement(String name) {
      this.name = name;
    }
  }

  private final ParsedSavepointStatement parsedSavepointStatement;

  public SavepointStatement(
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
    this.parsedSavepointStatement = parse(originalStatement.getSql());
  }

  static ParsedSavepointStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("savepoint")) {
      throw PGExceptionFactory.newPGException("not a valid SAVEPOINT statement: " + sql);
    }
    String name = parser.readIdentifierPart();
    if (name == null) {
      throw PGExceptionFactory.newPGException("Invalid savepoint name");
    }
    parser.throwIfHasMoreTokens();

    return new ParsedSavepointStatement(name);
  }

  @Override
  public String getCommandTag() {
    return "SAVEPOINT";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    setFutureStatementResult(Futures.immediateFuture(new NoResult()));
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this SAVEPOINT statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // SAVEPOINT does not support binding any parameters, so we just return the same statement.
    return this;
  }
}
