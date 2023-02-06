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
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.statements.TruncateStatement.ParsedTruncateStatement.Builder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class TruncateStatement extends IntermediatePortalStatement {
  static final class ParsedTruncateStatement {
    static final class Builder {
      final ImmutableList.Builder<TableOrIndexName> tables = ImmutableList.builder();
      boolean only;
      boolean star;
      boolean restartIdentity;
      boolean cascade;
    }

    final ImmutableList<TableOrIndexName> tables;
    final boolean only;
    final boolean star;
    final boolean restartIdentity;
    final boolean cascade;

    private ParsedTruncateStatement(Builder builder) {
      this.tables = builder.tables.build();
      this.only = builder.only;
      this.star = builder.star;
      this.restartIdentity = builder.restartIdentity;
      this.cascade = builder.cascade;
    }
  }

  private final ParsedTruncateStatement truncateStatement;

  public TruncateStatement(
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
    this.truncateStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "TRUNCATE TABLE";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.DDL;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    setFutureStatementResult(backendConnection.execute(this));
  }

  public List<Statement> getDeleteStatements() {
    List<Statement> statements = new ArrayList<>(truncateStatement.tables.size());
    for (TableOrIndexName table : truncateStatement.tables) {
      statements.add(Statement.of("delete from " + table.toString()));
    }
    return statements;
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this TRUNCATE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // TRUNCATE does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedTruncateStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    ParsedTruncateStatement.Builder builder = new Builder();
    if (!parser.eatKeyword("truncate")) {
      throw PGExceptionFactory.newPGException("not a valid TRUNCATE statement: " + sql);
    }
    parser.eatKeyword("table");
    builder.only = parser.eatKeyword("only");
    TableOrIndexName table = parser.readTableOrIndexName();
    if (table == null) {
      throw PGExceptionFactory.newPGException("invalid or missing table name");
    }
    builder.tables.add(table);
    builder.star = parser.eatToken("*");
    while (parser.eatToken(",")) {
      TableOrIndexName name = parser.readTableOrIndexName();
      if (name == null) {
        throw PGExceptionFactory.newPGException("invalid or missing table name");
      }
      builder.tables.add(name);
    }
    if (!parser.eatKeyword("continue", "identity")) {
      builder.restartIdentity = parser.eatKeyword("restart", "identity");
    }
    if (!parser.eatKeyword("restrict")) {
      builder.cascade = parser.eatKeyword("cascade");
    }

    parser.throwIfHasMoreTokens();
    return new ParsedTruncateStatement(builder);
  }
}
