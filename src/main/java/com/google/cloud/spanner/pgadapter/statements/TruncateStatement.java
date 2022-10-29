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
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class TruncateStatement extends IntermediatePortalStatement {
  static final class ParsedTruncateStatement {
    final List<TableOrIndexName> tables;
    final boolean only;
    final boolean star;
    final boolean restartIdentity;
    final boolean continueIdentity;
    final boolean cascade;
    final boolean restrict;

    private ParsedTruncateStatement(
        List<TableOrIndexName> tables,
        boolean only,
        boolean star,
        boolean restartIdentity,
        boolean continueIdentity,
        boolean cascade,
        boolean restrict) {
      this.tables = tables;
      this.only = only;
      this.star = star;
      this.restartIdentity = restartIdentity;
      this.continueIdentity = continueIdentity;
      this.cascade = cascade;
      this.restrict = restrict;
    }
  }

  private final ParsedTruncateStatement truncateStatement;

  public TruncateStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
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
  public Future<DescribePortalMetadata> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this TRUNCATE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement bind(
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
    if (!parser.eatKeyword("truncate")) {
      throw PGExceptionFactory.newPGException("not a valid TRUNCATE statement: " + sql);
    }
    parser.eatKeyword("table");
    boolean only = parser.eatKeyword("only");
    List<TableOrIndexName> tables = new ArrayList<>();
    TableOrIndexName table = parser.readTableOrIndexName();
    if (table == null) {
      throw PGExceptionFactory.newPGException("invalid or missing table name");
    }
    tables.add(table);
    boolean star = parser.eatToken("*");
    while (parser.eatToken(",")) {
      TableOrIndexName name = parser.readTableOrIndexName();
      if (name == null) {
        throw PGExceptionFactory.newPGException("invalid or missing table name");
      }
      tables.add(name);
    }
    boolean restartIdentity = parser.eatKeyword("restart", "identity");
    boolean continueIdentity = parser.eatKeyword("continue", "identity");
    boolean cascade = parser.eatKeyword("cascade");
    boolean restrict = parser.eatKeyword("restrict");
    parser.skipWhitespaces();
    if (parser.getPos() < parser.getSql().length()) {
      throw PGExceptionFactory.newPGException(
          "Syntax error. Unexpected tokens: " + parser.getSql().substring(parser.getPos()));
    }
    return new ParsedTruncateStatement(
        tables, only, star, restartIdentity, continueIdentity, cascade, restrict);
  }
}
