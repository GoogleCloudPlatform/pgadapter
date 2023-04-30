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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.DeclareStatement.ParsedDeclareStatement.Builder;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;

@InternalApi
public class DeclareStatement extends IntermediatePortalStatement {
  enum Sensitivity {
    ASENSITIVE,
    INSENSITIVE,
  }

  enum Scroll {
    SCROLL,
    NO_SCROLL,
  }

  enum Holdability {
    HOLD,
    NO_HOLD,
  }

  static final class ParsedDeclareStatement {
    static final class Builder {
      String name;
      boolean binary;
      Sensitivity sensitivity;
      Scroll scroll;
      Holdability holdability;
      String sql;
    }

    final String name;
    final boolean binary;
    final Sensitivity sensitivity;
    final Scroll scroll;
    final Holdability holdability;
    final Statement originalPreparedStatement;
    final ParsedStatement parsedPreparedStatement;

    private ParsedDeclareStatement(Builder builder) {
      this.name = builder.name;
      this.binary = builder.binary;
      this.sensitivity = builder.sensitivity;
      this.scroll = builder.scroll;
      this.holdability = builder.holdability;
      this.originalPreparedStatement = Statement.of(builder.sql);
      this.parsedPreparedStatement =
          AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(originalPreparedStatement);
    }
  }

  private final ParsedDeclareStatement declareStatement;

  public DeclareStatement(
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
    this.declareStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "DECLARE";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    if (!this.executed) {
      this.executed = true;
      try {
        if (declareStatement.scroll == Scroll.SCROLL) {
          throw PGExceptionFactory.newPGException("scrollable cursors are not supported");
        }
        new ParseMessage(
                connectionHandler,
                declareStatement.name,
                new int[0],
                declareStatement.parsedPreparedStatement,
                declareStatement.originalPreparedStatement)
            .send();
        new BindMessage(
                connectionHandler,
                "",
                declareStatement.name,
                new byte[0][],
                ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
            .send();
        new DescribeMessage(
                connectionHandler,
                PreparedType.Statement,
                declareStatement.name,
                ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
            .send();
      } catch (Exception exception) {
        setFutureStatementResult(Futures.immediateFailedFuture(exception));
        return;
      }
      setFutureStatementResult(Futures.immediateFuture(new NoResult(getCommandTag())));
    }
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this DECLARE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // DECLARE does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedDeclareStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    // DECLARE name [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ]
    //    CURSOR [ { WITH | WITHOUT } HOLD ] FOR query
    // The keywords ASENSITIVE, BINARY, INSENSITIVE, and SCROLL can appear in any order.
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("declare")) {
      throw PGExceptionFactory.newPGException(
          "not a valid DECLARE statement: " + sql, SQLState.SyntaxError);
    }
    Builder builder = new Builder();
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null || name.schema != null) {
      throw PGExceptionFactory.newPGException("invalid cursor name: " + sql, SQLState.SyntaxError);
    }
    builder.name = unquoteOrFoldIdentifier(name.name);
    while (parser.hasMoreTokens() && !parser.peekKeyword("cursor")) {
      if (parser.eatKeyword("binary")) {
        builder.binary = true;
      } else if (parser.eatKeyword("asensitive")) {
        builder.sensitivity = Sensitivity.ASENSITIVE;
      } else if (parser.eatKeyword("insensitive")) {
        builder.sensitivity = Sensitivity.INSENSITIVE;
      } else if (parser.eatKeyword("scroll")) {
        builder.scroll = Scroll.SCROLL;
      } else if (parser.eatKeyword("no", "scroll")) {
        builder.scroll = Scroll.NO_SCROLL;
      } else {
        throw PGExceptionFactory.newPGException("syntax error: " + sql, SQLState.SyntaxError);
      }
    }
    if (!parser.eatKeyword("cursor")) {
      throw PGExceptionFactory.newPGException(
          "missing expected CURSOR keyword: " + sql, SQLState.SyntaxError);
    }
    if (parser.eatKeyword("with", "hold")) {
      builder.holdability = Holdability.HOLD;
    } else if (parser.eatKeyword("without", "hold")) {
      builder.holdability = Holdability.NO_HOLD;
    }
    if (!parser.eatKeyword("for")) {
      throw PGExceptionFactory.newPGException(
          "missing expected FOR keyword: " + sql, SQLState.SyntaxError);
    }
    if (!parser.hasMoreTokens()) {
      throw PGExceptionFactory.newPGException(
          "missing query for cursor: " + sql, SQLState.SyntaxError);
    }
    builder.sql = parser.getSql().substring(parser.getPos()).trim();

    return new ParsedDeclareStatement(builder);
  }
}
