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

import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

abstract class AbstractFetchOrMoveStatement extends IntermediatePortalStatement {
  enum Direction {
    FORWARD_ALL,
    BACKWARD_ALL,
    NEXT,
    PRIOR,
    FIRST,
    LAST,
    ABSOLUTE(true),
    RELATIVE(true),
    ALL,
    FORWARD(true),
    BACKWARD(true);

    final boolean supportsCount;

    Direction() {
      this(false);
    }

    Direction(boolean supportsCount) {
      this.supportsCount = supportsCount;
    }
  }

  abstract static class ParsedFetchOrMoveStatement {
    final String name;
    final Direction direction;
    final Long count;

    ParsedFetchOrMoveStatement(String name, Direction direction, Long count) {
      this.name = name;
      this.direction = direction;
      this.count = count;
    }
  }

  private final ParsedFetchOrMoveStatement fetchOrMoveStatement;

  public AbstractFetchOrMoveStatement(
      String name,
      IntermediatePreparedStatement preparedStatement,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes,
      ParsedFetchOrMoveStatement fetchOrMoveStatement) {
    super(name, preparedStatement, parameters, parameterFormatCodes, resultFormatCodes);
    this.fetchOrMoveStatement = fetchOrMoveStatement;
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    if (!this.executed) {
      try {
        new DescribeMessage(
                connectionHandler,
                PreparedType.Portal,
                fetchOrMoveStatement.name,
                ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
            .send();
        new ExecuteMessage(
                connectionHandler,
                fetchOrMoveStatement.name,
                100,
                "FETCH",
                ManuallyCreatedToken.MANUALLY_CREATED_TOKEN)
            .send();
        // Set a null result to indicate that this statement should not return any result.
        setFutureStatementResult(Futures.immediateFuture(null));
      } catch (Exception exception) {
        setFutureStatementResult(Futures.immediateFailedFuture(exception));
      }
    }
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that the FETCH or MOVE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // FETCH and MOVE do not support binding any parameters, so we just return the same statement.
    return this;
  }

  static <T extends ParsedFetchOrMoveStatement> T parse(String sql, String type, Class<T> clazz) {
    Preconditions.checkNotNull(sql);
    Preconditions.checkNotNull(type);

    // {MOVE | FETCH} [ direction ] [ FROM | IN ] cursor_name
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword(type)) {
      throw PGExceptionFactory.newPGException(
          "not a valid " + type.toUpperCase() + " statement: " + sql, SQLState.SyntaxError);
    }
    Direction direction;
    if (parser.eatKeyword("forward")) {
      if (parser.eatKeyword("all")) {
        direction = Direction.FORWARD_ALL;
      } else {
        direction = Direction.FORWARD;
      }
    } else if (parser.eatKeyword("backward")) {
      if (parser.eatKeyword("all")) {
        direction = Direction.BACKWARD_ALL;
      } else {
        direction = Direction.BACKWARD;
      }
    } else {
      direction =
          Arrays.stream(Direction.values())
              .filter(dir -> parser.eatKeyword(dir.name()))
              .findFirst()
              .orElse(null);
    }
    Long count = null;
    if (parser.peekNumericLiteral()) {
      count = parser.readIntegerLiteral();
      if (count == null) {
        throw PGExceptionFactory.newPGException("syntax error: " + sql, SQLState.SyntaxError);
      }
    }
    if (count != null && direction != null && !direction.supportsCount) {
      throw PGExceptionFactory.newPGException(
          "unexpected <count> argument: " + sql, SQLState.SyntaxError);
    } else if (count == null && direction == Direction.ABSOLUTE) {
      throw PGExceptionFactory.newPGException(
          "missing or invalid <count> argument for ABSOLUTE: " + sql, SQLState.SyntaxError);
    } else if (count == null && direction == Direction.RELATIVE) {
      throw PGExceptionFactory.newPGException(
          "missing or invalid <count> argument for RELATIVE: " + sql, SQLState.SyntaxError);
    }
    // Skip 'from' or 'in'.
    boolean ignore = parser.eatKeyword("from") || parser.eatKeyword("in");
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null || name.schema != null) {
      throw PGExceptionFactory.newPGException("invalid cursor name: " + sql, SQLState.SyntaxError);
    }
    if (parser.hasMoreTokens()) {
      throw PGExceptionFactory.newPGException(
          "unexpected tokens after cursor name: " + sql, SQLState.SyntaxError);
    }

    try {
      return clazz
          .getDeclaredConstructor(String.class, Direction.class, Long.class)
          .newInstance(unquoteOrFoldIdentifier(name.name), direction, count);
    } catch (Exception exception) {
      throw PGExceptionFactory.newPGException(
          "internal error: " + exception.getMessage(), SQLState.InternalError);
    }
  }
}
