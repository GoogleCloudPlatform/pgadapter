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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@InternalApi
public class PrepareStatement extends IntermediatePortalStatement {
  static final class ParsedPreparedStatement {
    final String name;
    final int[] dataTypes;
    final Statement originalPreparedStatement;
    final ParsedStatement parsedPreparedStatement;

    private ParsedPreparedStatement(String name, int[] dataTypes, String sql) {
      this.name = name;
      this.dataTypes = dataTypes;
      this.originalPreparedStatement = Statement.of(sql);
      this.parsedPreparedStatement =
          AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(originalPreparedStatement);
    }
  }

  private final ParsedPreparedStatement preparedStatement;

  public PrepareStatement(
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
    this.preparedStatement = parse(originalStatement.getSql());
  }

  @Override
  public String getCommandTag() {
    return "PREPARE";
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
        new ParseMessage(
                connectionHandler,
                preparedStatement.name,
                preparedStatement.dataTypes,
                preparedStatement.parsedPreparedStatement,
                preparedStatement.originalPreparedStatement)
            .send();
        new DescribeMessage(
                connectionHandler,
                PreparedType.Statement,
                preparedStatement.name,
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
    // Return null to indicate that this PREPARE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // COPY does not support binding any parameters, so we just return the same statement.
    return this;
  }

  static ParsedPreparedStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("prepare")) {
      throw PGExceptionFactory.newPGException("not a valid PREPARE statement: " + sql);
    }
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null || name.schema != null) {
      throw PGExceptionFactory.newPGException("invalid prepared statement name");
    }
    ImmutableList.Builder<Integer> dataTypesBuilder = ImmutableList.builder();
    if (parser.eatToken("(")) {
      List<String> dataTypesNames = parser.parseExpressionList();
      if (dataTypesNames == null || dataTypesNames.isEmpty()) {
        throw PGExceptionFactory.newPGException("invalid data type list");
      }
      if (!parser.eatToken(")")) {
        throw PGExceptionFactory.newPGException("missing closing parentheses in data type list");
      }
      dataTypesBuilder.addAll(
          dataTypesNames.stream()
              .map(LiteralParser::dataTypeNameToOid)
              .collect(Collectors.toList()));
    }
    if (!parser.eatKeyword("as")) {
      throw PGExceptionFactory.newPGException("missing 'AS' keyword in PREPARE statement: " + sql);
    }
    return new ParsedPreparedStatement(
        name.name,
        dataTypesBuilder.build().stream().mapToInt(i -> i).toArray(),
        parser.getSql().substring(parser.getPos()).trim());
  }
}
