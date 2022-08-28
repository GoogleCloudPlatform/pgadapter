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
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.postgresql.core.Oid;

@InternalApi
public class PrepareStatement extends IntermediatePortalStatement {
  private static final ImmutableMap<String, Integer> TYPE_NAME_TO_OID_MAPPING =
      ImmutableMap.<String, Integer>builder()
          .put("bigint", Oid.INT8)
          .put("int8", Oid.INT8)
          .put("boolean", Oid.BOOL)
          .put("bool", Oid.BOOL)
          .put("bytea", Oid.BYTEA)
          .put("character varying", Oid.VARCHAR)
          .put("varchar", Oid.VARCHAR)
          .put("date", Oid.DATE)
          .put("double precision", Oid.FLOAT8)
          .put("float8", Oid.FLOAT8)
          .put("jsonb", Oid.JSONB)
          .put("numeric", Oid.NUMERIC)
          .put("text", Oid.TEXT)
          .put("timestamp with time zone", Oid.TIMESTAMPTZ)
          .put("timestamptz", Oid.TIMESTAMPTZ)
          .build();

  static final class ParsedPreparedStatement {
    private final String name;
    private final int[] dataTypes;
    private final Statement originalPreparedStatement;
    private final ParsedStatement parsedPreparedStatement;

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
    super(connectionHandler, options, name, parsedStatement, originalStatement);
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

  @Override
  public Future<DescribePortalMetadata> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this PREPARE statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement bind(
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
      if (dataTypesNames.isEmpty()) {
        throw PGExceptionFactory.newPGException("invalid data type list");
      }
      dataTypesBuilder.addAll(
          dataTypesNames.stream()
              .map(PrepareStatement::dataTypeNameToOid)
              .collect(Collectors.toList()));
    }
    if (!parser.eatKeyword("as")) {
      throw PGExceptionFactory.newPGException("missing 'AS' keyword in PREPARE statement: " + sql);
    }
    return new ParsedPreparedStatement(
        name.name,
        dataTypesBuilder.build().stream().mapToInt(i -> i).toArray(),
        parser.getSql().substring(parser.getPos()));
  }

  static int dataTypeNameToOid(String type) {
    SimpleParser parser = new SimpleParser(type);
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null || name.schema != null) {
      throw PGExceptionFactory.newPGException("unknown type name: " + type);
    }
    if (parser.eatToken("(")) {
      // Just skip everything inside parentheses.
      parser.parseExpression();
    }
    String typeNameWithoutParams;
    if (parser.getPos() < parser.getSql().length()) {
      typeNameWithoutParams = name.name + parser.getSql().substring(parser.getPos());
    } else {
      typeNameWithoutParams = name.name;
    }
    typeNameWithoutParams = typeNameWithoutParams.replaceAll("\\s+", " ");
    Integer oid = TYPE_NAME_TO_OID_MAPPING.get(typeNameWithoutParams);
    if (oid != null) {
      return oid;
    }
    throw PGExceptionFactory.newPGException("unknown type name: " + type);
  }
}
