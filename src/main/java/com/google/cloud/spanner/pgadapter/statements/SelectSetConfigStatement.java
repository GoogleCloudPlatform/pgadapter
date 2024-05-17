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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SetStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.QuotedString;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;

/**
 * SELECT SET_CONFIG(setting_name text, new_value text, is_local boolean) is translated to the
 * equivalent SET statement.
 */
public class SelectSetConfigStatement extends IntermediatePortalStatement {
  private final SetStatement setStatement;

  public SelectSetConfigStatement(
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
    this.setStatement = parse(originalStatement.getSql());
  }

  static SetStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    // SELECT SET_CONFIG(setting_name text, new_value text, is_local boolean)
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("select", "set_config")) {
      throw PGExceptionFactory.newPGException(
          "not a valid SELECT SET_CONFIG statement: " + sql, SQLState.SyntaxError);
    }
    if (!parser.eatToken("(")) {
      throw PGExceptionFactory.newPGException("missing '(' for set_config", SQLState.SyntaxError);
    }
    QuotedString setting = parser.readSingleQuotedString();
    if (!parser.eatToken(",")) {
      throw PGExceptionFactory.newPGException(
          "missing ',' after setting name", SQLState.SyntaxError);
    }
    QuotedString value = parser.readSingleQuotedString();
    if (!parser.eatToken(",")) {
      throw PGExceptionFactory.newPGException(
          "missing ',' after setting value", SQLState.SyntaxError);
    }
    String localString = parser.readKeyword().toLowerCase(Locale.ENGLISH);
    boolean local = BooleanParser.toBoolean(localString);
    if (!parser.eatToken(")")) {
      throw PGExceptionFactory.newPGException("missing ')' for set_config", SQLState.SyntaxError);
    }
    parser.throwIfHasMoreTokens();

    try {
      TableOrIndexName settingName = TableOrIndexName.parse(setting.getValue());
      return new SetStatement(local, settingName, value.rawValue);
    } catch (IllegalArgumentException illegalArgumentException) {
      throw PGExceptionFactory.newPGException(
          illegalArgumentException.getMessage(), SQLState.SyntaxError);
    }
  }

  @Override
  public String getCommandTag() {
    return "SELECT";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    try {
      setStatement.execute(backendConnection.getSessionState());
    } catch (Throwable throwable) {
      setFutureStatementResult(Futures.immediateFailedFuture(throwable));
      return;
    }
    setFutureStatementResult(
        Futures.immediateFuture(
            new QueryResult(
                ClientSideResultSet.forRows(
                    Type.struct(StructField.of("set_config", Type.string())),
                    ImmutableList.of(
                        Struct.newBuilder().set("set_config").to(setStatement.value).build())))));
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    ResultSet resultSet =
        ClientSideResultSet.forRows(
            Type.struct(StructField.of("set_config", Type.string())), ImmutableList.of());
    return Futures.immediateFuture(new QueryResult(resultSet));
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    return this;
  }
}
