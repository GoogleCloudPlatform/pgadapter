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
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.ShowStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.QuotedString;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;

/**
 * SELECT CURRENT_SETTING(setting_name text [, missing_ok boolean ] ) is translated to the
 * equivalent SHOW statement.
 */
public class SelectCurrentSettingStatement extends IntermediatePortalStatement {
  private final ShowStatement showStatement;

  public SelectCurrentSettingStatement(
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
    this.showStatement = parse(originalStatement.getSql());
  }

  static ShowStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    // SELECT CURRENT_SETTING('setting_name'[, missing_ok])
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("select", "current_setting")) {
      throw PGExceptionFactory.newPGException(
          "not a valid SELECT CURRENT_SETTING statement: " + sql, SQLState.SyntaxError);
    }
    if (!parser.eatToken("(")) {
      throw PGExceptionFactory.newPGException(
          "missing '(' for current_setting", SQLState.SyntaxError);
    }
    QuotedString setting = parser.readSingleQuotedString();
    if (setting == null) {
      throw PGExceptionFactory.newPGException(
          "missing or invalid setting name", SQLState.SyntaxError);
    }
    boolean missingOk = false;
    if (parser.eatToken(",")) {
      String missingOkString = parser.readKeyword().toLowerCase(Locale.ENGLISH);
      missingOk = BooleanParser.toBoolean(missingOkString);
    }
    if (!parser.eatToken(")")) {
      throw PGExceptionFactory.newPGException(
          "missing ')' for current_setting", SQLState.SyntaxError);
    }
    parser.throwIfHasMoreTokens();

    try {
      TableOrIndexName settingName = TableOrIndexName.parse(setting.getValue());
      return new ShowStatement(settingName, "current_setting", missingOk);
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
    setFutureStatementResult(
        Futures.immediateFuture(showStatement.execute(backendConnection.getSessionState())));
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    ResultSet resultSet =
        ClientSideResultSet.forRows(
            Type.struct(StructField.of("current_setting", Type.string())), ImmutableList.of());
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
