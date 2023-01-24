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

package com.google.cloud.spanner.myadapter.statements;

import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Strings;
import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.myadapter.parsers.BooleanParser;
import com.google.cloud.spanner.myadapter.parsers.Parser;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.session.SessionState.SessionVariableType;
import com.google.cloud.spanner.myadapter.session.SystemVariable;
import com.google.cloud.spanner.myadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.myadapter.utils.QueryResult;
import com.google.cloud.spanner.myadapter.utils.UpdateCount;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import javax.annotation.Nullable;

/** Simple parser for session management commands (SET/SHOW/RESET variable_name) */
@InternalApi
public class SessionStatementParser {

  public static final String SET_KEYWORD = "set";
  public static final String SELECT_KEYWORD = "select";
  public static final String NAMES_KEYWORD = "names";
  public static final String EQUALS_SIGN = "=";
  public static final String GLOBAL_KEYWORD = "global";
  public static final String AS_KEYWORD = "as";
  public static final String FROM_KEYWORD = "FROM";
  public static final String SYSTEM_VARIABLE_PREFIX = "@@";
  public static final String UD_VARIABLE_PREFIX = "@";

  public abstract static class SessionStatement {
    public abstract StatementResult execute(
        SessionState sessionState, BackendConnection backendConnection);
  }

  static class VariableColumn {
    static class Builder {
      Builder columnName(String columnName) {
        this.columnName = columnName;
        return this;
      }

      Builder variableName(String variableName) {
        this.variableName = variableName;
        return this;
      }

      Builder setSystemVariable() {
        this.systemVariable = true;
        return this;
      }

      VariableColumn build() {
        return new VariableColumn(this);
      }

      String columnName;
      String variableName;
      SessionVariableType type = SessionVariableType.SYSTEM;
      boolean systemVariable = false;
    }

    String columnName;
    String variableName;
    SessionVariableType type;
    boolean systemVariable;

    VariableColumn(Builder builder) {
      this.columnName = builder.columnName;
      ;
      this.variableName = builder.variableName;
      this.type = builder.type;
      this.systemVariable = builder.systemVariable;
    }
  }

  public static class SetStatement extends SessionStatement {
    static class Builder {
      boolean systemVariable = false;
      SessionVariableType type = SessionVariableType.SYSTEM;
      String name = null;
      String value = null;

      Builder systemVariable() {
        this.systemVariable = true;
        return this;
      }

      Builder name(String name) {
        this.name = name;
        return this;
      }

      Builder value(String value) {
        this.value = value;
        return this;
      }

      SetStatement build() {
        return new SetStatement(this);
      }
    }

    boolean systemVariable;
    SessionVariableType type;
    String variableName;
    String variableValue;

    SetStatement(Builder builder) {
      this.systemVariable = builder.systemVariable;
      this.type = builder.type;
      this.variableName = builder.name;
      this.variableValue = builder.value;
    }

    @Override
    public StatementResult execute(SessionState sessionState, BackendConnection backendConnection) {
      setStatementPreHook(backendConnection);
      sessionState.set(variableName, variableValue, type);
      setStatementPostHook();
      return new UpdateCount(0L);
    }

    private void setStatementPreHook(BackendConnection backendConnection) {
      if (SessionState.AUTOCOMMIT_KEYWORD.equals(variableName)) {
        if (BooleanParser.TRUE_VALUES.contains(variableValue)) {
          backendConnection.processSetAutocommit();
        } else if (BooleanParser.FALSE_VALUES.contains(variableValue)) {
          backendConnection.processUnsetAutocommit();
        }
      }
    }

    private void setStatementPostHook() {}
  }

  static class SelectStatement extends SessionStatement {
    static class Builder {
      Builder addVariableColumn(VariableColumn variableColumn) {
        variableColumnList.add(variableColumn);
        return this;
      }

      ArrayList<VariableColumn> variableColumnList = new ArrayList<VariableColumn>();
    }

    SelectStatement(Builder builder) {
      this.variableColumns = builder.variableColumnList;
    }

    @Override
    public StatementResult execute(SessionState sessionState, BackendConnection backendConnection) {
      ArrayList<StructField> structFields = new ArrayList<Type.StructField>();
      Struct.Builder rowBuilder = Struct.newBuilder();
      for (VariableColumn column : variableColumns) {
        SystemVariable variable = sessionState.get(column.variableName, column.type);
        structFields.add(StructField.of(column.columnName, variable.getType()));
        rowBuilder
            .set(column.columnName)
            .to(Parser.parseToSpannerValue(variable.getValue(), variable.getType().getCode()));
      }

      Type rowType = Type.struct(structFields);
      ImmutableList<Struct> rows = ImmutableList.of(rowBuilder.build());
      Preconditions.checkNotNull(rowType);
      Preconditions.checkNotNull(rows);
      Preconditions.checkArgument(rowType.getCode() == Code.STRUCT);
      ResultSet resultSet = ResultSets.forRows(rowType, rows);
      return new QueryResult(resultSet);
    }

    ArrayList<VariableColumn> variableColumns;
  }

  public static @Nullable SessionStatement parse(ParsedStatement parsedStatement) {
    if (parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      // TODO : See if we need special handling for client side statements.
      // Disabling this right now as "SET AUTOCOMMIT 1" is getting treated as
      // client side statement, but it's not a valid GoogleSQL statement.
    }
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eatKeyword(SET_KEYWORD)) {
      return parseSetStatement(parser);
    }
    if (parser.eatKeyword(SELECT_KEYWORD)
        && !parser.peek(true, true, FROM_KEYWORD)
        && parser.peek(true, false, SYSTEM_VARIABLE_PREFIX)) {
      return parseSelectStatement(parser);
    }

    return null;
  }

  static SetStatement parseSetStatement(SimpleParser parser) {
    // TODO: Support a comma separated list of session variable assignments. Righ now only a
    // single variable is allowed.
    SetStatement.Builder builder = new SetStatement.Builder();
    if (parser.eatKeyword(NAMES_KEYWORD)) {
      builder.systemVariable();
      builder.name(NAMES_KEYWORD);
    } else {
      if (parser.eatToken(SYSTEM_VARIABLE_PREFIX) || !parser.eatToken(UD_VARIABLE_PREFIX)) {
        // This is a system variable
        builder.systemVariable();
      }

      TableOrIndexName name = parser.readTableOrIndexName();
      if (name == null) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SET statement: "
                + parser.getSql()
                + ". Expected configuration parameter name.");
      }

      if (GLOBAL_KEYWORD.equals(name.getUnquotedSchema())) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Global session variables are not supported");
      }

      builder.name(name.getUnquotedName());
      if (!parser.eatToken(EQUALS_SIGN)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SET statement: " + parser.getSql() + ". Expected =.");
      }
    }

    String value = parser.unquoteOrFoldIdentifier(parser.parseExpression());
    if (value == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected value.");
    }
    builder.value(value);
    String remaining = parser.parseExpression();
    if (!Strings.isNullOrEmpty(remaining)) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: "
              + parser.getSql()
              + ". Expected end of statement after "
              + value);
    }

    return builder.build();
  }

  static SelectStatement parseSelectStatement(SimpleParser parser) {
    SelectStatement.Builder builder = new SelectStatement.Builder();
    String[] columnExpressions = parser.getSql().substring(parser.getPos()).split(",");
    for (String expression : columnExpressions) {
      SimpleParser expressionParser = new SimpleParser(expression);
      VariableColumn.Builder columnBuilder = new VariableColumn.Builder();
      String columnName = expression.trim();
      if (expressionParser.eatToken(SYSTEM_VARIABLE_PREFIX)
          || !expressionParser.eatToken(UD_VARIABLE_PREFIX)) {
        columnBuilder.setSystemVariable();
      }
      TableOrIndexName name = expressionParser.readTableOrIndexName();
      if (name == null) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SELECT statement expression: " + expressionParser.getSql());
      }

      if (GLOBAL_KEYWORD.equals(name.getUnquotedSchema())) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Global session variables are not supported");
      }

      if (expressionParser.eatKeyword(AS_KEYWORD)) {
        columnName = expressionParser.readKeyword();
      }
      columnBuilder.columnName(columnName);
      columnBuilder.variableName(name.getUnquotedName());
      builder.addVariableColumn(columnBuilder.build());
    }
    return new SelectStatement(builder);
  }
}
