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

import com.google.api.client.util.Strings;
import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.PGSetting;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SetStatement.Builder;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Simple parser for session management commands (SET/SHOW/RESET variable_name) */
@InternalApi
public class SessionStatementParser {
  abstract static class SessionStatement {
    protected final String extension;
    protected final String name;

    SessionStatement(TableOrIndexName name) {
      if (name == null) {
        this.extension = null;
        this.name = null;
      } else {
        this.extension = toParameterExtension(name);
        this.name = toParameterName(name);
      }
    }

    String getKey() {
      if (extension != null && name != null) {
        return extension + "." + name;
      } else if (extension == null && name != null) {
        return name;
      }
      return "all";
    }

    abstract StatementResult execute(SessionState sessionState, Connection connection);
  }

  static class SetStatement extends SessionStatement {
    static class Builder {
      private boolean local;
      private TableOrIndexName name;
      private String value;

      Builder setLocal() {
        this.local = true;
        return this;
      }

      Builder setName(TableOrIndexName name) {
        this.name = name;
        return this;
      }

      Builder setValue(String value) {
        this.value = value;
        return this;
      }

      SetStatement build() {
        return new SetStatement(local, name, value);
      }
    }

    private static final NoResult SET_RESULT = new NoResult("SET");
    final boolean local;
    final String value;

    SetStatement(boolean local, TableOrIndexName name, String value) {
      super(Preconditions.checkNotNull(name));
      this.local = local;
      this.value = unquote(value);
    }

    @Override
    public StatementResult execute(SessionState sessionState, Connection connection) {
      if (extension == null && "default_transaction_isolation".equalsIgnoreCase(name)) {
        IsolationLevel isolationLevel =
            value == null
                ? sessionState.getInitialIsolationLevel()
                : SessionState.toSpannerIsolationLevel(value);
        if (local) {
          sessionState.setLocal(extension, name, sessionState.toPgIsolationLevel(isolationLevel));
        } else {
          sessionState.setDefaultIsolationLevel(isolationLevel, connection.isInTransaction());
          BackendConnection.syncConnectionDefaultIsolationLevel(sessionState, connection);
        }
      } else if (extension == null && "transaction_isolation".equalsIgnoreCase(name)) {
        if (value == null) {
          throw PGExceptionFactory.newPGException(
              "parameter \"transaction_isolation\" cannot be reset", SQLState.FeatureNotSupported);
        }
        IsolationLevel isolationLevel =
            SessionState.toSpannerIsolationLevel("transaction_isolation", value);
        if (isolationLevel == IsolationLevel.ISOLATION_LEVEL_UNSPECIFIED) {
          throw SessionState.invalidIsolationLevelError("transaction_isolation", value);
        }
        if (connection.isInTransaction()) {
          connection.execute(
              Statement.of(
                  "SET TRANSACTION ISOLATION LEVEL "
                      + BackendConnection.toSqlIsolationClause(isolationLevel)));
          sessionState.setTransactionIsolationLevel(isolationLevel);
        } else {
          sessionState.setLocal(
              null, "transaction_isolation", sessionState.toPgIsolationLevel(isolationLevel));
        }
      } else if (local) {
        sessionState.setLocal(extension, name, value);
      } else {
        sessionState.set(extension, name, value);
      }
      return SET_RESULT;
    }

    public boolean equals(Object o) {
      if (!(o instanceof SetStatement)) {
        return false;
      }
      SetStatement other = (SetStatement) o;
      return Objects.equals(this.extension, other.extension)
          && Objects.equals(this.name, other.name)
          && Objects.equals(this.local, other.local)
          && Objects.equals(this.value, other.value);
    }

    @Override
    public String toString() {
      return "set "
          + (local ? "local " : "")
          + getKey()
          + " to "
          + (value == null ? "default" : value);
    }
  }

  static class ResetStatement extends SessionStatement {
    private static final NoResult RESET_RESULT = new NoResult("RESET");

    static ResetStatement createResetAll() {
      return new ResetStatement(null);
    }

    ResetStatement(TableOrIndexName name) {
      super(name);
    }

    @Override
    public StatementResult execute(SessionState sessionState, Connection connection) {
      if (extension == null && name != null) {
        if ("default_transaction_isolation".equalsIgnoreCase(name)) {
          sessionState.setDefaultIsolationLevel(
              sessionState.getInitialIsolationLevel(), connection.isInTransaction());
          BackendConnection.syncConnectionDefaultIsolationLevel(sessionState, connection);
        } else if ("transaction_isolation".equalsIgnoreCase(name)) {
          throw PGExceptionFactory.newPGException(
              "parameter \"transaction_isolation\" cannot be reset", SQLState.FeatureNotSupported);
        } else {
          PGSetting setting = sessionState.get(null, name);
          sessionState.set(null, name, setting.getResetVal());
        }
      } else if (extension != null && name != null) {
        sessionState.set(extension, name, null);
      } else {
        BackendConnection.resetConnection(sessionState, connection);
      }
      return RESET_RESULT;
    }

    public boolean equals(Object o) {
      if (!(o instanceof ResetStatement)) {
        return false;
      }
      ResetStatement other = (ResetStatement) o;
      return Objects.equals(this.name, other.name);
    }

    @Override
    public String toString() {
      return "reset " + (name == null ? "all" : getKey());
    }
  }

  static class ShowStatement extends SessionStatement {
    final String header;
    final boolean missingOk;

    static ShowStatement createShowAll() {
      return new ShowStatement(null);
    }

    ShowStatement(TableOrIndexName name) {
      this(name, null, false);
    }

    ShowStatement(TableOrIndexName name, String header, boolean missingOk) {
      super(name);
      this.header = header;
      this.missingOk = missingOk;
    }

    @Override
    public StatementResult execute(SessionState sessionState, Connection connection) {
      if (name != null) {
        String value;
        if (missingOk) {
          PGSetting pgSetting = sessionState.tryGet(extension, name);
          value = pgSetting == null ? null : pgSetting.getSetting();
        } else {
          value = sessionState.get(extension, name).getSetting();
        }
        return new QueryResult(
            ClientSideResultSet.forRows(
                Type.struct(StructField.of(getKey(), Type.string())),
                ImmutableList.of(Struct.newBuilder().set(getKey()).to(value).build())));
      }
      return new QueryResult(
          ClientSideResultSet.forRows(
              Type.struct(
                  StructField.of("name", Type.string()),
                  StructField.of("setting", Type.string()),
                  StructField.of("description", Type.string())),
              sessionState.getAll().stream()
                  .map(
                      setting ->
                          Struct.newBuilder()
                              .set("name")
                              .to(setting.getCasePreservingKey())
                              .set("setting")
                              .to(setting.getSetting())
                              .set("description")
                              .to(setting.getShortDesc())
                              .build())
                  .collect(Collectors.toList())));
    }

    public boolean equals(Object o) {
      if (!(o instanceof ShowStatement)) {
        return false;
      }
      ShowStatement other = (ShowStatement) o;
      return Objects.equals(this.name, other.name);
    }

    @Override
    public String toString() {
      return "show " + (name == null ? "all" : getKey());
    }
  }

  private static String toParameterExtension(TableOrIndexName name) {
    return name.schema == null ? null : unquote(name.schema).toLowerCase(Locale.ROOT);
  }

  private static String toParameterName(TableOrIndexName name) {
    return unquote(name.name).toLowerCase(Locale.ROOT);
  }

  private static String unquote(String value) {
    if (value == null) {
      return null;
    }
    if (value.length() < 2) {
      return value;
    }
    if (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
      return value.substring(1, value.length() - 1);
    }
    if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  public static @Nullable SessionStatement parse(ParsedStatement parsedStatement, String sql) {
    if (parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      ClientSideStatementType clientSideType = parsedStatement.getClientSideStatementType();
      if (clientSideType == ClientSideStatementType.SHOW_TRANSACTION_ISOLATION_LEVEL) {
        return new ShowStatement(new TableOrIndexName("transaction_isolation"));
      }
      if (clientSideType == ClientSideStatementType.SHOW_DEFAULT_TRANSACTION_ISOLATION) {
        return new ShowStatement(new TableOrIndexName("default_transaction_isolation"));
      }
      if (clientSideType != ClientSideStatementType.RESET_ALL
          && clientSideType != ClientSideStatementType.SET_DEFAULT_TRANSACTION_ISOLATION
          && clientSideType != ClientSideStatementType.SET_READONLY) {
        // This statement is handled by the Connection API.
        return null;
      }
    }
    SimpleParser parser = new SimpleParser(sql);
    if (parser.eatKeyword("set")) {
      if (parsedStatement.getType() == StatementType.CLIENT_SIDE) {
        try {
          SetStatement statement = parseSetStatement(parser);
          return statement.extension == null
                  && "default_transaction_isolation".equalsIgnoreCase(statement.name)
              ? statement
              : null;
        } catch (Throwable ignore) {
          return null;
        }
      }
      return parseSetStatement(parser);
    }
    if (parser.eatKeyword("reset")) {
      return parseResetStatement(parser);
    }
    if (parser.eatKeyword("show")) {
      return parseShowStatement(parser);
    }

    return null;
  }

  static SetStatement parseSetStatement(SimpleParser parser) {
    SetStatement.Builder builder = new Builder();
    boolean ateSession = false;
    if (parser.eatKeyword("local")) {
      builder.setLocal();
    } else {
      ateSession = parser.eatKeyword("session");
    }
    if (!builder.local
        && ateSession
        && eatKeywordsAtomically(
            parser, "characteristics", "as", "transaction", "isolation", "level")) {
      String isolationValue;
      if (eatKeywordsAtomically(parser, "repeatable", "read")
          || parser.eatKeyword("repeatable_read")) {
        isolationValue = "repeatable read";
      } else if (parser.eatKeyword("serializable")) {
        isolationValue = "serializable";
      } else if (parser.eatKeyword("default")) {
        isolationValue = null;
      } else {
        String expr = parser.parseExpression();
        if (expr == null) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              "Invalid SET statement: " + parser.getSql() + ". Expected value.");
        }
        isolationValue = expr;
      }
      while (parser.eatToken(";")) {
        // Skip optional trailing semicolons.
      }
      if (parser.hasMoreTokens()) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SET statement: "
                + parser.getSql()
                + ". Expected end of statement after "
                + (isolationValue == null ? "default" : isolationValue));
      }
      return builder
          .setName(new TableOrIndexName("default_transaction_isolation"))
          .setValue(isolationValue)
          .build();
    }
    TableOrIndexName name;
    boolean isAliasStatement = false;
    boolean isTimeZoneAlias = false;
    if (parser.eatKeyword("time", "zone")) {
      name = new TableOrIndexName("TIMEZONE");
      isAliasStatement = true;
      isTimeZoneAlias = true;
    } else if (parser.eatKeyword("names")) {
      name = new TableOrIndexName("CLIENT_ENCODING");
      isAliasStatement = true;
    } else {
      name = parser.readTableOrIndexName();
    }
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected configuration parameter name.");
    }
    builder.setName(name);
    if (!isAliasStatement && !(parser.eatKeyword("to") || parser.eatToken("="))) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected TO or =.");
    }
    String value = parser.parseExpression();
    if (value == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected value.");
    }
    if ("default".equalsIgnoreCase(value) || (isTimeZoneAlias && "local".equalsIgnoreCase(value))) {
      builder.setValue(null);
    } else {
      builder.setValue(value);
    }
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

  static ResetStatement parseResetStatement(SimpleParser parser) {
    if (parser.eatKeyword("all")) {
      String remaining = parser.parseExpression();
      if (!Strings.isNullOrEmpty(remaining)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SHOW statement: "
                + parser.getSql()
                + ". Expected end of statement after \"all\"");
      }
      return ResetStatement.createResetAll();
    }
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid RESET statement: "
              + parser.getSql()
              + ". Expected configuration parameter name.");
    }
    String remaining = parser.parseExpression();
    if (!Strings.isNullOrEmpty(remaining)) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid RESET statement: "
              + parser.getSql()
              + ". Expected end of statement after "
              + name);
    }
    return new ResetStatement(name);
  }

  static ShowStatement parseShowStatement(SimpleParser parser) {
    if (parser.eatKeyword("all")) {
      String remaining = parser.parseExpression();
      if (!Strings.isNullOrEmpty(remaining)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SHOW statement: "
                + parser.getSql()
                + ". Expected end of statement after \"all\"");
      }
      return ShowStatement.createShowAll();
    }
    TableOrIndexName name = null;
    if (eatKeywordsAtomically(parser, "time", "zone")) {
      name = new TableOrIndexName("TIMEZONE");
    } else if (eatKeywordsAtomically(parser, "transaction", "isolation", "level")) {
      name = new TableOrIndexName("transaction_isolation");
    } else {
      name = parser.readTableOrIndexName();
    }
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SHOW statement: "
              + parser.getSql()
              + ". Expected configuration parameter name or ALL.");
    }
    String remaining = parser.parseExpression();
    if (!Strings.isNullOrEmpty(remaining)) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SHOW statement: "
              + parser.getSql()
              + ". Expected end of statement after "
              + name);
    }
    return new ShowStatement(name);
  }

  private static boolean eatKeywordsAtomically(SimpleParser parser, String... keywords) {
    int pos = parser.getPos();
    if (parser.eatKeyword(keywords)) {
      return true;
    }
    parser.setPos(pos);
    return false;
  }
}
