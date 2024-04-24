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
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.session.PGSetting;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SetStatement.Builder;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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

    abstract StatementResult execute(SessionState sessionState);
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
    public StatementResult execute(SessionState sessionState) {
      if (local) {
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
    public StatementResult execute(SessionState sessionState) {
      if (extension == null && name != null) {
        PGSetting setting = sessionState.get(null, name);
        sessionState.set(null, name, setting.getResetVal());
      } else if (extension != null && name != null) {
        sessionState.set(extension, name, null);
      } else {
        sessionState.resetAll();
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
    public StatementResult execute(SessionState sessionState) {
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

  public static @Nullable SessionStatement parse(ParsedStatement parsedStatement) {
    if (parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      // This statement is handled by the Connection API.
      return null;
    }
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eatKeyword("set")) {
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
    if (parser.eatKeyword("local")) {
      builder.setLocal();
    } else {
      // Ignore, this is the default.
      parser.eatKeyword("session");
    }
    TableOrIndexName name;
    boolean isAliasStatement = false;
    if (parser.eatKeyword("time", "zone")) {
      name = new TableOrIndexName("TIMEZONE");
      isAliasStatement = true;
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
    if ("default".equalsIgnoreCase(value)
        || (isAliasStatement && "local".equalsIgnoreCase(value))) {
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
    if (parser.eatKeyword("time", "zone")) {
      name = new TableOrIndexName("TIMEZONE");
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
}
