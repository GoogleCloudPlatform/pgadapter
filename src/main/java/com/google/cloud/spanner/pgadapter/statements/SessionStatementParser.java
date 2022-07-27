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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SetStatement.Builder;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import javax.annotation.Nullable;

public class SessionStatementParser {
  interface SessionStatement {
    void execute(SessionState sessionState);
  }

  static class SetStatement implements SessionStatement {
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

    final boolean local;
    final TableOrIndexName name;
    final String value;

    private SetStatement(boolean local, TableOrIndexName name, String value) {
      this.local = local;
      this.name = name;
      this.value = value;
    }

    @Override
    public void execute(SessionState sessionState) {
      if (local) {
        sessionState.setLocal(name.toString(), value);
      } else {
        sessionState.set(name.toString(), value);
      }
    }
  }

  static class ResetStatement implements SessionStatement {
    final TableOrIndexName name;

    private ResetStatement(TableOrIndexName name) {
      this.name = name;
    }

    @Override
    public void execute(SessionState sessionState) {}
  }

  static class ShowStatement implements SessionStatement {
    final TableOrIndexName name;

    static ShowStatement createShowAll() {
      return new ShowStatement(null);
    }

    private ShowStatement(TableOrIndexName name) {
      this.name = name;
    }

    @Override
    public void execute(SessionState sessionState) {}
  }

  public static @Nullable SessionStatement parse(ParsedStatement parsedStatement) {
    if (parsedStatement.getType() != StatementType.UNKNOWN) {
      return null;
    }
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eat("set")) {
      return parseSetStatement(parser);
    }
    if (parser.eat("reset")) {
      return parseResetStatement(parser);
    }
    if (parser.eat("show")) {
      return parseShowStatement(parser);
    }

    return null;
  }

  static SetStatement parseSetStatement(SimpleParser parser) {
    SetStatement.Builder builder = new Builder();
    if (parser.eat("local")) {
      builder.setLocal();
    } else {
      // Ignore, this is the default.
      parser.eat("session");
    }
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected configuration parameter name.");
    }
    builder.setName(name);

    if (!(parser.eat("to") || parser.eat("="))) {
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
    builder.setValue(value);

    return builder.build();
  }

  static ResetStatement parseResetStatement(SimpleParser parser) {
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid RESET statement: "
              + parser.getSql()
              + ". Expected configuration parameter name.");
    }
    return new ResetStatement(name);
  }

  static ShowStatement parseShowStatement(SimpleParser parser) {
    if (parser.eat("all")) {
      return ShowStatement.createShowAll();
    }
    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SHOW statement: "
              + parser.getSql()
              + ". Expected configuration parameter name or ALL.");
    }
    return new ShowStatement(name);
  }
}
