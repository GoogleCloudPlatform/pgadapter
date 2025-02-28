// Copyright 2025 Google LLC
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

import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.QuotedString;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;

class EscapeClauseParser {
  private static final Cache<String, Statement> REMOVED_ESCAPE_CLAUSES_CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .concurrencyLevel(Runtime.getRuntime().availableProcessors())
          .build();

  /** Removes LIKE ... ESCAPE '\' clauses from the statement. */
  static Statement removeDefaultEscapeClauses(Statement statement, String lowerCaseSql) {
    // If there is no 'escape' clause, then we know that we don't have to analyze any further.
    if (!lowerCaseSql.contains("escape")) {
      return statement;
    }
    try {
      return REMOVED_ESCAPE_CLAUSES_CACHE.get(
          statement.getSql(), () -> removeDefaultEscapeClauses(statement));
    } catch (ExecutionException executionException) {
      throw SpannerExceptionFactory.asSpannerException(executionException.getCause());
    }
  }

  static Statement removeDefaultEscapeClauses(Statement statement) {
    SimpleParser parser = new SimpleParser(statement.getSql());
    while (parser.getPos() < parser.getSql().length()) {
      parser.parseExpressionUntilKeyword(ImmutableList.of("escape"), false, false, false);
      if (parser.getPos() == parser.getSql().length()) {
        break;
      }
      int pos = parser.getPos();
      if (parser.eatKeyword("escape")) {
        QuotedString escape = parser.readSingleQuotedString();
        if ("\\".equals(escape.getValue())) {
          int endPos = parser.getPos();
          statement =
              Statement.of(
                  statement.getSql().substring(0, pos) + statement.getSql().substring(endPos));
          parser = new SimpleParser(statement.getSql());
          parser.setPos(pos);
        }
      }
    }
    return statement;
  }
}
