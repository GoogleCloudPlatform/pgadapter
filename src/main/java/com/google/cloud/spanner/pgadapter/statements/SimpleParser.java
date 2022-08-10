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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/** A very simple parser that can interpret SQL statements to find specific parts in the string. */
class SimpleParser {
  /** Name of table or index. */
  static class TableOrIndexName {
    /** Schema is an optional schema name prefix. */
    final String schema;
    /** Name is the actual object name. */
    final String name;

    TableOrIndexName(String name) {
      this.schema = null;
      this.name = name;
    }

    TableOrIndexName(String schema, String name) {
      this.schema = schema;
      this.name = name;
    }

    @Override
    public String toString() {
      if (schema == null) {
        return name;
      }
      return schema + "." + name;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TableOrIndexName)) {
        return false;
      }
      TableOrIndexName other = (TableOrIndexName) o;
      return Objects.equals(this.schema, other.schema) && Objects.equals(this.name, other.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.schema, this.name);
    }
  }

  private final String sql;
  private int pos;

  SimpleParser(String sql) {
    this.sql = sql;
  }

  String getSql() {
    return sql;
  }

  int getPos() {
    return pos;
  }

  void setPos(int pos) {
    this.pos = pos;
  }

  List<String> parseExpressionList() {
    return parseExpressionListUntilKeyword(null);
  }

  List<String> parseExpressionListUntilKeyword(@Nullable String keyword) {
    skipWhitespaces();
    List<String> result = new ArrayList<>();
    int start = pos;
    while (pos < sql.length()) {
      String expression =
          parseExpressionUntilKeyword(
              keyword == null ? ImmutableList.of() : ImmutableList.of(keyword));
      if (expression == null) {
        return null;
      }
      result.add(expression);
      if (!eatToken(",")) {
        break;
      }
    }
    if (start == pos) {
      return null;
    }
    return result;
  }

  String parseExpression() {
    return parseExpressionUntilKeyword(ImmutableList.of());
  }

  String parseExpressionUntilKeyword(ImmutableList<String> keywords) {
    skipWhitespaces();
    int start = pos;
    boolean quoted = false;
    char startQuote = 0;
    int parens = 0;
    while (pos < sql.length()) {
      if (quoted) {
        if (sql.charAt(pos) == startQuote && sql.charAt(pos - 1) != '\\') {
          quoted = false;
        }
      } else {
        if (sql.charAt(pos) == '\'' || sql.charAt(pos) == '"') {
          quoted = true;
          startQuote = sql.charAt(pos);
        } else if (sql.charAt(pos) == '(') {
          parens++;
        } else if (sql.charAt(pos) == ')') {
          parens--;
          if (parens < 0) {
            break;
          }
        } else if (parens == 0 && sql.charAt(pos) == ',') {
          break;
        }
        if (keywords.stream().anyMatch(this::peekKeyword)) {
          break;
        }
      }
      pos++;
    }
    if (pos == start || quoted || parens > 0) {
      return null;
    }
    return sql.substring(start, pos).trim();
  }

  TableOrIndexName readTableOrIndexName() {
    String nameOrSchema = readIdentifierPart();
    if (nameOrSchema == null) {
      return null;
    }
    if (peekToken(".")) {
      String name = "";
      if (eatDotOperator()) {
        name = readIdentifierPart();
        if (name == null) {
          name = "";
        }
      }
      return new TableOrIndexName(nameOrSchema, name);
    }
    return new TableOrIndexName(nameOrSchema);
  }

  String readIdentifierPart() {
    skipWhitespaces();
    if (pos >= sql.length()) {
      return null;
    }
    boolean quoted = sql.charAt(pos) == '"';
    int start = pos;
    if (quoted) {
      pos++;
    }
    boolean first = true;
    while (pos < sql.length()) {
      if (quoted) {
        if (sql.charAt(pos) == '"') {
          if (pos < (sql.length() - 1) && sql.charAt(pos + 1) == '"') {
            pos++;
          } else {
            return sql.substring(start, ++pos);
          }
        }
      } else {
        if (first) {
          if (!isValidIdentifierFirstChar(sql.charAt(pos))) {
            return null;
          }
          first = false;
        } else {
          if (!isValidIdentifierChar(sql.charAt(pos))) {
            return sql.substring(start, pos);
          }
        }
      }
      pos++;
    }
    if (quoted) {
      return null;
    }
    return sql.substring(start);
  }

  private boolean isValidIdentifierFirstChar(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private boolean isValidIdentifierChar(char c) {
    return isValidIdentifierFirstChar(c) || Character.isDigit(c) || c == '$';
  }

  boolean peekKeyword(String keyword) {
    return peek(true, true, keyword);
  }

  boolean peekToken(String token) {
    return peek(false, false, token);
  }

  boolean peek(boolean skipWhitespaceBefore, boolean requireWhitespaceAfter, String keyword) {
    return internalEat(keyword, skipWhitespaceBefore, requireWhitespaceAfter, false);
  }

  String eatAnyKeyword() {
    return null;
  }

  boolean eatKeyword(String... keywords) {
    return eat(true, true, keywords);
  }

  boolean eatToken(String token) {
    return eat(true, false, token);
  }

  boolean eatDotOperator() {
    if (eat(false, false, ".")) {
      if (pos == sql.length() || Character.isWhitespace(sql.charAt(pos))) {
        return false;
      }
      return true;
    }
    return false;
  }

  boolean eat(boolean skipWhitespaceBefore, boolean requireWhitespaceAfter, String... keywords) {
    boolean result = true;
    for (String keyword : keywords) {
      result &= internalEat(keyword, skipWhitespaceBefore, requireWhitespaceAfter, true);
    }
    return result;
  }

  private boolean internalEat(
      String keyword,
      boolean skipWhitespaceBefore,
      boolean requireWhitespaceAfter,
      boolean updatePos) {
    if (skipWhitespaceBefore) {
      skipWhitespaces();
    }
    if (pos + keyword.length() > sql.length()) {
      return false;
    }
    if (sql.substring(pos, pos + keyword.length()).equalsIgnoreCase(keyword)
        && (!requireWhitespaceAfter || isWhitespaceOrParensOrEnd(pos + keyword.length()))) {
      if (updatePos) {
        pos = pos + keyword.length();
      }
      return true;
    }
    return false;
  }

  private boolean isWhitespaceOrParensOrEnd(int index) {
    if (sql.length() == index) {
      return true;
    }
    return Character.isWhitespace(sql.charAt(index))
        || sql.charAt(index) == '('
        || sql.charAt(index) == ')';
  }

  void skipWhitespaces() {
    while (sql.length() > pos && Character.isWhitespace(sql.charAt(pos))) {
      pos++;
    }
  }
}
