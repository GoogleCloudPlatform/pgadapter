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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.text.StringEscapeUtils;

/** A very simple parser that can interpret SQL statements to find specific parts in the string. */
@InternalApi
public class SimpleParser {
  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';
  private static final char DOUBLE_QUOTE = '"';
  private static final char HYPHEN = '-';
  private static final char SLASH = '/';
  private static final char ASTERISK = '*';
  private static final char DOLLAR = '$';

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

    public String getUnquotedSchema() {
      return unquoteOrFoldIdentifier(this.schema);
    }

    public String getUnquotedName() {
      return unquoteOrFoldIdentifier(this.name);
    }

    public String getUnquotedQualifiedName() {
      if (schema == null) {
        return getUnquotedName();
      }
      return getUnquotedSchema() + "." + getUnquotedName();
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
      return Objects.equals(
              unquoteOrFoldIdentifier(this.schema), unquoteOrFoldIdentifier(other.schema))
          && Objects.equals(
              unquoteOrFoldIdentifier(this.name), unquoteOrFoldIdentifier(other.name));
    }

    @Override
    public int hashCode() {
      return Objects.hash(unquoteOrFoldIdentifier(this.schema), unquoteOrFoldIdentifier(this.name));
    }
  }

  static class TypeDefinition {
    final String name;
    final int length;
    final int scale;
    final boolean array;

    TypeDefinition(String name, int length, int scale, boolean array) {
      this.name = name;
      this.length = length;
      this.scale = scale;
      this.array = array;
    }

    String getNameAndArrayBrackets() {
      return array ? name + "[]" : name;
    }
  }

  static class QuotedString {
    final boolean escaped;
    final char quote;
    final String rawValue;
    private String value;

    QuotedString(boolean escaped, char quote, String rawValue) {
      this.escaped = escaped;
      this.quote = quote;
      this.rawValue = rawValue;
    }

    String getValue() {
      if (this.value == null) {
        this.value =
            this.escaped
                ? unescapeQuotedStringValue(this.rawValue, this.quote)
                : quotedStringValue(this.rawValue, this.quote);
      }
      return this.value;
    }

    static String quotedStringValue(String quotedString, char quoteChar) {
      if (quotedString.length() < 2
          || quotedString.charAt(0) != quoteChar
          || quotedString.charAt(quotedString.length() - 1) != quoteChar) {
        throw PGExceptionFactory.newPGException(
            quotedString + " is not a valid string", SQLState.SyntaxError);
      }
      String doubleQuotes = String.valueOf(quoteChar) + quoteChar;
      String singleQuote = String.valueOf(quoteChar);
      return quotedString
          .substring(1, quotedString.length() - 1)
          .replace(doubleQuotes, singleQuote);
    }

    static String unescapeQuotedStringValue(String quotedString, char quoteChar) {
      if (quotedString.length() < 2
          || quotedString.charAt(0) != quoteChar
          || quotedString.charAt(quotedString.length() - 1) != quoteChar) {
        throw PGExceptionFactory.newPGException(
            quotedString + " is not a valid string", SQLState.SyntaxError);
      }
      if (quotedString.startsWith(quoteChar + "\\x")) {
        throw PGExceptionFactory.newPGException(
            "PGAdapter does not support hexadecimal byte values in string literals",
            SQLState.SyntaxError);
      }
      String result =
          StringEscapeUtils.unescapeJava(quotedString.substring(1, quotedString.length() - 1));
      String doubleQuotes = String.valueOf(quoteChar) + quoteChar;
      String singleQuote = String.valueOf(quoteChar);
      return result.replace(doubleQuotes, singleQuote);
    }
  }

  static String unquoteOrFoldIdentifier(String identifier) {
    if (Strings.isNullOrEmpty(identifier)) {
      return null;
    }
    if (identifier.charAt(0) == '"'
        && identifier.charAt(identifier.length() - 1) == '"'
        && identifier.length() > 1) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier.toLowerCase();
  }

  static Statement copyStatement(Statement original, String sql) {
    Statement.Builder builder = Statement.newBuilder(sql);
    Map<String, Value> parameters = original.getParameters();
    for (Entry<String, Value> param : parameters.entrySet()) {
      builder.bind(param.getKey()).to(param.getValue());
    }
    return builder.build();
  }

  private String sql;
  private int pos;

  SimpleParser(String sql) {
    this.sql = sql;
  }

  String getSql() {
    return sql;
  }

  void setSql(String sql) {
    this.sql = sql;
  }

  int getPos() {
    return pos;
  }

  void setPos(int pos) {
    this.pos = pos;
  }

  /** Returns the command tag of the given SQL string */
  public static String parseCommand(String sql) {
    SimpleParser parser = new SimpleParser(sql);
    if (parser.eatKeyword("with")) {
      do {
        if (!parser.skipCommonTableExpression()) {
          // Return WITH as the command tag if we encounter an invalid CTE. This is for safety, as
          // the chances that we encounter an invalid CTE is a lot larger than encountering any
          // other statement without a logical first keyword.
          return "WITH";
        }
      } while (parser.eatToken(","));
    }
    String keyword = parser.readKeyword();
    if (Strings.isNullOrEmpty(keyword)) {
      keyword = new SimpleParser(sql).readKeyword();
    }
    return keyword.toUpperCase();
  }

  /** Returns true if the given sql string is the given command. */
  public static boolean isCommand(String command, String query) {
    Preconditions.checkNotNull(command);
    Preconditions.checkNotNull(query);
    return new SimpleParser(query).peekKeyword(command);
  }

  /**
   * Splits the given sql string into multiple sql statements. A semi-colon (;) indicates the end of
   * a statement.
   */
  ImmutableList<String> splitStatements() {
    // First check trivial cases with only one statement.
    int firstIndexOfDelimiter = sql.indexOf(STATEMENT_DELIMITER);
    if (firstIndexOfDelimiter == -1) {
      return ImmutableList.of(sql);
    }
    if (firstIndexOfDelimiter == sql.length() - 1) {
      return ImmutableList.of(sql.substring(0, sql.length() - 1));
    }

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    int lastFoundSeparatorPos = 0;
    while (skipCommentsAndLiterals() && pos < sql.length()) {
      if (sql.charAt(pos) == STATEMENT_DELIMITER) {
        String stmt = sql.substring(lastFoundSeparatorPos, pos).trim();
        builder.add(stmt);
        lastFoundSeparatorPos = pos + 1;
      }
      pos++;
    }

    if (lastFoundSeparatorPos < sql.length()) {
      String trimmed = sql.substring(lastFoundSeparatorPos).trim();
      if (trimmed.length() > 0) {
        builder.add(trimmed);
      }
    }
    return builder.build();
  }

  boolean skipCommonTableExpression() {
    if (readIdentifierPart() == null) {
      return false;
    }
    if (eatToken("(")) {
      List<String> columnNames = parseExpressionList();
      if (columnNames == null || columnNames.isEmpty()) {
        return false;
      }
      if (!eatToken(")")) {
        return false;
      }
    }
    if (!eatKeyword("as")) {
      return false;
    }
    if (!eatToken("(")) {
      return false;
    }
    parseExpressionUntilKeyword(ImmutableList.of());
    if (!eatToken(")")) {
      return false;
    }
    return true;
  }

  List<String> parseExpressionList() {
    return parseExpressionListUntilKeyword(null, false);
  }

  List<String> parseExpressionListUntilKeyword(
      @Nullable String keyword, boolean sameParensLevelAsStart) {
    skipWhitespaces();
    List<String> result = new ArrayList<>();
    int start = pos;
    while (pos < sql.length()) {
      String expression =
          parseExpressionUntilKeyword(
              keyword == null ? ImmutableList.of() : ImmutableList.of(keyword),
              sameParensLevelAsStart,
              true);
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
    return parseExpressionUntilKeyword(keywords, false, true);
  }

  String parseExpressionUntilKeyword(
      ImmutableList<String> keywords,
      boolean sameParensLevelAsStart,
      boolean stopAtEndOfExpression) {
    skipWhitespaces();
    int start = pos;
    boolean valid;
    int parens = 0;
    while ((valid = skipCommentsAndLiterals()) && pos < sql.length()) {
      if (sql.charAt(pos) == '(') {
        parens++;
      } else if (sql.charAt(pos) == ')') {
        parens--;
        if (stopAtEndOfExpression && parens < 0) {
          break;
        }
      } else if (stopAtEndOfExpression && parens == 0 && sql.charAt(pos) == ',') {
        break;
      }
      if ((!sameParensLevelAsStart || parens == 0)
          && keywords.stream().anyMatch(this::peekKeyword)) {
        break;
      }
      pos++;
    }
    if (pos == start || !valid || parens > 0) {
      return null;
    }
    return sql.substring(start, pos).trim();
  }

  List<TableOrIndexName> readColumnList(String name) {
    return expressionListToColumnNames(name, parseExpressionListUntilKeyword(null, true));
  }

  List<TableOrIndexName> readColumnListInParentheses(String name) {
    if (eatToken("(")) {
      List<String> expressions = parseExpressionListUntilKeyword(")", true);
      if (!eatToken(")")) {
        throw PGExceptionFactory.newPGException(
            String.format("missing closing parentheses for %s column list", name),
            SQLState.SyntaxError);
      }
      return expressionListToColumnNames(name, expressions);
    } else {
      throw PGExceptionFactory.newPGException(
          String.format("missing opening parentheses for %s", name), SQLState.SyntaxError);
    }
  }

  List<TableOrIndexName> expressionListToColumnNames(String name, List<String> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      throw PGExceptionFactory.newPGException(
          String.format("empty %s columns list", name), SQLState.SyntaxError);
    }
    List<TableOrIndexName> result = new ArrayList<>(expressions.size());
    for (String expression : expressions) {
      TableOrIndexName column = new SimpleParser(expression).readTableOrIndexName();
      if (column == null) {
        throw PGExceptionFactory.newPGException(
            "Invalid column name: " + expression, SQLState.SyntaxError);
      }
      result.add(column);
    }
    return result;
  }

  @Nonnull
  String readKeyword() {
    skipWhitespaces();
    int startPos = pos;
    while (pos < sql.length() && !isValidEndOfKeyword(pos)) {
      pos++;
    }
    return sql.substring(startPos, pos);
  }

  TypeDefinition readType() {
    StringBuilder name = new StringBuilder();
    int length = 0;
    int scale = 0;
    boolean array = false;
    String keyword;
    while (!(keyword = readKeyword()).equalsIgnoreCase("")) {
      if (name.length() > 0) {
        name.append(' ');
      }
      name.append(keyword);
    }
    if (eatToken("(")) {
      String lengthString = parseExpression();
      length = parseInt(lengthString);
      if (eatToken(",")) {
        String scaleExpression = parseExpression();
        scale = parseInt(scaleExpression);
      }
      if (!eatToken(")")) {
        throw PGExceptionFactory.newPGException("missing ')' for type length");
      }
    }
    if (eatToken("[")) {
      if (!eatToken("]")) {
        throw PGExceptionFactory.newPGException("missing ']' for array definition");
      }
      array = true;
    }
    return new TypeDefinition(name.toString(), length, scale, array);
  }

  int parseInt(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException exception) {
      throw PGExceptionFactory.toPGException(exception);
    }
  }

  TableOrIndexName readTableOrIndexName() {
    String nameOrSchema = readIdentifierPart();
    if (nameOrSchema == null) {
      return null;
    }
    if (peek(false, false, ".")) {
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
    return peek(true, false, token);
  }

  boolean peek(boolean skipWhitespaceBefore, boolean requireWhitespaceAfter, String keyword) {
    return internalEat(keyword, skipWhitespaceBefore, requireWhitespaceAfter, false);
  }

  boolean eatKeyword(String... keywords) {
    return eat(true, true, keywords);
  }

  boolean eatToken(String token) {
    return eat(true, false, token);
  }

  /** Eats everything until an end parentheses at the same level as the current level. */
  String eatSubExpression() {
    int start = pos;
    boolean valid;
    int parens = 0;
    while ((valid = skipCommentsAndLiterals()) && pos < sql.length()) {
      if (sql.charAt(pos) == '(') {
        parens++;
      } else if (sql.charAt(pos) == ')') {
        parens--;
        if (parens < 0) {
          break;
        }
      }
      pos++;
    }
    if (pos == start || !valid || parens >= 0) {
      return null;
    }
    return sql.substring(start, pos);
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

  boolean peekJoinKeyword() {
    return peekKeyword("join")
        || peekKeyword("left")
        || peekKeyword("right")
        || peekKeyword("full")
        || peekKeyword("inner")
        || peekKeyword("outer")
        || peekKeyword("cross");
  }

  boolean eatJoinType() {
    boolean foundValidJoin;
    if (eatKeyword("left") || eatKeyword("right") || eatKeyword("full")) {
      eatKeyword("outer");
      foundValidJoin = eatKeyword("join");
    } else if (eatKeyword("inner")) {
      foundValidJoin = eatKeyword("join");
    } else if (eatKeyword("cross")) {
      foundValidJoin = eatKeyword("join");
    } else {
      foundValidJoin = eatKeyword("join");
    }
    return foundValidJoin;
  }

  boolean eatJoinCondition() {
    if (eatKeyword("on")) {
      parseExpressionUntilKeyword(
          ImmutableList.of("left", "right", "full", "inner", "cross", "join", "where"), true, true);
    } else if (eatKeyword("using")) {
      if (eatToken("(")) {
        parseExpressionList();
        return eatToken(")");
      }
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
    int originalPos = pos;
    if (skipWhitespaceBefore) {
      skipWhitespaces();
    }
    if (pos + keyword.length() > sql.length()) {
      if (!updatePos) {
        pos = originalPos;
      }
      return false;
    }
    if (sql.substring(pos, pos + keyword.length()).equalsIgnoreCase(keyword)
        && (!requireWhitespaceAfter || isValidEndOfKeyword(pos + keyword.length()))) {
      if (updatePos) {
        pos = pos + keyword.length();
      } else {
        pos = originalPos;
      }
      return true;
    }
    if (!updatePos) {
      pos = originalPos;
    }
    return false;
  }

  private boolean isValidEndOfKeyword(int index) {
    if (sql.length() == index) {
      return true;
    }
    return !isValidIdentifierChar(sql.charAt(index));
  }

  boolean skipCommentsAndLiterals() {
    if (pos >= sql.length()) {
      return true;
    }
    if ((sql.charAt(pos) == 'e' || sql.charAt(pos) == 'E')
        && sql.length() > (pos + 1)
        && sql.charAt(pos + 1) == '\'') {
      pos++;
      return skipQuotedString(true);
    } else if (sql.charAt(pos) == SINGLE_QUOTE || sql.charAt(pos) == DOUBLE_QUOTE) {
      return skipQuotedString(false);
    } else if (sql.charAt(pos) == HYPHEN
        && sql.length() > (pos + 1)
        && sql.charAt(pos + 1) == HYPHEN) {
      return skipSingleLineComment();
    } else if (sql.charAt(pos) == SLASH
        && sql.length() > (pos + 1)
        && sql.charAt(pos + 1) == ASTERISK) {
      return skipMultiLineComment();
    } else if (sql.charAt(pos) == DOLLAR
        && sql.length() > (pos + 1)
        && (sql.charAt(pos + 1) == DOLLAR || isValidIdentifierFirstChar(sql.charAt(pos + 1)))
        && sql.indexOf(DOLLAR, pos + 1) > -1) {
      return skipDollarQuotedString();
    } else {
      return true;
    }
  }

  QuotedString readSingleQuotedString() {
    return readQuotedString(SINGLE_QUOTE);
  }

  QuotedString readDoubleQuotedString() {
    return readQuotedString(DOUBLE_QUOTE);
  }

  QuotedString readQuotedString(char quote) {
    skipWhitespaces();
    if (pos >= sql.length()) {
      throw PGExceptionFactory.newPGException("Unexpected end of expression", SQLState.SyntaxError);
    }
    boolean escaped = eatToken("e");
    if (sql.charAt(pos) != quote) {
      throw PGExceptionFactory.newPGException(
          "Invalid quote character: " + sql.charAt(pos), SQLState.SyntaxError);
    }
    int startPos = pos;
    if (skipQuotedString(escaped)) {
      return new QuotedString(escaped, quote, sql.substring(startPos, pos));
    }
    throw PGExceptionFactory.newPGException("Missing end quote character", SQLState.SyntaxError);
  }

  boolean skipQuotedString(boolean escaped) {
    char quote = sql.charAt(pos);
    pos++;
    while (pos < sql.length()) {
      if (sql.charAt(pos) == quote) {
        if (sql.length() > (pos + 1) && sql.charAt(pos + 1) == quote) {
          // This is an escaped quote. Skip one ahead.
          pos++;
        } else {
          pos++;
          return true;
        }
      } else if (escaped
          && sql.charAt(pos) == '\\'
          && sql.length() > (pos + 1)
          && sql.charAt(pos + 1) == quote) {
        // This is an escaped quote. Skip one ahead.
        pos++;
      } else if (escaped
          && sql.charAt(pos) == '\\'
          && sql.length() > (pos + 1)
          && sql.charAt(pos + 1) == '\\') {
        // This is an escaped backslash. Skip one ahead.
        pos++;
      }
      pos++;
    }
    pos = sql.length();
    return false;
  }

  void skipWhitespaces() {
    while (pos < sql.length()) {
      if (sql.charAt(pos) == HYPHEN && sql.length() > (pos + 1) && sql.charAt(pos + 1) == HYPHEN) {
        skipSingleLineComment();
      } else if (sql.charAt(pos) == SLASH
          && sql.length() > (pos + 1)
          && sql.charAt(pos + 1) == ASTERISK) {
        skipMultiLineComment();
      } else if (Character.isWhitespace(sql.charAt(pos))) {
        pos++;
      } else {
        break;
      }
    }
  }

  boolean skipSingleLineComment() {
    int endIndex = sql.indexOf('\n', pos + 2);
    if (endIndex == -1) {
      pos = sql.length();
      return true;
    }
    pos = endIndex + 1;
    return true;
  }

  boolean skipMultiLineComment() {
    int level = 1;
    pos += 2;
    while (pos < sql.length()) {
      if (sql.charAt(pos) == SLASH && sql.length() > (pos + 1) && sql.charAt(pos + 1) == ASTERISK) {
        level++;
      }
      if (sql.charAt(pos) == ASTERISK && sql.length() > (pos + 1) && sql.charAt(pos + 1) == SLASH) {
        level--;
        if (level == 0) {
          pos += 2;
          return true;
        }
      }
      pos++;
    }
    pos = sql.length();
    return false;
  }

  String parseDollarQuotedTag() {
    // Look ahead to the next dollar sign (if any). Everything in between is the quote tag.
    StringBuilder tag = new StringBuilder();
    while (pos < sql.length()) {
      char c = sql.charAt(pos);
      if (c == DOLLAR) {
        pos++;
        return tag.toString();
      }
      if (!isValidIdentifierChar(c)) {
        break;
      }
      tag.append(c);
      pos++;
    }
    return null;
  }

  boolean skipDollarQuotedString() {
    if (sql.charAt(pos) != DOLLAR) {
      return false;
    }
    pos++;
    String tag = parseDollarQuotedTag();
    if (tag == null) {
      return false;
    }
    while (pos < sql.length()) {
      if (sql.charAt(pos++) == DOLLAR) {
        int currentPos = pos;
        String endTag = parseDollarQuotedTag();
        if (Objects.equals(tag, endTag)) {
          return true;
        }
        pos = currentPos;
      }
    }
    return false;
  }

  boolean hasMoreTokens() {
    skipWhitespaces();
    return getPos() < getSql().length();
  }

  void throwIfHasMoreTokens() {
    skipWhitespaces();
    if (getPos() < getSql().length()) {
      throw PGExceptionFactory.newPGException(
          "Syntax error. Unexpected tokens: " + getSql().substring(getPos()), SQLState.SyntaxError);
    }
  }

  @Override
  public String toString() {
    if (sql.contains("\n")) {
      return sql.substring(0, pos) + "|" + sql.substring(Math.min(pos, sql.length() - 1));
    }
    return sql
        + "\n"
        + Strings.repeat(" ", pos)
        + "^"
        + Strings.repeat(" ", Math.max(sql.length() - pos - 1, 0));
  }
}
