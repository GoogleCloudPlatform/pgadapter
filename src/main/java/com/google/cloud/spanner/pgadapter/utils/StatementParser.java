// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.common.base.Preconditions;

public class StatementParser {
  private static final String GSQL_STATEMENT = "/*GSQL*/";
  private static final char SINGLE_QUOTE = '\'';
  private static final char DOUBLE_QUOTE = '"';
  private static final char BACKTICK_QUOTE = '`';
  private static final char HYPHEN = '-';
  private static final char DASH = '#';
  private static final char SLASH = '/';
  private static final char BACK_SLASH = '\\';
  private static final char ASTERISK = '*';
  private static final char DOLLAR = '$';
  private static final char SEMICOLON = ';';
  /**
   * Removes comments from and trims the given sql statement. Spanner supports three types of
   * comments:
   *
   * <ul>
   *   <li>Single line comments starting with '--'
   *   <li>Single line comments starting with '#'
   *   <li>Multi line comments between '/&#42;' and '&#42;/'
   * </ul>
   *
   * Reference: https://cloud.google.com/spanner/docs/lexical#comments
   *
   * @param sql The sql statement to remove comments from and to trim.
   * @return the sql statement without the comments and leading and trailing spaces.
   */
  public static String removeCommentsAndTrim(String sql) {
    if (sql.startsWith(GSQL_STATEMENT)) {
      return removeCommentsAndTrimGSQL(sql);
    }
    return removeCommentsAndTrimPostgres(sql);
  }

  private static String removeCommentsAndTrimPostgres(String sql) {
    Preconditions.checkNotNull(sql);
    String currentTag = null;
    boolean isInQuoted = false;
    boolean isInSingleLineComment = false;
    int multiLineCommentLevel = 0;
    char startQuote = 0;
    boolean lastCharWasEscapeChar = false;
    StringBuilder res = new StringBuilder(sql.length());
    int index = 0;
    while (index < sql.length()) {
      char c = sql.charAt(index);
      if (isInQuoted) {
        if ((c == '\n' || c == '\r') && startQuote != DOLLAR) {
          throw new IllegalArgumentException("SQL statement contains an unclosed literal: " + sql);
        } else if (c == startQuote) {
          if (c == DOLLAR) {
            // Check if this is the end of the current dollar quoted string.
            String tag = parseDollarQuotedString(sql, index + 1);
            if (tag != null && tag.equals(currentTag)) {
              index += tag.length() + 1;
              res.append(c);
              res.append(tag);
              isInQuoted = false;
              startQuote = 0;
            }
          } else if (lastCharWasEscapeChar) {
            lastCharWasEscapeChar = false;
          } else {
            isInQuoted = false;
            startQuote = 0;
          }
        } else if (c == BACK_SLASH) {
          lastCharWasEscapeChar = true;
        } else {
          lastCharWasEscapeChar = false;
        }
        res.append(c);
      } else {
        // We are not in a quoted string.
        if (isInSingleLineComment) {
          if (c == '\n') {
            isInSingleLineComment = false;
            // Include the line feed in the result.
            res.append(c);
          }
        } else if (multiLineCommentLevel > 0) {
          if (sql.length() > index + 1 && c == ASTERISK && sql.charAt(index + 1) == SLASH) {
            multiLineCommentLevel--;
            index++;
          } else if (sql.length() > index + 1 && c == SLASH && sql.charAt(index + 1) == ASTERISK) {
            multiLineCommentLevel++;
            index++;
          }
        } else {
          // Check for -- which indicates the start of a single-line comment.
          if (sql.length() > index + 1 && c == HYPHEN && sql.charAt(index + 1) == HYPHEN) {
            // This is a single line comment.
            isInSingleLineComment = true;
          } else if (sql.length() > index + 1 && c == SLASH && sql.charAt(index + 1) == ASTERISK) {
            multiLineCommentLevel++;
            index++;
          } else {
            if (c == SINGLE_QUOTE || c == DOUBLE_QUOTE) {
              isInQuoted = true;
              startQuote = c;
            } else if (c == DOLLAR
                && sql.length() > index + 1
                && !Character.isDigit(sql.charAt(index + 1))) {
              currentTag = parseDollarQuotedString(sql, index + 1);
              if (currentTag != null) {
                isInQuoted = true;
                startQuote = DOLLAR;
                index += currentTag.length() + 1;
                res.append(c);
                res.append(currentTag);
              }
            }
            res.append(c);
          }
        }
      }
      index++;
    }
    if (isInQuoted) {
      throw new IllegalArgumentException("SQL statement contains an unclosed literal: " + sql);
    }
    if (multiLineCommentLevel > 0) {
      throw new IllegalArgumentException(
          "SQL statement contains an unterminated block comment: " + sql);
    }
    if (res.length() > 0 && res.charAt(res.length() - 1) == SEMICOLON) {
      res.deleteCharAt(res.length() - 1);
    }
    return res.toString().trim();
  }

  static String parseDollarQuotedString(String sql, int index) {
    // Look ahead to the next dollar sign (if any). Everything in between is the quote tag.
    final char DOLLAR = '$';
    StringBuilder tag = new StringBuilder();
    while (index < sql.length()) {
      char c = sql.charAt(index);
      if (c == DOLLAR) {
        return tag.toString();
      }
      if (!Character.isJavaIdentifierPart(c)) {
        break;
      }
      tag.append(c);
      index++;
    }
    return null;
  }

  private static String removeCommentsAndTrimGSQL(String sql) {
    Preconditions.checkNotNull(sql);
    boolean isInQuoted = false;
    boolean isInSingleLineComment = false;
    boolean isInMultiLineComment = false;
    char startQuote = 0;
    boolean lastCharWasEscapeChar = false;
    boolean isTripleQuoted = false;
    StringBuilder res = new StringBuilder(sql.length());
    int index = 0;
    while (index < sql.length()) {
      char c = sql.charAt(index);
      if (isInQuoted) {
        if ((c == '\n' || c == '\r') && !isTripleQuoted) {
          throw new IllegalArgumentException("SQL statement contains an unclosed literal: " + sql);
        } else if (c == startQuote) {
          if (lastCharWasEscapeChar) {
            lastCharWasEscapeChar = false;
          } else if (isTripleQuoted) {
            if (sql.length() > index + 2
                && sql.charAt(index + 1) == startQuote
                && sql.charAt(index + 2) == startQuote) {
              isInQuoted = false;
              startQuote = 0;
              isTripleQuoted = false;
              res.append(c).append(c);
              index += 2;
            }
          } else {
            isInQuoted = false;
            startQuote = 0;
          }
        } else if (c == BACK_SLASH) {
          lastCharWasEscapeChar = true;
        } else {
          lastCharWasEscapeChar = false;
        }
        res.append(c);
      } else {
        // We are not in a quoted string.
        if (isInSingleLineComment) {
          if (c == '\n') {
            isInSingleLineComment = false;
            // Include the line feed in the result.
            res.append(c);
          }
        } else if (isInMultiLineComment) {
          if (sql.length() > index + 1 && c == ASTERISK && sql.charAt(index + 1) == SLASH) {
            isInMultiLineComment = false;
            index++;
          }
        } else {
          if (c == DASH
              || (sql.length() > index + 1 && c == HYPHEN && sql.charAt(index + 1) == HYPHEN)) {
            // This is a single line comment.
            isInSingleLineComment = true;
          } else if (sql.length() > index + 1 && c == SLASH && sql.charAt(index + 1) == ASTERISK) {
            isInMultiLineComment = true;
            index++;
          } else {
            if (c == SINGLE_QUOTE || c == DOUBLE_QUOTE || c == BACKTICK_QUOTE) {
              isInQuoted = true;
              startQuote = c;
              // Check whether it is a triple-quote.
              if (sql.length() > index + 2
                  && sql.charAt(index + 1) == startQuote
                  && sql.charAt(index + 2) == startQuote) {
                isTripleQuoted = true;
                res.append(c).append(c);
                index += 2;
              }
            }
            res.append(c);
          }
        }
      }
      index++;
    }
    if (isInQuoted) {
      throw new IllegalArgumentException("SQL statement contains an unclosed literal: " + sql);
    }
    if (res.length() > 0 && res.charAt(res.length() - 1) == SEMICOLON) {
      res.deleteCharAt(res.length() - 1);
    }
    return GSQL_STATEMENT + res.toString().trim();
  }

  /**
   * Simple method to escape SQL. Ultimately it is preferred that a user uses PreparedStatements but
   * for the case of psql emulation, we apply this to provide a simple layer of protection to the
   * user. Here we simple escape the single quote if it is not currently escaped. Note that the only
   * reason we are doing it manually is because StringEscapeUtils deprecated escapeSql.
   */
  public static String singleQuoteEscape(String sql) {
    StringBuilder result = new StringBuilder();
    boolean currentCharacterIsEscaped = false;
    for (char currentCharacter : sql.toCharArray()) {
      if (currentCharacterIsEscaped) {
        currentCharacterIsEscaped = false;
      } else if (currentCharacter == '\\') {
        currentCharacterIsEscaped = true;
      } else if (currentCharacter == '\'') {
        result.append('\\');
      }
      result.append(currentCharacter);
    }
    return result.toString();
  }
}
