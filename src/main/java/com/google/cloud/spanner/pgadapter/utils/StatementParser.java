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

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Objects;

// TODO: Replace everything here with parsing in the client library.
@InternalApi
public class StatementParser {
  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';
  private static final char DOUBLE_QUOTE = '"';
  private static final char HYPHEN = '-';
  private static final char SLASH = '/';
  private static final char ASTERISK = '*';
  private static final char DOLLAR = '$';

  /**
   * Simple method to escape SQL. Ultimately it is preferred that a user uses PreparedStatements but
   * for the case of psql emulation, we apply this to provide a simple layer of protection to the
   * user. This method simply duplicates all single quotes (i.e. ' becomes '').
   */
  public static String singleQuoteEscape(String sql) {
    return sql.replace("'", "''");
  }

  /** Determines the (update) command that was received from the sql string. */
  public static String parseCommand(String sql) {
    Preconditions.checkNotNull(sql);
    for (int i = 0; i < sql.length(); i++) {
      if (Character.isSpaceChar(sql.charAt(i))) {
        return sql.substring(0, i).toUpperCase();
      }
    }
    return sql.toUpperCase();
  }

  /** Returns true if the given sql string is the given command. */
  public static boolean isCommand(String command, String query) {
    Preconditions.checkNotNull(command);
    Preconditions.checkNotNull(query);
    if (query.equalsIgnoreCase(command)) {
      return true;
    }
    if (query.length() <= command.length()) {
      return false;
    }
    return Character.isSpaceChar(query.charAt(command.length()))
        && query.substring(0, command.length()).equalsIgnoreCase(command);
  }

  /**
   * Splits the given sql string into multiple sql statements. A semi-colon (;) indicates the end of
   * a statement.
   */
  public static ImmutableList<String> splitStatements(String sql) {
    // First check trivial cases with only one statement.
    int firstIndexOfDelimiter = sql.indexOf(STATEMENT_DELIMITER);
    if (firstIndexOfDelimiter == -1) {
      return ImmutableList.of(sql);
    }
    if (firstIndexOfDelimiter == sql.length() - 1) {
      return ImmutableList.of(sql.substring(0, sql.length() - 1));
    }

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    int index = 0;
    int i = 0;
    while (i < sql.length()) {
      if (sql.charAt(i) == SINGLE_QUOTE || sql.charAt(i) == DOUBLE_QUOTE) {
        i = skipQuotedString(sql, i);
      } else if (sql.charAt(i) == HYPHEN && sql.length() > (i + 1) && sql.charAt(i + 1) == HYPHEN) {
        i = skipSingleLineComment(sql, i);
      } else if (sql.charAt(i) == SLASH
          && sql.length() > (i + 1)
          && sql.charAt(i + 1) == ASTERISK) {
        i = skipMultiLineComment(sql, i);
      } else if (sql.charAt(i) == DOLLAR
          && sql.length() > (i + 1)
          && (sql.charAt(i + 1) == DOLLAR || Character.isJavaIdentifierPart(sql.charAt(i + 1)))
          && sql.indexOf(DOLLAR, i + 1) > -1) {
        i = skipDollarQuotedString(sql, i);
      } else {
        if (sql.charAt(i) == STATEMENT_DELIMITER) {
          String stmt = sql.substring(index, i).trim();
          builder.add(stmt);
          index = i + 1;
        }
        i++;
      }
    }

    if (index < sql.length()) {
      String trimmed = sql.substring(index).trim();
      if (trimmed.length() > 0) {
        builder.add(trimmed);
      }
    }
    return builder.build();
  }

  static int skipQuotedString(String sql, int startIndex) {
    char quote = sql.charAt(startIndex);
    int i = startIndex + 1;
    while (i < sql.length()) {
      if (sql.charAt(i) == quote) {
        if (sql.length() > (i + 1) && sql.charAt(i + 1) == quote) {
          // This is an escaped quote. Skip one ahead.
          i++;
        } else {
          return i + 1;
        }
      }
      i++;
    }
    return sql.length();
  }

  static int skipSingleLineComment(String sql, int startIndex) {
    int endIndex = sql.indexOf('\n', startIndex + 2);
    if (endIndex == -1) {
      return sql.length();
    }
    return endIndex + 1;
  }

  static int skipMultiLineComment(String sql, int startIndex) {
    int i = startIndex + 2;
    while (i < sql.length()) {
      if (sql.charAt(i) == ASTERISK && sql.length() > (i + 1) && sql.charAt(i + 1) == SLASH) {
        return i + 2;
      }
      i++;
    }
    return sql.length();
  }

  static String parseDollarQuotedTag(String sql, int startIndex) {
    // Look ahead to the next dollar sign (if any). Everything in between is the quote tag.
    StringBuilder tag = new StringBuilder();
    while (startIndex < sql.length()) {
      char c = sql.charAt(startIndex);
      if (c == DOLLAR) {
        return tag.toString();
      }
      if (!Character.isJavaIdentifierPart(c)) {
        break;
      }
      tag.append(c);
      startIndex++;
    }
    return null;
  }

  static int skipDollarQuotedString(String sql, int startIndex) {
    String tag = parseDollarQuotedTag(sql, startIndex + 1);
    if (tag == null) {
      return startIndex + 1;
    }
    int i = startIndex + 1;
    while (i < sql.length()) {
      if (sql.charAt(i) == DOLLAR) {
        String endTag = parseDollarQuotedTag(sql, i + 1);
        if (Objects.equals(tag, endTag)) {
          return i + endTag.length() + 2;
        }
      }
      i++;
    }
    return sql.length();
  }
}
