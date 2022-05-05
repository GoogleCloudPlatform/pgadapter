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
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

// TODO: Replace everything here with parsing in the client library.
@InternalApi
public class StatementParser {

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

  public static ImmutableList<String> parseCommands(ImmutableList<ParsedStatement> statements) {
    Preconditions.checkNotNull(statements);

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (ParsedStatement stat : statements) {
      builder.add(parseCommand(stat.getSqlWithoutComments()));
    }
    return builder.build();
  }
}
