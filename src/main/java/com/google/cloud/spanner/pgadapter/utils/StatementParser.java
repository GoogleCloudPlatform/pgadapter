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
    String[] tokens = sql.split("\\s+", 2);
    if (tokens.length > 0) {
      return tokens[0].toUpperCase();
    }
    return null;
  }
}
