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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.Map.Entry;

public class TableParser {
  private static final ImmutableList<String> KEYWORDS_BEFORE_TABLE =
      ImmutableList.of("from", "join", "insert", "update", "delete");
  private final Statement originalStatement;
  private final SimpleParser parser;

  public TableParser(Statement statement) {
    this.originalStatement = statement;
    this.parser = new SimpleParser(statement.getSql());
  }

  public Statement replaceTables(Map<TableOrIndexName, TableOrIndexName> tableReplacements) {
    boolean potentialMatch = false;
    String lowerCaseSql = parser.getSql().toLowerCase();
    for (Entry<TableOrIndexName, TableOrIndexName> entry : tableReplacements.entrySet()) {
      if (lowerCaseSql.contains(entry.getKey().name.toLowerCase())
          && (entry.getKey().schema == null
              || lowerCaseSql.contains(entry.getKey().schema.toLowerCase()))) {
        potentialMatch = true;
        break;
      }
    }
    if (!potentialMatch) {
      return originalStatement;
    }

    boolean replaced = false;
    while (parser.getPos() < parser.getSql().length()) {
      parser.parseExpressionUntilKeyword(KEYWORDS_BEFORE_TABLE, false);
      if (parser.getPos() >= parser.getSql().length()) {
        break;
      }

      if (parser.eatKeyword("insert")) {
        parser.eatKeyword("into");
      } else if (parser.eatKeyword("delete")) {
        parser.eatKeyword("from");
      } else if (!(parser.eatKeyword("update")
          || parser.eatKeyword("from")
          || parser.eatKeyword("join"))) {
        // This shouldn't happen.
        return originalStatement;
      }
      if (parser.eatToken("(")) {
        continue;
      }
      int positionBeforeName = parser.getPos();
      TableOrIndexName tableOrIndexName = parser.readTableOrIndexName();
      if (tableOrIndexName == null) {
        continue;
      }
      if (tableReplacements.containsKey(tableOrIndexName)) {
        replaced = true;
        parser.setSql(
            parser.getSql().substring(0, positionBeforeName)
                + tableReplacements.get(tableOrIndexName)
                + parser.getSql().substring(parser.getPos()));
        // Reset the position to take into account that the new name might have been shorter than
        // the replaced name.
        parser.setPos(positionBeforeName);
      }
    }
    return replaced
        ? SimpleParser.copyStatement(originalStatement, parser.getSql())
        : originalStatement;
  }
}
