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

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

class TableParser {
  private static final ImmutableSet<TableOrIndexName> EMPTY_TABLE_SET = ImmutableSet.of();
  private static final ImmutableList<String> KEYWORDS_BEFORE_TABLE =
      ImmutableList.of("from", "join", "insert", "update", "delete");
  private final Statement originalStatement;
  private final SimpleParser parser;

  TableParser(Statement statement) {
    this.originalStatement = statement;
    this.parser = new SimpleParser(statement.getSql());
  }

  Tuple<Set<TableOrIndexName>, Statement> detectAndReplaceTables(
      ImmutableMap<TableOrIndexName, TableOrIndexName> detectAndReplaceMap) {
    return detectAndReplaceTables(detectAndReplaceMap, true);
  }

  Tuple<Set<TableOrIndexName>, Statement> detectAndReplaceTables(
      ImmutableMap<TableOrIndexName, TableOrIndexName> detectAndReplaceMap,
      boolean searchForKeywordBeforeFirstTable) {
    boolean potentialMatch = false;
    String lowerCaseSql = parser.getSql().toLowerCase();
    for (Entry<TableOrIndexName, TableOrIndexName> entry : detectAndReplaceMap.entrySet()) {
      if (lowerCaseSql.contains(entry.getKey().name.toLowerCase())
          && (entry.getKey().schema == null
              || lowerCaseSql.contains(entry.getKey().schema.toLowerCase()))) {
        potentialMatch = true;
        break;
      }
    }
    if (!potentialMatch) {
      return Tuple.of(EMPTY_TABLE_SET, originalStatement);
    }

    ImmutableSet.Builder<TableOrIndexName> detectedTablesBuilder = ImmutableSet.builder();
    boolean detectedOrReplacedTable = false;
    while (parser.getPos() < parser.getSql().length()) {
      boolean multipleTables = false;
      if (searchForKeywordBeforeFirstTable) {
        parser.parseExpressionUntilKeyword(KEYWORDS_BEFORE_TABLE, false, false);
        if (parser.getPos() >= parser.getSql().length()) {
          break;
        }

        if (parser.eatKeyword("insert")) {
          parser.eatKeyword("into");
        } else if (parser.eatKeyword("delete")) {
          parser.eatKeyword("from");
        } else if (parser.eatKeyword("update")) {
          parser.eatKeyword("only");
        } else if (parser.eatKeyword("from") || parser.eatKeyword("join")) {
          // There could be multiple tables.
          multipleTables = true;
        } else {
          // This shouldn't happen.
          return Tuple.of(EMPTY_TABLE_SET, originalStatement);
        }
      }
      searchForKeywordBeforeFirstTable = true;
      detectedOrReplacedTable =
          parseTableList(detectAndReplaceMap, detectedTablesBuilder, multipleTables)
              || detectedOrReplacedTable;
    }
    return detectedOrReplacedTable
        ? Tuple.of(
            detectedTablesBuilder.build(),
            SimpleParser.copyStatement(originalStatement, parser.getSql()))
        : Tuple.of(EMPTY_TABLE_SET, originalStatement);
  }

  private boolean parseTableList(
      ImmutableMap<TableOrIndexName, TableOrIndexName> detectAndReplaceMap,
      ImmutableSet.Builder<TableOrIndexName> detectedTablesBuilder,
      boolean multipleTables) {
    boolean detectedOrReplacedTable = false;
    boolean wasJoin = false;
    do {
      if (parser.eatToken("(")) {
        int startPosition = parser.getPos();
        String subExpression = parser.eatSubExpression();
        TableParser subParser = new TableParser(Statement.of(subExpression));
        Tuple<Set<TableOrIndexName>, Statement> subResult =
            subParser.detectAndReplaceTables(
                detectAndReplaceMap,
                subParser.parser.peekKeyword("select") || subParser.parser.peekKeyword("with"));
        if (!subResult.x().isEmpty()) {
          detectedOrReplacedTable = true;
          detectedTablesBuilder.addAll(subResult.x());
          parser.setSql(
              parser.getSql().substring(0, startPosition)
                  + subResult.y().getSql()
                  + parser.getSql().substring(parser.getPos()));
          // Set the position of the parser to after the replaced sub-expression.
          parser.setPos(startPosition + subResult.y().getSql().length());
          parser.eatToken(")");
        }
      } else {
        // Skip all whitespaces to get the actual position before the next table name.
        parser.skipWhitespaces();
        int positionBeforeName = parser.getPos();
        TableOrIndexName tableOrIndexName = parser.readTableOrIndexName();
        if (tableOrIndexName == null) {
          break;
        }
        if (detectAndReplaceMap.containsKey(tableOrIndexName)) {
          detectedOrReplacedTable = true;
          // Add the translated table name to the set of discovered tables so that a CTE can be
          // added
          // for it.
          detectedTablesBuilder.add(
              Objects.requireNonNull(detectAndReplaceMap.get(tableOrIndexName)));
          // Check if the entry in the table map contains a different replacement value than the
          // original. Some tables may be added to the map of replacements with the same replacement
          // value as the original with the sole purpose of detecting the use of the table.
          if (!Objects.equals(detectAndReplaceMap.get(tableOrIndexName), tableOrIndexName)) {
            parser.setSql(
                parser.getSql().substring(0, positionBeforeName)
                    + detectAndReplaceMap.get(tableOrIndexName)
                    + parser.getSql().substring(parser.getPos()));
            // Set the position of the parser to after the replaced table name.
            parser.setPos(
                positionBeforeName
                    + Objects.requireNonNull(detectAndReplaceMap.get(tableOrIndexName))
                        .toString()
                        .length());
          }
        }
      }
      // Skip any aliases.
      if (multipleTables && !parser.peekJoinKeyword()) {
        parser.eatKeyword("as");
        parser.readIdentifierPart();
      }
      if (wasJoin) {
        // Skip the join condition.
        parser.eatJoinCondition();
      }
    } while (multipleTables && (parser.eatToken(",") || (wasJoin = parser.eatJoinType())));

    return detectedOrReplacedTable;
  }

  @Override
  public String toString() {
    return parser.toString();
  }
}
