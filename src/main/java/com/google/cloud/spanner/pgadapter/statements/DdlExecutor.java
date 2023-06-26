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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer.ReplacementStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.function.Supplier;

/**
 * {@link DdlExecutor} inspects DDL statements before executing these to support commonly used DDL
 * features that are not (yet) supported by the backend.
 */
class DdlExecutor {

  private final BackendConnection backendConnection;
  private final Connection connection;
  private final Supplier<ImmutableList<QueryPartReplacer>> ddlStatementReplacements;

  /** Constructor for a {@link DdlExecutor} for the given connection. */
  DdlExecutor(
      BackendConnection backendConnection,
      Supplier<ImmutableList<QueryPartReplacer>> ddlStatementReplacements) {
    this.backendConnection = backendConnection;
    this.connection = backendConnection.getSpannerConnection();
    this.ddlStatementReplacements = ddlStatementReplacements;
  }

  /**
   * Removes any quotes around the given identifier, and folds the identifier to lower case if it
   * was not quoted.
   */
  static String unquoteIdentifier(String identifier) {
    if (identifier.length() < 2) {
      return identifier;
    }
    if (identifier.startsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier.toLowerCase();
  }

  /**
   * Executes the given DDL statement. This can also include some translations that are needed to
   * make the statement compatible with Cloud Spanner.
   */
  StatementResult execute(ParsedStatement parsedStatement, Statement statement) {
    if (!ddlStatementReplacements.get().isEmpty()) {
      String replaced = applyReplacers(statement.getSql());
      if (!replaced.equals(statement.getSql())) {
        statement = Statement.of(replaced);
        parsedStatement = AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
      }
    }
    return connection.execute(translate(parsedStatement, statement));
  }

  @VisibleForTesting
  String applyReplacers(String sql) {
    for (QueryPartReplacer functionReplacement : ddlStatementReplacements.get()) {
      Tuple<String, ReplacementStatus> result = functionReplacement.replace(sql);
      if (result.y() == ReplacementStatus.STOP) {
        return result.x();
      }
      sql = result.x();
    }
    return sql;
  }

  /**
   * Translates a DDL statement that can contain 'create table' statement with a named primary key
   * into one that is supported by the backend.
   */
  Statement translate(ParsedStatement parsedStatement, Statement statement) {
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eatKeyword("create")) {
      statement = translateCreate(parser, statement);
    }

    return statement;
  }

  Statement translateCreate(SimpleParser parser, Statement statement) {
    if (parser.eatKeyword("table")) {
      return maybeRemovePrimaryKeyConstraintName(statement);
    }
    return statement;
  }

  Statement maybeRemovePrimaryKeyConstraintName(Statement createTableStatement) {
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(createTableStatement);
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    parser.eatKeyword("create", "table", "if", "not", "exists");
    TableOrIndexName tableName = parser.readTableOrIndexName();
    if (tableName == null) {
      return createTableStatement;
    }
    if (!parser.eatToken("(")) {
      return createTableStatement;
    }
    while (true) {
      if (parser.eatToken(")")) {
        break;
      } else if (parser.eatToken(",")) {
        continue;
      } else if (parser.peekKeyword("constraint")) {
        int positionBeforeConstraintDefinition = parser.getPos();
        parser.eatKeyword("constraint");
        String constraintName = unquoteIdentifier(parser.readIdentifierPart());
        int positionAfterConstraintName = parser.getPos();
        if (parser.eatKeyword("primary", "key")) {
          if (!constraintName.equalsIgnoreCase("pk_" + unquoteIdentifier(tableName.name))) {
            return createTableStatement;
          }
          return Statement.of(
              parser.getSql().substring(0, positionBeforeConstraintDefinition)
                  + parser.getSql().substring(positionAfterConstraintName));
        } else {
          parser.parseExpression();
        }
      } else {
        parser.parseExpression();
      }
    }
    return createTableStatement;
  }
}
