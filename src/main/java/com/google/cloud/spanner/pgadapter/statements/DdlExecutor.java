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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import java.util.function.Function;

/**
 * {@link DdlExecutor} inspects DDL statements before executing these to support commonly used DDL
 * features that are not (yet) supported by the backend.
 */
class DdlExecutor {
  private static final NoResult NO_RESULT = new NoResult();
  private final BackendConnection backendConnection;
  private final Connection connection;

  DdlExecutor(BackendConnection backendConnection) {
    this.backendConnection = backendConnection;
    this.connection = backendConnection.getSpannerConnection();
  }

  static String unquoteIdentifier(String identifier) {
    if (identifier.length() < 2) {
      return identifier;
    }
    if (identifier.startsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier.toLowerCase();
  }

  StatementResult execute(ParsedStatement parsedStatement, Statement statement) {
    Statement translated = translate(parsedStatement, statement);
    if (translated != null) {
      return connection.execute(translated);
    }
    return NO_RESULT;
  }

  Statement translate(ParsedStatement parsedStatement, Statement statement) {
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eat("create")) {
      statement = translateCreate(parser, statement);
    } else if (parser.eat("drop")) {
      statement = translateDrop(parser, statement);
    }

    return statement;
  }

  Statement translateCreate(SimpleParser parser, Statement statement) {
    if (parser.eat("table")) {
      return translateCreateTable(parser, statement);
    }
    boolean unique = parser.eat("unique");
    if (parser.eat("index")) {
      return translateCreateIndex(parser, statement, unique);
    }
    return statement;
  }

  Statement translateDrop(SimpleParser parser, Statement statement) {
    if (parser.eat("table")) {
      return translateDropTable(parser, statement);
    }
    if (parser.eat("index")) {
      return translateDropIndex(parser, statement);
    }
    return statement;
  }

  Statement translateCreateTable(SimpleParser parser, Statement statement) {
    return translateCreateTableOrIndex(parser, statement, "table", this::tableExists);
  }

  Statement translateCreateIndex(SimpleParser parser, Statement statement, boolean unique) {
    return translateCreateTableOrIndex(
        parser, statement, unique ? "unique index" : "index", this::indexExists);
  }

  Statement translateDropTable(SimpleParser parser, Statement statement) {
    return translateDropTableOrIndex(parser, statement, "table", this::tableExists);
  }

  Statement translateDropIndex(SimpleParser parser, Statement statement) {
    return translateDropTableOrIndex(parser, statement, "index", this::indexExists);
  }

  private Statement translateCreateTableOrIndex(
      SimpleParser parser,
      Statement statement,
      String type,
      Function<String, Boolean> existsFunction) {
    if (!parser.eat("if", "not", "exists")) {
      return statement;
    }
    int identifierPosition = parser.getPos();
    if (identifierPosition >= parser.getSql().length()) {
      return statement;
    }
    String name = parser.readIdentifier();
    if (name == null) {
      return statement;
    }
    // Check if the table exists or not.
    if (existsFunction.apply(name)) {
      // Return null to indicate that the statement should be skipped.
      return null;
    }

    // Return the DDL statement without the 'if not exists' clause.
    return Statement.of("create " + type + parser.getSql().substring(identifierPosition));
  }

  private Statement translateDropTableOrIndex(
      SimpleParser parser,
      Statement statement,
      String type,
      Function<String, Boolean> existsFunction) {
    if (!parser.eat("if", "exists")) {
      return statement;
    }
    int identifierPosition = parser.getPos();
    if (identifierPosition >= parser.getSql().length()) {
      return statement;
    }
    String name = parser.readIdentifier();
    if (name == null) {
      return statement;
    }
    // Check if the table exists or not.
    if (!existsFunction.apply(name)) {
      // Return null to indicate that the statement should be skipped.
      return null;
    }

    // Return the DDL statement without the 'if exists' clause.
    return Statement.of("drop " + type + parser.getSql().substring(identifierPosition));
  }

  boolean tableExists(String name) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select 1 from information_schema.tables where table_schema=$1 and table_name=$2")
                    .bind("p1")
                    .to(backendConnection.getCurrentSchema())
                    .bind("p2")
                    .to(unquoteIdentifier(name))
                    .build())) {
      return resultSet.next();
    }
  }

  boolean indexExists(String name) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select 1 from information_schema.indexes where table_schema=$1 and index_name=$2")
                    .bind("p1")
                    .to(backendConnection.getCurrentSchema())
                    .bind("p2")
                    .to(unquoteIdentifier(name))
                    .build())) {
      return resultSet.next();
    }
  }
}
