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
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer.ReplacementStatus;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    Statement translated = translate(parsedStatement, statement);
    if (!backendConnection.getSessionState().isSupportDropCascade()) {
      return connection.execute(translated);
    } else {
      ImmutableList<Statement> allStatements = getDependentStatements(translated);
      if (allStatements.size() == 1) {
        return connection.execute(allStatements.get(0));
      } else {
        boolean startedBatch = false;
        try {
          if (!connection.isDdlBatchActive()) {
            connection.execute(Statement.of("START BATCH DDL"));
            startedBatch = true;
          }
          StatementResult result = null;
          for (Statement dropDependency : allStatements) {
            result = connection.execute(dropDependency);
          }
          if (startedBatch) {
            result = connection.execute(Statement.of("RUN BATCH"));
          }
          return result;
        } catch (Throwable t) {
          if (startedBatch && connection.isDdlBatchActive()) {
            connection.abortBatch();
          }
          throw t;
        }
      }
    }
  }

  private String applyReplacers(String sql) {
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

  ImmutableList<Statement> getDependentStatements(Statement statement) {
    ImmutableList<Statement> defaultResult = ImmutableList.of(statement);
    SimpleParser parser = new SimpleParser(statement.getSql());
    if (!parser.eatKeyword("drop")) {
      return defaultResult;
    }
    parser.eatKeyword("if", "exists");
    if (parser.eatKeyword("table")) {
      TableOrIndexName tableName = parser.readTableOrIndexName();
      if (tableName == null) {
        return defaultResult;
      }
      boolean cascade = false;
      String sqlWithoutCascade = parser.getSql().substring(0, parser.getPos());
      if (parser.eatKeyword("cascade")) {
        cascade = true;
      } else {
        // This is the default, so no need to register it specifically.
        parser.eatKeyword("restrict");
      }
      if (parser.hasMoreTokens()) {
        return defaultResult;
      }
      ImmutableList.Builder<Statement> builder = ImmutableList.builder();
      builder.addAll(getDropDependentIndexesStatements(tableName));
      if (cascade) {
        // We don't have any way to get the views that depend on this table.
        builder.addAll(getDropDependentForeignKeyConstraintsStatements(tableName));
      }
      builder.add(cascade ? Statement.of(sqlWithoutCascade) : statement);
      return builder.build();
    } else if (parser.eatKeyword("schema")) {
      TableOrIndexName schemaName = parser.readTableOrIndexName();
      if (schemaName == null || schemaName.schema != null) {
        return defaultResult;
      }
      if (!parser.eatKeyword("cascade")) {
        return defaultResult;
      }
      ImmutableList.Builder<Statement> builder = ImmutableList.builder();
      builder.addAll(getDropSchemaIndexesStatements(schemaName));
      builder.addAll(getDropSchemaForeignKeysStatements(schemaName));
      builder.addAll(getDropSchemaViewsStatements(schemaName));
      builder.addAll(getDropSchemaTablesStatements(schemaName));
      return builder.build();
    }
    return defaultResult;
  }

  ImmutableList<Statement> getDropDependentIndexesStatements(TableOrIndexName tableName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, index_name "
                            + "from information_schema.indexes "
                            + "where table_schema=$1 and table_name=$2 "
                            + "and not index_type = 'PRIMARY_KEY' "
                            + "and spanner_is_managed = 'NO'")
                    .bind("p1")
                    .to(
                        unquoteIdentifier(
                            tableName.schema == null
                                ? backendConnection.getCurrentSchema()
                                : tableName.schema))
                    .bind("p2")
                    .to(unquoteIdentifier(tableName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "drop index \"%s\".\"%s\"",
                    resultSet.getString("table_schema"), resultSet.getString("index_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropDependentForeignKeyConstraintsStatements(
      TableOrIndexName tableName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select child.table_schema, child.table_name, rc.constraint_name\n"
                            + "from information_schema.referential_constraints rc\n"
                            + "inner join information_schema.table_constraints parent\n"
                            + "  on  rc.unique_constraint_catalog=parent.constraint_catalog\n"
                            + "  and rc.unique_constraint_schema=parent.constraint_schema\n"
                            + "  and rc.unique_constraint_name=parent.constraint_name\n"
                            + "inner join information_schema.table_constraints child\n"
                            + "  on  rc.constraint_catalog=child.constraint_catalog\n"
                            + "  and rc.constraint_schema=child.constraint_schema\n"
                            + "  and rc.constraint_name=child.constraint_name\n"
                            + "where parent.table_schema=$1\n"
                            + "and parent.table_name=$2")
                    .bind("p1")
                    .to(
                        unquoteIdentifier(
                            tableName.schema == null
                                ? backendConnection.getCurrentSchema()
                                : tableName.schema))
                    .bind("p2")
                    .to(unquoteIdentifier(tableName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "alter table \"%s\".\"%s\" drop constraint \"%s\"",
                    resultSet.getString("table_schema"),
                    resultSet.getString("table_name"),
                    resultSet.getString("constraint_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropSchemaForeignKeysStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select child.table_schema, child.table_name, rc.constraint_name\n"
                            + "from information_schema.referential_constraints rc\n"
                            + "inner join information_schema.table_constraints parent\n"
                            + "  on  rc.unique_constraint_catalog=parent.constraint_catalog\n"
                            + "  and rc.unique_constraint_schema=parent.constraint_schema\n"
                            + "  and rc.unique_constraint_name=parent.constraint_name\n"
                            + "inner join information_schema.table_constraints child\n"
                            + "  on  rc.constraint_catalog=child.constraint_catalog\n"
                            + "  and rc.constraint_schema=child.constraint_schema\n"
                            + "  and rc.constraint_name=child.constraint_name\n"
                            + "where parent.table_schema=$1")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "alter table \"%s\".\"%s\" drop constraint \"%s\"",
                    resultSet.getString("table_schema"),
                    resultSet.getString("table_name"),
                    resultSet.getString("constraint_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropSchemaIndexesStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, index_name "
                            + "from information_schema.indexes "
                            + "where table_schema=$1 "
                            + "and not index_type = 'PRIMARY_KEY' "
                            + "and spanner_is_managed = 'NO'")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "drop index \"%s\".\"%s\"",
                    resultSet.getString("table_schema"), resultSet.getString("index_name"))));
      }
      return dropStatements.build();
    }
  }

  private static final class Table implements Comparable<Table> {
    private final String schema;
    private final String name;
    private final String parent;

    Table(String schema, String name, String parent) {
      this.schema = Preconditions.checkNotNull(schema);
      this.name = Preconditions.checkNotNull(name);
      this.parent = parent;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.schema, this.name);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Table)) {
        return false;
      }
      Table other = (Table) o;
      return Objects.equals(this.schema, other.schema) && Objects.equals(this.name, other.name);
    }

    @Override
    public int compareTo(Table o) {
      if (o.parent != null && Objects.equals(this.name, o.parent)) {
        return 1;
      }
      if (this.parent != null && Objects.equals(this.parent, o.name)) {
        return -1;
      }
      if (o.parent != null && this.parent == null) {
        return 1;
      }
      if (this.parent != null && o.parent == null) {
        return -1;
      }
      if (!Objects.equals(this.schema, o.schema)) {
        return this.schema.compareTo(o.schema);
      }
      return this.name.compareTo(o.name);
    }
  }

  ImmutableList<Statement> getDropSchemaViewsStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, table_name "
                            + "from information_schema.tables "
                            + "where table_schema=$1 "
                            + "and table_type='VIEW'")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      while (resultSet.next()) {
        dropStatements.add(
            Statement.of(
                String.format(
                    "drop view \"%s\".\"%s\"",
                    resultSet.getString("table_schema"), resultSet.getString("table_name"))));
      }
      return dropStatements.build();
    }
  }

  ImmutableList<Statement> getDropSchemaTablesStatements(TableOrIndexName schemaName) {
    DatabaseClient client = connection.getDatabaseClient();
    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "select table_schema, table_name, parent_table_name "
                            + "from information_schema.tables "
                            + "where table_schema=$1 "
                            + "and table_type='BASE TABLE'")
                    .bind("p1")
                    .to(unquoteIdentifier(schemaName.name))
                    .build())) {
      List<Table> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(
            new Table(
                resultSet.getString("table_schema"),
                resultSet.getString("table_name"),
                resultSet.isNull("parent_table_name")
                    ? null
                    : resultSet.getString("parent_table_name")));
      }
      Collections.sort(tables);
      ImmutableList.Builder<Statement> dropStatements = ImmutableList.builder();
      for (Table table : tables) {
        dropStatements.add(
            Statement.of(String.format("drop table \"%s\".\"%s\"", table.schema, table.name)));
      }
      return dropStatements.build();
    }
  }
}
