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

import com.google.cloud.spanner.AbstractLazyInitializer;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * {@link DdlExecutor} inspects DDL statements before executing these to support commonly used DDL
 * features that are not (yet) supported by the backend.
 */
class DdlExecutor {
  static class BackendSupportsIfExists extends AbstractLazyInitializer<Boolean> {
    private final DatabaseId databaseId;
    private final Connection connection;

    BackendSupportsIfExists(DatabaseId databaseId, Connection connection) {
      this.databaseId = databaseId;
      this.connection = connection;
    }

    @Override
    protected Boolean initialize() {
      Spanner spanner = ConnectionOptionsHelper.getSpanner(this.connection);
      DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
      try {
        adminClient
            .updateDatabaseDdl(
                this.databaseId.getInstanceId().getInstance(),
                this.databaseId.getDatabase(),
                ImmutableList.of(
                    "create table if not exists test (id bigint primary key)",
                    "create table invalid (id primary key)"),
                null)
            .get();
        // This statement should not be reachable, as at least one of the statements should fail.
        return Boolean.TRUE;
      } catch (ExecutionException exception) {
        SpannerException spannerException =
            SpannerExceptionFactory.asSpannerException(exception.getCause());
        if (spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT
            && spannerException
                .getMessage()
                .contains("<IF NOT EXISTS> clause is not supported in <CREATE TABLE> statement")) {
          return Boolean.FALSE;
        }
        return Boolean.TRUE;
      } catch (InterruptedException interruptedException) {
        throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
      }
    }
  }

  static final class NotExecuted implements StatementResult {
    @Override
    public ResultType getResultType() {
      return ResultType.NO_RESULT;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateCount() {
      throw new UnsupportedOperationException();
    }
  }

  private static final NotExecuted NOT_EXECUTED = new NotExecuted();
  private final AbstractLazyInitializer<Boolean> backendSupportsIfExists;
  private final BackendConnection backendConnection;
  private final Connection connection;

  /**
   * Constructor only intended for testing. This will create an executor that always assumes that
   * the backend does not support 'if [not] exists'.
   */
  @VisibleForTesting
  DdlExecutor(BackendConnection backendConnection) {
    this(
        new AbstractLazyInitializer<Boolean>() {
          @Override
          protected Boolean initialize() {
            return Boolean.FALSE;
          }
        },
        backendConnection);
  }

  DdlExecutor(DatabaseId databaseId, BackendConnection backendConnection) {
    this(
        new BackendSupportsIfExists(databaseId, backendConnection.getSpannerConnection()),
        backendConnection);
  }

  private DdlExecutor(
      AbstractLazyInitializer<Boolean> backendSupportsIfExists,
      BackendConnection backendConnection) {
    this.backendConnection = backendConnection;
    this.connection = backendConnection.getSpannerConnection();
    this.backendSupportsIfExists = backendSupportsIfExists;
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
    return NOT_EXECUTED;
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
    return maybeExecuteStatement(
        parser, statement, "create", type, name -> !existsFunction.apply(name));
  }

  private Statement translateDropTableOrIndex(
      SimpleParser parser,
      Statement statement,
      String type,
      Function<String, Boolean> existsFunction) {
    if (!parser.eat("if", "exists")) {
      return statement;
    }
    return maybeExecuteStatement(parser, statement, "drop", type, existsFunction);
  }

  Statement maybeExecuteStatement(
      SimpleParser parser,
      Statement statement,
      String command,
      String type,
      Function<String, Boolean> shouldExecuteFunction) {
    if (backendSupportsIfExists()) {
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
    if (!shouldExecuteFunction.apply(name)) {
      // Return null to indicate that the statement should be skipped.
      return null;
    }

    // Return the DDL statement without the 'if [not] exists' clause.
    return Statement.of(command + " " + type + parser.getSql().substring(identifierPosition));
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

  boolean backendSupportsIfExists() {
    try {
      return backendSupportsIfExists.get();
    } catch (Exception exception) {
      throw SpannerExceptionFactory.asSpannerException(exception);
    }
  }
}
