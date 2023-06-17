// Copyright 2023 Google LLC
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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * SHOW DATABASE DDL [FOR POSTGRESQL] returns a result set with the DDL statements that are needed
 * to (re-)create the schema of the current database. The FOR POSTGRESQL clause will instruct
 * PGAdapter to only return DDL statements that are compatible with PostgreSQL. Cloud Spanner
 * specific clauses will be commented out in the returned DDL.
 */
public class ShowDatabaseDdlStatement extends IntermediatePortalStatement {
  static class ParsedShowDatabaseDdlStatement {
    final boolean forPostgreSQL;

    ParsedShowDatabaseDdlStatement(boolean forPostgreSQL) {
      this.forPostgreSQL = forPostgreSQL;
    }
  }

  private final ParsedShowDatabaseDdlStatement parsedShowDatabaseDdlStatement;

  public ShowDatabaseDdlStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(
        name,
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            name,
            NO_PARAMETER_TYPES,
            parsedStatement,
            originalStatement),
        NO_PARAMS,
        ImmutableList.of(),
        ImmutableList.of());
    this.parsedShowDatabaseDdlStatement = parse(originalStatement.getSql());
  }

  static ParsedShowDatabaseDdlStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    // SHOW DATABASE DDL [FOR POSTGRESQL]
    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("show", "database", "ddl")) {
      throw PGExceptionFactory.newPGException("not a valid SHOW DATABASE DDL statement: " + sql);
    }
    boolean forPostgres = false;
    if (parser.eatKeyword("for")) {
      if (!parser.hasMoreTokens()) {
        throw PGExceptionFactory.newPGException("missing 'POSTGRESQL' keyword: " + sql);
      }
      forPostgres = parser.eatKeyword("postgresql");
    }
    parser.throwIfHasMoreTokens();

    return new ParsedShowDatabaseDdlStatement(forPostgres);
  }

  @Override
  public String getCommandTag() {
    return "SHOW";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.CLIENT_SIDE;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    setFutureStatementResult(Futures.immediateFuture(execute(backendConnection)));
  }

  private StatementResult execute(BackendConnection backendConnection) {
    Connection connection = backendConnection.getSpannerConnection();
    Spanner spanner = ConnectionOptionsHelper.getSpanner(connection);
    ResultSet resultSet =
        ClientSideResultSet.forRows(
            Type.struct(StructField.of("Statement", Type.string())),
            getDatabaseDdl(spanner, connectionHandler.getDatabaseId()).stream()
                .map(
                    statement ->
                        Struct.newBuilder()
                            .set("Statement")
                            .to(
                                (parsedShowDatabaseDdlStatement.forPostgreSQL
                                        ? DdlTranslator.translate(statement)
                                        : statement)
                                    + ";")
                            .build())
                .collect(Collectors.toList()));
    return new QueryResult(resultSet);
  }

  private List<String> getDatabaseDdl(Spanner spanner, DatabaseId databaseId) {
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    return databaseAdminClient.getDatabaseDdl(
        databaseId.getInstanceId().getInstance(), databaseId.getDatabase());
  }

  @Override
  public Future<StatementResult> describeAsync(BackendConnection backendConnection) {
    ResultSet resultSet =
        ClientSideResultSet.forRows(
            Type.struct(StructField.of("Statement", Type.string())), ImmutableList.of());
    return Futures.immediateFuture(new QueryResult(resultSet));
  }

  @Override
  public IntermediatePortalStatement createPortal(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // SHOW DATABASE DDL does not support binding any parameters, so we just return the same
    // statement.
    return this;
  }
}
