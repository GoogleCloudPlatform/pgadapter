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

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.DB_STATEMENT;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.isCommand;
import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.COPY;

import com.google.api.core.InternalApi;
import com.google.cloud.Tuple;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.PostgreSQLStatementParser;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.commands.Command;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.ConnectionState;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FlushMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.List;

/**
 * Class that represents a simple query protocol statement. This statement can contain multiple
 * semi-colon separated SQL statements. The simple query protocol internally uses the extended query
 * protocol to execute the statement(s) in the SQL string, but does not return all the messages that
 * would have been returned by the extended protocol.
 */
@InternalApi
public class SimpleQueryStatement {
  private static final PostgreSQLStatementParser PARSER =
      (PostgreSQLStatementParser) AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  private final ConnectionHandler connectionHandler;
  private final OptionsMetadata options;
  private final ImmutableList<Statement> statements;

  public SimpleQueryStatement(
      OptionsMetadata options, Statement originalStatement, ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
    this.options = options;
    this.statements = parseStatements(originalStatement);
  }

  public void execute() throws Exception {
    // Do a Parse-Describe-Bind-Execute round-trip for each statement in the query string.
    // Finish with a Sync to close any implicit transaction and to return the results.
    for (Statement originalStatement : this.statements) {
      boolean isFirst = this.statements.get(0) == originalStatement;
      try {
        ParsedStatement parsedStatement = PARSER.parse(originalStatement);
        Statement statement = originalStatement;
        if (options.requiresMatcher()
            || connectionHandler.getWellKnownClient() == WellKnownClient.PSQL) {
          statement = translatePotentialMetadataCommand(statement, connectionHandler);
        }
        Tuple<Boolean, String> potentiallyReplacedStatement =
            replaceKnownUnsupportedQueries(
                this.connectionHandler.getWellKnownClient(), this.options, statement.getSql());
        if (potentiallyReplacedStatement.x()
            && !potentiallyReplacedStatement.y().equals(statement.getSql())) {
          // The original statement was replaced.
          statement = Statement.of(potentiallyReplacedStatement.y());
          parsedStatement = PARSER.parse(statement);
        }
        // We need to flush the entire pipeline if we encounter a COPY statement, as COPY statements
        // require additional messages to be sent back and forth, and this ensures that we get
        // everything in the correct order.
        boolean isCopy = isCommand(COPY, statement.getSql());
        if (!isFirst && isCopy) {
          new FlushMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
          if (connectionHandler
                  .getExtendedQueryProtocolHandler()
                  .getBackendConnection()
                  .getConnectionState()
              == ConnectionState.ABORTED) {
            break;
          }
        }
        new ParseMessage(connectionHandler, parsedStatement, statement).send();
        new BindMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
        new DescribeMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
        new ExecuteMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
      } catch (Exception ignore) {
        // Stop further processing if an exception occurs.
        break;
      }
    }
    new SyncMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
  }

  /** Replaces any known unsupported query (e.g. JDBC metadata queries). */
  static Tuple<Boolean, String> replaceKnownUnsupportedQueries(
      WellKnownClient client, OptionsMetadata options, String sql) {
    if ((options.isReplaceJdbcMetadataQueries() || client == WellKnownClient.JDBC)
        && JdbcMetadataStatementHelper.isPotentialJdbcMetadataStatement(sql)) {
      return Tuple.of(Boolean.TRUE, JdbcMetadataStatementHelper.replaceJdbcMetadataStatement(sql));
    }
    return Tuple.of(Boolean.FALSE, sql);
  }

  /**
   * Translate a Postgres Specific command into something Spanner can handle. Currently, this is
   * only concerned with PSQL specific meta-commands.
   *
   * @param parsedStatement The SQL statement to be translated.
   * @return The translated SQL statement if it matches any {@link Command} statement. Otherwise,
   *     returns the original Statement.
   */
  @VisibleForTesting
  static Statement translatePotentialMetadataCommand(
      Statement parsedStatement, ConnectionHandler connectionHandler) {
    Tracer tracer = connectionHandler.getExtendedQueryProtocolHandler().getTracer();
    Span span =
        tracer
            .spanBuilder("translatePotentialMetadataCommand")
            .setAttribute(
                "pgadapter.connection_id", connectionHandler.getTraceConnectionId().toString())
            .setAttribute(DB_STATEMENT, parsedStatement.getSql())
            .startSpan();
    try (Scope ignore = span.makeCurrent()) {
      for (Command currentCommand :
          Command.getCommands(
              parsedStatement.getSql(),
              connectionHandler.getSpannerConnection(),
              connectionHandler.getServer().getOptions().getCommandMetadataJSON())) {
        if (currentCommand.is()) {
          return Statement.of(currentCommand.translate());
        }
      }
      return parsedStatement;
    } finally {
      span.end();
    }
  }

  protected static ImmutableList<Statement> parseStatements(Statement statement) {
    Preconditions.checkNotNull(statement);
    ImmutableList.Builder<Statement> builder = ImmutableList.builder();
    SimpleParser parser = new SimpleParser(statement.getSql());
    for (String sql : parser.splitStatements()) {
      builder.add(Statement.of(sql));
    }
    return builder.build();
  }

  public List<Statement> getStatements() {
    return this.statements;
  }

  public String getStatement(int index) {
    return this.statements.get(index).getSql();
  }
}
