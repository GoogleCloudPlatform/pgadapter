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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.PostgreSQLStatementParser;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.commands.Command;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.ManuallyCreatedToken;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
  private final ImmutableList<ParsedStatement> statements;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';
  private static final char DOUBLE_QUOTE = '"';

  public SimpleQueryStatement(
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
    this.options = options;
    this.statements = parseStatements(parsedStatement);
  }

  public void execute() throws Exception {
    // Do a Parse-Describe-Bind-Execute round-trip for each statement in the query string.
    // Finish with a Sync to close any implicit transaction and to return the results.
    for (ParsedStatement statement : this.statements) {
      if (options.requiresMatcher()) {
        statement = translatePotentialMetadataCommand(statement, connectionHandler);
      }
      statement = replaceKnownUnsupportedQueries(this.options, statement);
      new ParseMessage(connectionHandler, statement).send();
      new BindMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
      new DescribeMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
      new ExecuteMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
    }
    new SyncMessage(connectionHandler, ManuallyCreatedToken.MANUALLY_CREATED_TOKEN).send();
  }

  /** Replaces any known unsupported query (e.g. JDBC metadata queries). */
  static ParsedStatement replaceKnownUnsupportedQueries(
      OptionsMetadata options, ParsedStatement parsedStatement) {
    if (options.isReplaceJdbcMetadataQueries()
        && JdbcMetadataStatementHelper.isPotentialJdbcMetadataStatement(
            parsedStatement.getSqlWithoutComments())) {
      return PARSER.parse(
          Statement.of(
              JdbcMetadataStatementHelper.replaceJdbcMetadataStatement(
                  parsedStatement.getSqlWithoutComments())));
    }
    return parsedStatement;
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
  static ParsedStatement translatePotentialMetadataCommand(
      ParsedStatement parsedStatement, ConnectionHandler connectionHandler) {
    for (Command currentCommand :
        Command.getCommands(
            parsedStatement.getSqlWithoutComments(),
            connectionHandler.getSpannerConnection(),
            connectionHandler.getServer().getOptions().getCommandMetadataJSON())) {
      if (currentCommand.is()) {
        return PARSER.parse(Statement.of(currentCommand.translate()));
      }
    }
    return parsedStatement;
  }

  static ImmutableList<String> splitStatements(String sql) {
    // First check trivial cases with only one statement.
    int firstIndexOfDelimiter = sql.indexOf(STATEMENT_DELIMITER);
    if (firstIndexOfDelimiter == -1) {
      return ImmutableList.of(sql);
    }
    if (firstIndexOfDelimiter == sql.length() - 1) {
      return ImmutableList.of(sql.substring(0, sql.length() - 1));
    }

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    // TODO: Fix this parsing, as it does not take all types of quotes into consideration.
    boolean insideQuotes = false;
    char quoteChar = 0;
    int index = 0;
    for (int i = 0; i < sql.length(); ++i) {
      if (insideQuotes) {
        if (sql.charAt(i) == quoteChar) {
          if (sql.length() > (i + 1) && sql.charAt(i + 1) == quoteChar) {
            // This is an escaped quote. Skip one ahead.
            i++;
          } else {
            insideQuotes = false;
          }
        }
      } else {
        if (sql.charAt(i) == SINGLE_QUOTE || sql.charAt(i) == DOUBLE_QUOTE) {
          quoteChar = sql.charAt(i);
          insideQuotes = true;
        } else {
          // skip semicolon inside quotes
          if (sql.charAt(i) == STATEMENT_DELIMITER) {
            String stmt = sql.substring(index, i).trim();
            // Statements with only ';' character are empty and dropped.
            if (stmt.length() > 0) {
              builder.add(stmt);
            }
            index = i + 1;
          }
        }
      }
    }

    if (index < sql.length()) {
      builder.add(sql.substring(index).trim());
    }
    return builder.build();
  }

  protected static ImmutableList<ParsedStatement> parseStatements(ParsedStatement stmt) {
    String sql = stmt.getSqlWithoutComments();
    Preconditions.checkNotNull(sql);
    ImmutableList.Builder<ParsedStatement> builder = ImmutableList.builder();
    for (String statement : splitStatements(sql)) {
      builder.add(PARSER.parse(Statement.of(statement)));
    }
    return builder.build();
  }

  public List<ParsedStatement> getStatements() {
    return this.statements;
  }

  public String getStatement(int index) {
    return this.statements.get(index).getSqlWithoutComments();
  }
}
