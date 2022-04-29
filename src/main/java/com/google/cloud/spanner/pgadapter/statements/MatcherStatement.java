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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.commands.Command;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;

/**
 * Meant to be utilized when running as a proxy for any interactive terminal tool either PSQL or
 * custom versions. Not meant for production runs. Generally here, all statements will take some
 * penalty by running matchers to determine whether they belong to a specific meta-command. If
 * matches, translates the command into something Spanner can handle.
 */
@InternalApi
public class MatcherStatement extends IntermediateStatement {

  public MatcherStatement(
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      ConnectionHandler connectionHandler) {
    super(options, translateSQL(parsedStatement, connectionHandler), connectionHandler);
  }

  @Override
  public void execute() {
    super.execute();
  }

  /**
   * Translate a Postgres Specific command into something Spanner can handle. Currently, this is
   * only concerned with PSQL specific meta-commands.
   *
   * @param parsedStatement The SQL statement to be translated.
   * @return The translated SQL statement if it matches any {@link Command} statement. Otherwise
   *     gives out the original Statement.
   */
  private static ParsedStatement translateSQL(
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
}
