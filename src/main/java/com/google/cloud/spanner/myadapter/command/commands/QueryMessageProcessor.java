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

package com.google.cloud.spanner.myadapter.command.commands;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.connection.SpannerStatementParser;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.statements.SessionStatementParser;
import com.google.cloud.spanner.myadapter.statements.SessionStatementParser.SessionStatement;
import com.google.cloud.spanner.myadapter.statements.SimpleParser;
import com.google.cloud.spanner.myadapter.translator.QueryTranslator;
import com.google.cloud.spanner.myadapter.translator.models.QueryAction;
import com.google.cloud.spanner.myadapter.translator.models.QueryReplacement;
import com.google.cloud.spanner.myadapter.utils.Converter;
import com.google.cloud.spanner.myadapter.wireinput.QueryMessage;
import com.google.cloud.spanner.myadapter.wireinput.WireMessage;
import com.google.cloud.spanner.myadapter.wireoutput.ColumnCountResponse;
import com.google.cloud.spanner.myadapter.wireoutput.ColumnDefinitionResponse;
import com.google.cloud.spanner.myadapter.wireoutput.EofResponse;
import com.google.cloud.spanner.myadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.myadapter.wireoutput.OkResponse;
import com.google.cloud.spanner.myadapter.wireoutput.RowResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryMessageProcessor extends MessageProcessor {

  private static final SpannerStatementParser PARSER =
      (SpannerStatementParser) AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL);

  private static final Logger logger = Logger.getLogger(QueryMessageProcessor.class.getName());

  private int currentSequenceNumber = -1;
  private final BackendConnection backendConnection;
  private final QueryTranslator queryTranslator;

  public QueryMessageProcessor(
      ConnectionMetadata connectionMetadata,
      SessionState sessionState,
      BackendConnection backendConnection,
      OptionsMetadata optionsMetadata) {
    super(connectionMetadata, sessionState);
    this.backendConnection = backendConnection;
    this.queryTranslator = new QueryTranslator(optionsMetadata);
  }

  @Override
  public void processMessage(WireMessage message) throws Exception {
    QueryMessage queryMessage = (QueryMessage) message;
    ImmutableList<Statement> statements = parseStatements(queryMessage.getOriginalStatement());
    currentSequenceNumber = queryMessage.getMessageSequenceNumber();

    for (Statement originalStatement : statements) {
      final Statement statement = originalStatement;
      logger.log(
          Level.INFO, () -> String.format("SQL query being processed: %s.", statement.getSql()));

      ParsedStatement parsedStatement = PARSER.parse(originalStatement);
      QueryReplacement queryReplacement =
          queryTranslator.translatedQuery(parsedStatement, originalStatement);
      if (queryReplacement.getAction() == QueryAction.RETURN_OK) {
        currentSequenceNumber =
            new OkResponse(currentSequenceNumber, connectionMetadata).send(true);
        continue;
      }
      parsedStatement = PARSER.parse(queryReplacement.getOutputQuery());
      try {
        StatementResult statementResult;
        SessionStatement sessionStatement = SessionStatementParser.parse(parsedStatement);
        if (sessionStatement != null) {
          statementResult =
              backendConnection.executeSessionStatement(sessionStatement, sessionState);
        } else {
          statementResult =
              backendConnection.executeQuery(
                  queryReplacement.getOutputQuery(), parsedStatement, sessionState);
        }

        switch (statementResult.getResultType()) {
          case RESULT_SET:
            processResultSet(statementResult.getResultSet(), queryReplacement);
            break;
          case UPDATE_COUNT:
            new OkResponse(
                    currentSequenceNumber, connectionMetadata, statementResult.getUpdateCount())
                .send(true);
            break;
          case NO_RESULT:
            new OkResponse(currentSequenceNumber, connectionMetadata).send(true);
            break;
        }

      } catch (Exception e) {
        logger.log(Level.WARNING, e, () -> "Query execution error.");
        new ErrorResponse(currentSequenceNumber, connectionMetadata, e.getMessage(), 1064)
            .send(true);
        // Stop further processing if an exception occurs.
        break;
      }
    }
  }

  private void processResultSet(ResultSet resultSet, QueryReplacement queryReplacement)
      throws Exception {
    int rowsSent = 0;
    // ResultSet cannot be accessed for pre-populated result sets without calling .next() at least
    // once. We create pre-populated result sets for things like system variable queries. So we must
    // call sendColumnDefinitions() only after calling resultSet.next() initially.
    while (resultSet.next()) {
      if (rowsSent < 1) {
        sendColumnDefinitions(resultSet, queryReplacement);
      }
      sendResultSetRow(resultSet, queryReplacement);
      rowsSent++;
    }

    // We must send column definitions back even if result set didn't have any rows as some clients
    // like hibernate expect it.
    if (rowsSent < 1) {
      sendColumnDefinitions(resultSet, queryReplacement);
    }
    currentSequenceNumber = new EofResponse(currentSequenceNumber, connectionMetadata).send(true);
  }

  private void sendResultSetRow(ResultSet resultSet, QueryReplacement queryReplacement)
      throws Exception {
    currentSequenceNumber =
        new RowResponse(currentSequenceNumber, connectionMetadata, resultSet).send();
  }

  private void sendColumnDefinitions(ResultSet resultSet, QueryReplacement queryReplacement)
      throws IOException {
    currentSequenceNumber =
        new ColumnCountResponse(
                currentSequenceNumber, connectionMetadata, resultSet.getColumnCount())
            .send();
    for (int i = 0; i < resultSet.getColumnCount(); ++i) {
      ColumnDefinitionResponse.Builder builder =
          new ColumnDefinitionResponse.Builder(currentSequenceNumber, connectionMetadata);
      // TODO : Assess how does fields like schema, table, originalTable affects the client, and
      // properly populate them.
      resultSet.getType().getStructFields().get(i).getType().getCode();
      currentSequenceNumber =
          builder
              .schema("schemaName")
              .table("tableName")
              .originalTable("oTableName")
              .column(
                  queryReplacement.overrideColumn(
                      resultSet.getType().getStructFields().get(i).getName()))
              .originalColumn("originalColumnName")
              .charset(
                  resultSet.getColumnType(i).getCode() == Code.BYTES
                      ? CHARSET_BINARY
                      : CHARSET_UTF8_MB4)
              .maxColumnLength(20)
              .columnType(
                  Converter.convertToMySqlCode(
                      resultSet.getType().getStructFields().get(i).getType().getCode()))
              .columnDefinitionFlags(0)
              .decimals(0)
              .build()
              .send();
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
}
