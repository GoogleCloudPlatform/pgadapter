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
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.Format;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.ParsedCopyStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyDoneResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyOutResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.WireOutput;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

/**
 * {@link CopyStatement} models a `COPY table TO STDOUT` statement. The same class is used both as
 * an {@link IntermediatePreparedStatement} and {@link IntermediatePortalStatement}, as COPY does
 * not support any statement parameters, which means that there is no difference between the two.
 */
@InternalApi
public class CopyToStatement extends IntermediatePortalStatement {
  public static final byte[] COPY_BINARY_HEADER =
      new byte[] {'P', 'G', 'C', 'O', 'P', 'Y', '\n', -1, '\r', '\n', '\0'};

  private final ParsedCopyStatement parsedCopyStatement;
  private final CSVFormat csvFormat;

  public CopyToStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedCopyStatement parsedCopyStatement) {
    super(
        connectionHandler,
        options,
        name,
        createParsedStatement(parsedCopyStatement),
        createSelectStatement(parsedCopyStatement));
    this.parsedCopyStatement = parsedCopyStatement;
    if (parsedCopyStatement.format == CopyStatement.Format.BINARY) {
      this.csvFormat = null;
    } else {
      CSVFormat.Builder formatBuilder =
          CSVFormat.Builder.create(CSVFormat.POSTGRESQL_TEXT)
              .setNullString(
                  parsedCopyStatement.nullString == null
                      ? CSVFormat.POSTGRESQL_TEXT.getNullString()
                      : parsedCopyStatement.nullString)
              .setRecordSeparator('\n')
              .setDelimiter(
                  parsedCopyStatement.delimiter == null
                      ? CSVFormat.POSTGRESQL_TEXT.getDelimiterString().charAt(0)
                      : parsedCopyStatement.delimiter)
              .setQuote(
                  parsedCopyStatement.quote == null
                      ? CSVFormat.POSTGRESQL_TEXT.getQuoteCharacter()
                      : parsedCopyStatement.quote)
              .setEscape(
                  parsedCopyStatement.escape == null
                      ? CSVFormat.POSTGRESQL_TEXT.getEscapeCharacter()
                      : parsedCopyStatement.escape);
      if (parsedCopyStatement.format == Format.TEXT) {
        formatBuilder.setQuoteMode(QuoteMode.NONE);
      } else {
        if (parsedCopyStatement.forceQuote == null) {
          formatBuilder.setQuoteMode(QuoteMode.MINIMAL);
        } else if (parsedCopyStatement.forceQuote.isEmpty()) {
          formatBuilder.setQuoteMode(QuoteMode.ALL_NON_NULL);
        } else {
          // The CSV parser does not support different quote modes per column.
          throw PGExceptionFactory.newPGException(
              "PGAdapter does not support force_quote modes per column", SQLState.InternalError);
        }
      }
      if (parsedCopyStatement.header) {
        if (parsedCopyStatement.columns == null) {
          formatBuilder.setHeader(
              retrieveHeader(
                  connectionHandler
                      .getExtendedQueryProtocolHandler()
                      .getBackendConnection()
                      .getSpannerConnection(),
                  parsedCopyStatement));
        } else {
          formatBuilder.setHeader(
              parsedCopyStatement.columns.stream()
                  .map(TableOrIndexName::getUnquotedName)
                  .toArray(String[]::new));
        }
      }
      this.csvFormat = formatBuilder.build();
    }
  }

  static ParsedStatement createParsedStatement(ParsedCopyStatement parsedCopyStatement) {
    return PARSER.parse(createSelectStatement(parsedCopyStatement));
  }

  static Statement createSelectStatement(ParsedCopyStatement parsedCopyStatement) {
    if (parsedCopyStatement.query != null) {
      return Statement.of(parsedCopyStatement.query);
    }
    if (parsedCopyStatement.columns != null) {
      return Statement.of(
          String.format(
              "select %s from %s",
              parsedCopyStatement.columns.stream()
                  .map(TableOrIndexName::toString)
                  .collect(Collectors.joining(", ")),
              parsedCopyStatement.table));
    }
    return Statement.of("select * from " + parsedCopyStatement.table);
  }

  static String[] retrieveHeader(Connection connection, ParsedCopyStatement parsedCopyStatement) {
    try (ResultSet resultSet =
        connection
            .getDatabaseClient()
            .singleUse()
            .analyzeQuery(createSelectStatement(parsedCopyStatement), QueryAnalyzeMode.PLAN)) {
      resultSet.next();
      return convertColumnNamesToStringArray(resultSet);
    }
  }

  static String[] convertColumnNamesToStringArray(ResultSet resultSet) {
    String[] result = new String[resultSet.getColumnCount()];
    for (int index = 0; index < resultSet.getColumnCount(); index++) {
      result[index] = resultSet.getType().getStructFields().get(index).getName();
    }
    return result;
  }

  @VisibleForTesting
  CSVFormat getCsvFormat() {
    return csvFormat;
  }

  public boolean isBinary() {
    return parsedCopyStatement.format == CopyStatement.Format.BINARY;
  }

  @Override
  public String getCommandTag() {
    return "COPY";
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.QUERY;
  }

  @Override
  public boolean containsResultSet() {
    return true;
  }

  @Override
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    setFutureStatementResult(backendConnection.executeCopyOut(parsedStatement, statement));
  }

  @Override
  public Future<DescribePortalMetadata> describeAsync(BackendConnection backendConnection) {
    // Return null to indicate that this COPY TO STDOUT statement does not return any
    // RowDescriptionResponse.
    return Futures.immediateFuture(null);
  }

  @Override
  public IntermediatePortalStatement bind(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // COPY does not support binding any parameters, so we just return the same statement.
    return this;
  }

  @Override
  public WireOutput[] createResultPrefix(ResultSet resultSet) {
    return this.parsedCopyStatement.format == CopyStatement.Format.BINARY
        ? new WireOutput[] {
          new CopyOutResponse(
              this.outputStream,
              resultSet.getColumnCount(),
              DataFormat.POSTGRESQL_BINARY.getCode()),
          CopyDataResponse.createBinaryHeader(this.outputStream)
        }
        : new WireOutput[] {
          new CopyOutResponse(
              this.outputStream, resultSet.getColumnCount(), DataFormat.POSTGRESQL_TEXT.getCode())
        };
  }

  @Override
  public CopyDataResponse createDataRowResponse(Converter converter) {
    return parsedCopyStatement.format == CopyStatement.Format.BINARY
        ? createBinaryDataResponse(converter)
        : createDataResponse(converter.getResultSet());
  }

  @Override
  public WireOutput[] createResultSuffix() {
    return this.parsedCopyStatement.format == Format.BINARY
        ? new WireOutput[] {
          CopyDataResponse.createBinaryTrailer(this.outputStream),
          new CopyDoneResponse(this.outputStream)
        }
        : new WireOutput[] {new CopyDoneResponse(this.outputStream)};
  }

  CopyDataResponse createDataResponse(ResultSet resultSet) {
    String[] data = new String[resultSet.getColumnCount()];
    for (int col = 0; col < resultSet.getColumnCount(); col++) {
      if (resultSet.isNull(col)) {
        data[col] = null;
      } else {
        Parser<?> parser = Parser.create(resultSet, resultSet.getColumnType(col), col);
        data[col] = parser.stringParse();
      }
    }
    String row = csvFormat.format((Object[]) data);
    return new CopyDataResponse(this.outputStream, row, csvFormat.getRecordSeparator().charAt(0));
  }

  CopyDataResponse createBinaryDataResponse(Converter converter) {
    return new CopyDataResponse(this.outputStream, converter);
  }
}
