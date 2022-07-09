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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyDoneResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyOutResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.WireOutput;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

/**
 * {@link CopyStatement} models a `COPY table TO STDOUT` statement. The same class is used both as
 * an {@link IntermediatePreparedStatement} and {@link IntermediatePortalStatement}, as COPY does
 * not support any statement parameters, which means that there is no difference between the two.
 */
@InternalApi
public class CopyToStatement extends IntermediatePortalStatement {
  private final CSVFormat csvFormat;

  public CopyToStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      CopyOptions copyOptions) {
    super(
        connectionHandler,
        options,
        name,
        createParsedStatement(copyOptions),
        createSelectStatement(copyOptions));
    CSVFormat.Builder formatBuilder =
        CSVFormat.Builder.create(CSVFormat.POSTGRESQL_TEXT)
            .setNullString(
                copyOptions.getNullString() == null
                    ? CSVFormat.POSTGRESQL_TEXT.getNullString()
                    : copyOptions.getNullString())
            .setRecordSeparator('\n')
            .setDelimiter(
                copyOptions.getDelimiter() == 0
                    ? CSVFormat.POSTGRESQL_TEXT.getDelimiterString().charAt(0)
                    : copyOptions.getDelimiter())
            .setQuote(
                copyOptions.getQuote() == 0
                    ? CSVFormat.POSTGRESQL_TEXT.getQuoteCharacter()
                    : copyOptions.getQuote())
            .setEscape(
                copyOptions.getEscape() == 0
                    ? CSVFormat.POSTGRESQL_TEXT.getEscapeCharacter()
                    : copyOptions.getEscape())
            .setQuoteMode(QuoteMode.NONE);
    if (copyOptions.hasHeader()) {
      formatBuilder.setHeader(copyOptions.getColumnNames().toArray(new String[0]));
    }
    this.csvFormat = formatBuilder.build();
  }

  static ParsedStatement createParsedStatement(CopyOptions copyOptions) {
    return PARSER.parse(createSelectStatement(copyOptions));
  }

  static Statement createSelectStatement(CopyOptions copyOptions) {
    return Statement.of("select * from " + copyOptions.getTableName());
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
  public CopyOutResponse createResultPrefix(ResultSet resultSet) {
    return new CopyOutResponse(this.outputStream, resultSet.getColumnCount(), 0);
  }

  @Override
  public WireOutput createDataRowResponse(ResultSet resultSet, QueryMode mode) {
    return createDataResponse(resultSet);
  }

  @Override
  public CopyDoneResponse createResultSuffix() {
    return new CopyDoneResponse(this.outputStream);
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
}
