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
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions;
import com.google.cloud.spanner.pgadapter.wireoutput.CommandCompleteResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyDataResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyDoneResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyOutResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.WireOutput;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@link CopyStatement} models a `COPY table TO STDOUT` statement. The same class is used both as
 * an {@link IntermediatePreparedStatement} and {@link IntermediatePortalStatement}, as COPY does
 * not support any statement parameters, which means that there is no difference between the two.
 */
@InternalApi
public class CopyToStatement extends IntermediatePortalStatement {
  private final CopyOptions copyOptions;
  private final DataFormat format;
  private final char delimiter;
  private final char rowTerminator;
  private final byte[] nullString;

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
    this.copyOptions = copyOptions;
    this.format = DataFormat.POSTGRESQL_TEXT;
    this.delimiter = copyOptions.getDelimiter() == 0 ? '\t' : copyOptions.getDelimiter();
    this.rowTerminator = '\n';
    this.nullString =
        copyOptions.getNullString() == null
            ? "\\N".getBytes(StandardCharsets.UTF_8)
            : copyOptions.getNullString().getBytes(StandardCharsets.UTF_8);
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
  public IntermediatePortalStatement bind(
      String name,
      byte[][] parameters,
      List<Short> parameterFormatCodes,
      List<Short> resultFormatCodes) {
    // COPY does not support binding any parameters, so we just return the same statement.
    return this;
  }

  void sendSpannerResult(BackendConnection backendConnection) {
    try (ResultSet resultSet =
        backendConnection
            .getSpannerConnection()
            .executeQuery(Statement.of("select * from " + copyOptions.getTableName()))) {
      new CopyOutResponse(
              this.connectionHandler.getConnectionMetadata().getOutputStream(),
              resultSet.getColumnCount(),
              0)
          .send();
      long rowCount = 0L;
      while (resultSet.next()) {
        createDataResponse(resultSet).send(false);
        rowCount++;
      }
      new CopyDoneResponse(this.connectionHandler.getConnectionMetadata().getOutputStream())
          .send(false);
      new CommandCompleteResponse(
              this.connectionHandler.getConnectionMetadata().getOutputStream(), "COPY " + rowCount)
          .send();
    } catch (SpannerException spannerException) {
      handleExecutionException(spannerException);
    } catch (Exception exception) {
      handleExecutionException(SpannerExceptionFactory.asSpannerException(exception));
    }
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
    byte[][] data = new byte[resultSet.getColumnCount()][];
    for (int col = 0; col < resultSet.getColumnCount(); col++) {
      if (resultSet.isNull(col)) {
        data[col] = nullString;
      } else {
        Parser<?> parser = Parser.create(resultSet, resultSet.getColumnType(col), col);
        data[col] = parser.parse(format);
      }
    }
    return new CopyDataResponse(this.outputStream, data, this.delimiter, this.rowTerminator);
  }
}
