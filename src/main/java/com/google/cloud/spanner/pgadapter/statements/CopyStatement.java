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

import static com.google.cloud.spanner.pgadapter.parsers.copy.Copy.parse;
import static com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions.Format;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.AutocommitDmlMode;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser;
import com.google.cloud.spanner.pgadapter.parsers.copy.TokenMgrError;
import com.google.cloud.spanner.pgadapter.utils.CopyDataReceiver;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter.CopyTransactionMode;
import com.google.common.base.Strings;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.csv.CSVFormat;

/**
 * {@link CopyStatement} models a `COPY table FROM STDIN` statement. The same class is used both as
 * an {@link IntermediatePreparedStatement} and {@link IntermediatePortalStatement}, as COPY does
 * not support any statement parameters, which means that there is no difference between the two.
 */
@InternalApi
public class CopyStatement extends IntermediatePortalStatement {
  private static final String COLUMN_NAME = "column_name";
  private static final String DATA_TYPE = "data_type";

  private final CopyTreeParser.CopyOptions options = new CopyTreeParser.CopyOptions();
  private CSVFormat format;

  // Table columns read from information schema.
  private Map<String, Type> tableColumns;
  private int indexedColumnsCount;
  private MutationWriter mutationWriter;
  // We need two threads to execute a CopyStatement:
  // 1. A thread to receive the CopyData messages from the client.
  // 2. A thread to write the received data to Cloud Spanner.
  // The two are kept separate to allow each to execute at the maximum speed that it can.
  // The link between them is a piped output/input stream that ensures that backpressure is applied
  // to the client if the client is sending data at a higher speed than that the writer can handle.
  private final ExecutorService executor = Executors.newFixedThreadPool(2);

  public CopyStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
  }

  @Override
  public boolean hasException() {
    return this.exception != null;
  }

  @Override
  public SpannerException getException() {
    // Do not clear exceptions on a CopyStatement.
    return this.exception;
  }

  @Override
  public long getUpdateCount() {
    // COPY statements continue to execute while the server continues to receive a stream of
    // CopyData messages AFTER we have received a flush/sync message. We therefore need to block
    // while waiting for the update count to be available once the server has received a CopyDone
    // or CopyFailed message.
    return getUpdateCount(ResultNotReadyBehavior.BLOCK);
  }

  @Override
  public void close() throws Exception {
    if (this.mutationWriter != null) {
      this.mutationWriter.close();
    }
    this.executor.shutdown();
    super.close();
  }

  @Override
  public StatementType getStatementType() {
    return StatementType.UPDATE;
  }

  /** @return Mapping of table column names to column type. */
  public Map<String, Type> getTableColumns() {
    return this.tableColumns;
  }

  /** CSVFormat for parsing copy data based on COPY statement options specified. */
  public void setParserFormat(CopyTreeParser.CopyOptions options) {
    this.format = CSVFormat.POSTGRESQL_TEXT;
    if (options.getFormat() == Format.CSV) {
      this.format = CSVFormat.POSTGRESQL_CSV;
    }
    if (!Strings.isNullOrEmpty(options.getNullString())) {
      this.format = this.format.withNullString(options.getNullString());
    }
    if (options.getDelimiter() != '\0') {
      this.format = this.format.withDelimiter(options.getDelimiter());
    }
    if (options.getEscape() != '\0') {
      this.format = this.format.withEscape(options.getEscape());
    }
    if (options.getQuote() != '\0') {
      this.format = this.format.withQuote(options.getQuote());
    }
    this.format = this.format.withHeader(this.tableColumns.keySet().toArray(new String[0]));
  }

  public CSVFormat getParserFormat() {
    return this.format;
  }

  public String getTableName() {
    return options.getTableName();
  }

  /** @return List of column names specified in COPY statement, if provided. */
  public List<String> getCopyColumnNames() {
    return options.getColumnNames();
  }

  /** @return Format type specified in COPY statement, if provided. */
  public String getFormatType() {
    return options.getFormat().toString();
  }

  /** @return True if copy data contains a header, false otherwise. */
  public boolean hasHeader() {
    return options.hasHeader();
  }

  /** @return Null string specified in COPY statement, if provided. */
  public String getNullString() {
    return this.format.getNullString();
  }

  /** @return Delimiter character specified in COPY statement, if provided. */
  public char getDelimiterChar() {
    return this.format.getDelimiter();
  }

  /** @return Escape character specified in COPY statement, if provided. */
  public char getEscapeChar() {
    return this.format.getEscapeCharacter();
  }

  /** @return Quote character specified in COPY statement, if provided. */
  public char getQuoteChar() {
    return this.format.getQuoteCharacter();
  }

  public MutationWriter getMutationWriter() {
    return this.mutationWriter;
  }

  /** @return 0 for text/csv formatting and 1 for binary */
  public int getFormatCode() {
    return (options.getFormat() == CopyTreeParser.CopyOptions.Format.BINARY) ? 1 : 0;
  }

  private void verifyCopyColumns() {
    if (options.getColumnNames().size() == 0) {
      // Use all columns if none were specified.
      return;
    }
    if (options.getColumnNames().size() > this.tableColumns.size()) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Number of copy columns provided exceed table column count");
    }
    LinkedHashMap<String, Type> tempTableColumns = new LinkedHashMap<>();
    // Verify that every copy column given is a valid table column name
    for (String copyColumn : options.getColumnNames()) {
      if (!this.tableColumns.containsKey(copyColumn)) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Column \"" + copyColumn + "\" of relation \"" + getTableName() + "\" does not exist");
      }
      // Update table column ordering with the copy column ordering
      tempTableColumns.put(copyColumn, tableColumns.get(copyColumn));
    }
    this.tableColumns = tempTableColumns;
  }

  private static Type parsePostgreSQLDataType(String columnType) {
    // Eliminate size modifiers in column type (e.g. character varying(100), etc.)
    int index = columnType.indexOf("(");
    columnType = (index > 0) ? columnType.substring(0, index) : columnType;
    switch (columnType) {
      case "boolean":
        return Type.bool();
      case "bigint":
        return Type.int64();
      case "float8":
      case "double precision":
        return Type.float64();
      case "numeric":
        return Type.pgNumeric();
      case "bytea":
        return Type.bytes();
      case "character varying":
      case "text":
        return Type.string();
      case "date":
        return Type.date();
      case "timestamp with time zone":
        return Type.timestamp();
      case "jsonb":
        return Type.pgJsonb();
      default:
        throw new IllegalArgumentException(
            "Unrecognized or unsupported column data type: " + columnType);
    }
  }

  private void queryInformationSchema() {
    Map<String, Type> tableColumns = new LinkedHashMap<>();
    Statement statement =
        Statement.newBuilder(
                "SELECT "
                    + COLUMN_NAME
                    + ", "
                    + DATA_TYPE
                    + " FROM information_schema.columns WHERE table_name = $1")
            .bind("p1")
            .to(getTableName())
            .build();
    try (ResultSet result = connection.getDatabaseClient().singleUse().executeQuery(statement)) {
      while (result.next()) {
        String columnName = result.getString(COLUMN_NAME);
        Type type = parsePostgreSQLDataType(result.getString(DATA_TYPE));
        tableColumns.put(columnName, type);
      }
    }

    if (tableColumns.isEmpty()) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Table " + getTableName() + " is not found in information_schema");
    }

    this.tableColumns = tableColumns;

    if (options.getColumnNames() != null) {
      verifyCopyColumns();
    }
    this.indexedColumnsCount = queryIndexedColumnsCount(tableColumns.keySet());
  }

  private int queryIndexedColumnsCount(Set<String> columnNames) {
    String sql =
        "SELECT COUNT(*) FROM information_schema.index_columns "
            + "WHERE table_schema='public' "
            + "and table_name=$1 "
            + "and column_name in "
            + IntStream.rangeClosed(2, columnNames.size() + 1)
                .mapToObj(i -> String.format("$%d", i))
                .collect(Collectors.joining(", ", "(", ")"));
    Statement.Builder builder = Statement.newBuilder(sql);
    builder.bind("p1").to(getTableName());
    int paramIndex = 2;
    for (String columnName : columnNames) {
      builder.bind(String.format("p%d", paramIndex)).to(columnName);
      paramIndex++;
    }
    Statement statement = builder.build();
    try (ResultSet resultSet = connection.getDatabaseClient().singleUse().executeQuery(statement)) {
      if (resultSet.next()) {
        return (int) resultSet.getLong(0);
      }
    }
    return 0;
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
  public void executeAsync(BackendConnection backendConnection) {
    this.executed = true;
    try {
      parseCopyStatement();
      queryInformationSchema();
      setParserFormat(this.options);
      mutationWriter =
          new MutationWriter(
              connectionHandler
                  .getExtendedQueryProtocolHandler()
                  .getBackendConnection()
                  .getSessionState(),
              getTransactionMode(),
              connection,
              options.getTableName(),
              getTableColumns(),
              indexedColumnsCount,
              this.options.getFormat(),
              getParserFormat(),
              hasHeader());
      setFutureStatementResult(
          backendConnection.executeCopy(
              parsedStatement,
              statement,
              new CopyDataReceiver(this, this.connectionHandler),
              mutationWriter,
              executor));
    } catch (Exception e) {
      SpannerException spannerException = SpannerExceptionFactory.asSpannerException(e);
      handleExecutionException(spannerException);
    }
  }

  private CopyTransactionMode getTransactionMode() {
    if (connection.isInTransaction()) {
      return CopyTransactionMode.Explicit;
    } else {
      if (connection.getAutocommitDmlMode() == AutocommitDmlMode.PARTITIONED_NON_ATOMIC) {
        return CopyTransactionMode.ImplicitNonAtomic;
      }
      return CopyTransactionMode.ImplicitAtomic;
    }
  }

  private void parseCopyStatement() {
    try {
      parse(parsedStatement.getSqlWithoutComments(), this.options);
    } catch (Exception | TokenMgrError e) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid COPY statement syntax: " + e);
    }
  }
}
