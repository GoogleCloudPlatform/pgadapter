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
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.ParsedCopyStatement.Direction;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.CopyDataReceiver;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter.CopyTransactionMode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
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
  public static IntermediatePortalStatement create(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      String name,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    ParsedCopyStatement parsedCopyStatement = parse(originalStatement.getSql());
    if (parsedCopyStatement.direction == Direction.FROM) {
      return new CopyStatement(
          connectionHandler,
          options,
          name,
          parsedStatement,
          originalStatement,
          parsedCopyStatement);
    } else if (parsedCopyStatement.direction == Direction.TO) {
      return new CopyToStatement(connectionHandler, options, name, parsedCopyStatement);
    } else {
      throw PGExceptionFactory.newPGException(
          "Unsupported COPY direction: " + parsedCopyStatement.direction, SQLState.InternalError);
    }
  }

  public enum Format {
    CSV,
    TEXT,
    BINARY,
  }

  private static final String COLUMN_NAME = "column_name";
  private static final String DATA_TYPE = "data_type";

  // private final CopyTreeParser.CopyOptions options = new CopyTreeParser.CopyOptions();
  private final ParsedCopyStatement parsedCopyStatement;
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
      Statement originalStatement,
      ParsedCopyStatement parsedCopyStatement) {
    super(connectionHandler, options, name, parsedStatement, originalStatement);
    this.parsedCopyStatement = parsedCopyStatement;
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
  public void setParserFormat(ParsedCopyStatement parsedCopyStatement) {
    this.format = CSVFormat.POSTGRESQL_TEXT;
    if (parsedCopyStatement.format == Format.CSV) {
      this.format = CSVFormat.POSTGRESQL_CSV;
    }
    if (!Strings.isNullOrEmpty(parsedCopyStatement.nullString)) {
      this.format = this.format.withNullString(parsedCopyStatement.nullString);
    }
    if (parsedCopyStatement.delimiter != null) {
      this.format = this.format.withDelimiter(parsedCopyStatement.delimiter);
    }
    if (parsedCopyStatement.escape != null) {
      this.format = this.format.withEscape(parsedCopyStatement.escape);
    }
    if (parsedCopyStatement.quote != null) {
      this.format = this.format.withQuote(parsedCopyStatement.quote);
    }
    this.format = this.format.withHeader(this.tableColumns.keySet().toArray(new String[0]));
  }

  public CSVFormat getParserFormat() {
    return this.format;
  }

  public TableOrIndexName getTableName() {
    return parsedCopyStatement.table;
  }

  /** @return List of column names specified in COPY statement, if provided. */
  public List<TableOrIndexName> getCopyColumnNames() {
    return parsedCopyStatement.columns;
  }

  /** @return Format type specified in COPY statement, if provided. */
  public String getFormatType() {
    return parsedCopyStatement.format.toString();
  }

  /** @return True if copy data contains a header, false otherwise. */
  public boolean hasHeader() {
    return parsedCopyStatement.header;
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
    return (parsedCopyStatement.format == Format.BINARY) ? 1 : 0;
  }

  private void verifyCopyColumns() {
    if (getCopyColumnNames().size() > this.tableColumns.size()) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Number of copy columns provided exceed table column count");
    }
    LinkedHashMap<String, Type> tempTableColumns = new LinkedHashMap<>();
    // Verify that every copy column given is a valid table column name
    for (TableOrIndexName copyColumn : getCopyColumnNames()) {
      if (!this.tableColumns.containsKey(copyColumn.getUnquotedName())) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Column " + copyColumn + " of relation " + getTableName() + " does not exist");
      }
      // Update table column ordering with the copy column ordering
      tempTableColumns.put(
          copyColumn.getUnquotedName(), tableColumns.get(copyColumn.getUnquotedName()));
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

  private void queryInformationSchema(BackendConnection backendConnection) {
    Map<String, Type> tableColumns = new LinkedHashMap<>();
    Statement statement =
        Statement.newBuilder(
                "SELECT "
                    + COLUMN_NAME
                    + ", "
                    + DATA_TYPE
                    + " FROM information_schema.columns "
                    + "WHERE schema_name = $1 "
                    + "AND table_name = $2")
            .bind("p1")
            .to(
                getTableName().schema == null
                    ? backendConnection.getCurrentSchema()
                    : getTableName().getUnquotedSchema())
            .bind("p2")
            .to(getTableName().getUnquotedName())
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

    if (getCopyColumnNames() != null) {
      verifyCopyColumns();
    }
    this.indexedColumnsCount = queryIndexedColumnsCount(backendConnection, tableColumns.keySet());
  }

  private int queryIndexedColumnsCount(
      BackendConnection backendConnection, Set<String> columnNames) {
    String sql =
        "SELECT COUNT(*) FROM information_schema.index_columns "
            + "WHERE table_schema=$1 "
            + "and table_name=$2 "
            + "and column_name in "
            + IntStream.rangeClosed(3, columnNames.size() + 2)
                .mapToObj(i -> String.format("$%d", i))
                .collect(Collectors.joining(", ", "(", ")"));
    Statement.Builder builder =
        Statement.newBuilder(sql)
            .bind("p1")
            .to(
                getTableName().schema == null
                    ? backendConnection.getCurrentSchema()
                    : getTableName().getUnquotedSchema())
            .bind("p2")
            .to(getTableName().getUnquotedName());
    int paramIndex = 3;
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
      queryInformationSchema(backendConnection);
      setParserFormat(this.parsedCopyStatement);
      mutationWriter =
          new MutationWriter(
              connectionHandler
                  .getExtendedQueryProtocolHandler()
                  .getBackendConnection()
                  .getSessionState(),
              getTransactionMode(),
              connection,
              getTableName().getUnquotedQualifiedName(),
              getTableColumns(),
              indexedColumnsCount,
              parsedCopyStatement.format,
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

  static final class ParsedCopyStatement {
    enum Direction {
      FROM,
      TO,
    }

    Direction direction;
    TableOrIndexName table;
    String query;
    List<TableOrIndexName> columns;
    Format format = Format.TEXT;
    boolean freeze;
    Character delimiter;
    String nullString;
    boolean header;
    boolean headerMatch;
    Character quote;
    Character escape;
    List<TableOrIndexName> forceQuote;
    List<TableOrIndexName> forceNotNull;
    List<TableOrIndexName> forceNull;
    String encoding;
  }

  /** Parses `COPY my_table FROM STDIN` statements. */
  static ParsedCopyStatement parse(String sql) {
    Preconditions.checkNotNull(sql);

    SimpleParser parser = new SimpleParser(sql);
    if (!parser.eatKeyword("copy")) {
      throw PGExceptionFactory.newPGException(
          "not a valid COPY statement: " + sql, SQLState.SyntaxError);
    }
    ParsedCopyStatement parsedCopyStatement = new ParsedCopyStatement();
    if (parser.eatToken("(")) {
      parsedCopyStatement.query =
          parser.parseExpressionUntilKeyword(ImmutableList.of(), true, true);
      if (!parser.eatToken(")")) {
        throw PGExceptionFactory.newPGException(
            "missing closing parentheses after query", SQLState.SyntaxError);
      }
    } else {
      parsedCopyStatement.table = parser.readTableOrIndexName();
      if (parsedCopyStatement.table == null) {
        throw PGExceptionFactory.newPGException(
            "invalid or missing table name", SQLState.SyntaxError);
      }
      if (parser.peekToken("(")) {
        parsedCopyStatement.columns = parser.readColumnListInParentheses("columns");
      }
    }

    if (!(parser.peekKeyword("from") || parser.peekKeyword("to"))) {
      throw PGExceptionFactory.newPGException(
          "missing 'FROM' or 'TO' keyword: " + sql, SQLState.SyntaxError);
    }
    parsedCopyStatement.direction = Direction.valueOf(parser.readKeyword().toUpperCase());
    if (parsedCopyStatement.direction == Direction.FROM) {
      if (!parser.eatKeyword("stdin")) {
        throw PGExceptionFactory.newPGException(
            "missing 'STDIN' keyword. PGAdapter only supports COPY ... FROM STDIN: " + sql,
            SQLState.SyntaxError);
      }
    } else {
      if (!parser.eatKeyword("stdout")) {
        throw PGExceptionFactory.newPGException(
            "missing 'STDOUT' keyword. PGAdapter only supports COPY ... TO STDOUT: " + sql,
            SQLState.SyntaxError);
      }
    }
    parser.eatKeyword("with");
    if (parser.eatToken("(")) {
      List<String> optionExpressions = parser.parseExpressionListUntilKeyword(null, true);
      if (!parser.eatToken(")")) {
        throw PGExceptionFactory.newPGException(
            "missing closing parentheses for options list", SQLState.SyntaxError);
      }
      if (optionExpressions.isEmpty()) {
        throw PGExceptionFactory.newPGException("empty options list: " + sql, SQLState.SyntaxError);
      }
      for (String optionExpression : optionExpressions) {
        SimpleParser optionParser = new SimpleParser(optionExpression);
        if (optionParser.eatKeyword("format")) {
          if (optionParser.peekKeyword("text")
              || optionParser.peekKeyword("csv")
              || optionParser.peekKeyword("binary")) {
            parsedCopyStatement.format = Format.valueOf(optionParser.readKeyword().toUpperCase());
          } else {
            throw PGExceptionFactory.newPGException("Invalid format option: " + optionExpression);
          }
        } else if (optionParser.eatKeyword("freeze")) {
          if (optionParser.hasMoreTokens()) {
            String value = optionParser.readKeyword();
            parsedCopyStatement.freeze = BooleanParser.toBoolean(value);
          } else {
            parsedCopyStatement.freeze = true;
          }
        } else if (optionParser.eatKeyword("delimiter")) {
          eatDelimiter(optionParser, parsedCopyStatement);
        } else if (optionParser.eatKeyword("null")) {
          parsedCopyStatement.nullString = optionParser.readSingleQuotedString().getValue();
        } else if (optionParser.eatKeyword("header")) {
          if (optionParser.eatKeyword("match")) {
            parsedCopyStatement.headerMatch = true;
            parsedCopyStatement.header = true;
          } else if (optionParser.hasMoreTokens()) {
            String value = optionParser.readKeyword();
            parsedCopyStatement.header = BooleanParser.toBoolean(value);
          } else {
            parsedCopyStatement.header = true;
          }
        } else if (optionParser.eatKeyword("quote")) {
          eatQuote(optionParser, parsedCopyStatement);
        } else if (optionParser.eatKeyword("escape")) {
          eatEscape(optionParser, parsedCopyStatement);
        } else if (optionParser.eatKeyword("force_quote")) {
          if (optionParser.eatToken("*")) {
            parsedCopyStatement.forceQuote = ImmutableList.of();
          } else {
            parsedCopyStatement.forceQuote =
                optionParser.readColumnListInParentheses("force_quote");
          }
        } else if (optionParser.eatKeyword("force_not_null")) {
          parsedCopyStatement.forceNotNull =
              optionParser.readColumnListInParentheses("force_not_null");
        } else if (optionParser.eatKeyword("force_null")) {
          parsedCopyStatement.forceNull = optionParser.readColumnListInParentheses("force_null");
        } else if (optionParser.eatKeyword("encoding")) {
          parsedCopyStatement.encoding = optionParser.readSingleQuotedString().getValue();
        } else {
          throw PGExceptionFactory.newPGException(
              "Invalid or unknown option: " + optionExpression, SQLState.SyntaxError);
        }
        optionParser.throwIfHasMoreTokens();
      }
      if (parser.eatKeyword("where")) {
        throw PGExceptionFactory.newPGException(
            "PGAdapter does not support conditions in COPY ... FROM STDIN: " + sql,
            SQLState.SyntaxError);
      }
    } else if (parser.peekKeyword("binary")
        || parser.peekKeyword("delimiter")
        || parser.peekKeyword("null")
        || parser.peekKeyword("csv")) {
      parseLegacyOptions(parser, parsedCopyStatement);
    }
    parser.eatToken(";");
    parser.throwIfHasMoreTokens();

    return parsedCopyStatement;
  }

  static void parseLegacyOptions(SimpleParser parser, ParsedCopyStatement parsedCopyStatement) {
    while (true) {
      if (parser.eatKeyword("binary")) {
        parsedCopyStatement.format = Format.BINARY;
      } else if (parser.eatKeyword("delimiter")) {
        parser.eatKeyword("as");
        eatDelimiter(parser, parsedCopyStatement);
      } else if (parser.eatKeyword("null")) {
        parser.eatKeyword("as");
        parsedCopyStatement.nullString = parser.readSingleQuotedString().getValue();
      } else if (parser.eatKeyword("csv")) {
        parsedCopyStatement.format = Format.CSV;
        parser.eatKeyword("header");
        boolean foundValid;
        do {
          foundValid = false;
          if (parser.eatKeyword("quote")) {
            parser.eatKeyword("as");
            eatQuote(parser, parsedCopyStatement);
            foundValid = true;
          } else if (parser.eatKeyword("escape")) {
            parser.eatKeyword("as");
            eatEscape(parser, parsedCopyStatement);
            foundValid = true;
          } else if (parsedCopyStatement.direction == Direction.FROM) {
            if (parser.eatKeyword("force", "not", "null")) {
              parsedCopyStatement.forceNotNull = parser.readColumnList("force not null");
              foundValid = true;
            }
          } else if (parsedCopyStatement.direction == Direction.TO) {
            if (parser.eatKeyword("force", "quote")) {
              foundValid = true;
              if (parser.eatToken("*")) {
                parsedCopyStatement.forceQuote = ImmutableList.of();
              } else {
                parsedCopyStatement.forceQuote = parser.readColumnList("force quote");
              }
            }
          }
        } while (foundValid);
      } else {
        break;
      }
    }
  }

  static void eatDelimiter(SimpleParser parser, ParsedCopyStatement parsedCopyStatement) {
    String delimiter = parser.readSingleQuotedString().getValue();
    if (delimiter.length() != 1) {
      throw PGException.newBuilder("COPY delimiter must be a single one-byte character")
          .setSQLState(SQLState.SyntaxError)
          .setHints(
              "Use an escaped string to create a delimiter with a special character, like a tab.\nExample: copy my_table to stdout (delimiter e'\\t')")
          .build();
    }
    parsedCopyStatement.delimiter = delimiter.charAt(0);
  }

  static void eatQuote(SimpleParser parser, ParsedCopyStatement parsedCopyStatement) {
    String quote = parser.readSingleQuotedString().getValue();
    if (quote.length() != 1) {
      throw PGException.newBuilder("COPY quote must be a single one-byte character")
          .setSQLState(SQLState.SyntaxError)
          .setHints(
              "Use an escaped string to specify a special quote character, for example using an octal value.\nExample: copy my_table to stdout (quote e'\\4', format csv)")
          .build();
    }
    parsedCopyStatement.quote = quote.charAt(0);
  }

  static void eatEscape(SimpleParser parser, ParsedCopyStatement parsedCopyStatement) {
    String escape = parser.readSingleQuotedString().getValue();
    if (escape.length() != 1) {
      throw PGException.newBuilder("COPY escape must be a single one-byte character")
          .setSQLState(SQLState.SyntaxError)
          .setHints(
              "Use an escaped string to specify a special escape character, for example using an octal value.\nExample: copy my_table to stdout (escape e'\\4', format csv)")
          .build();
    }
    parsedCopyStatement.escape = escape.charAt(0);
  }

  //  private void parseCopyStatement() {
  //    try {
  //      Copy.parse(parsedStatement.getSqlWithoutComments(), this.options);
  //    } catch (Exception | TokenMgrError e) {
  //      throw SpannerExceptionFactory.newSpannerException(
  //          ErrorCode.INVALID_ARGUMENT, "Invalid COPY statement syntax: " + e);
  //    }
  //  }
}
