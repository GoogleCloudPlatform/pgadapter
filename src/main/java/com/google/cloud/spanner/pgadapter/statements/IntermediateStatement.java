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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.PostgreSQLStatementParser;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
public class IntermediateStatement {
  protected static final PostgreSQLStatementParser PARSER =
      (PostgreSQLStatementParser) AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected final OptionsMetadata options;
  private final ResultType resultType;
  protected ResultSet statementResult;
  protected boolean hasMoreData;
  protected SpannerException exception;
  protected final ParsedStatement parsedStatement;
  protected final String command;
  protected boolean executed;
  protected final Connection connection;
  protected Long updateCount;
  protected final ImmutableList<String> statements;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';

  public IntermediateStatement(
      OptionsMetadata options, ParsedStatement parsedStatement, Connection connection) {
    this(
        options,
        parsedStatement,
        connection,
        parseStatements(parsedStatement.getSqlWithoutComments()));
  }

  protected IntermediateStatement(
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      Connection connection,
      ImmutableList<String> statements) {
    this.options = options;
    this.parsedStatement = replaceKnownUnsupportedQueries(parsedStatement);
    this.statements = statements;
    this.command = StatementParser.parseCommand(this.parsedStatement.getSqlWithoutComments());
    this.connection = connection;
    // Note: This determines the result type based on the first statement in the SQL statement. That
    // means that it assumes that if this is a batch of statements, all the statements in the batch
    // will have the same type of result (that is; they are all DML statements, all DDL statements,
    // all queries, etc.). That is a safe assumption for now, as PgAdapter currently only supports
    // all-DML and all-DDL batches.
    this.resultType = determineResultType(this.parsedStatement);
  }

  protected ParsedStatement replaceKnownUnsupportedQueries(ParsedStatement parsedStatement) {
    if (this.options.isReplaceJdbcMetadataQueries()
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
   * Determines the result type based on the given sql string. The sql string must already been
   * stripped of any comments that might precede the actual sql string.
   *
   * @param parsedStatement The parsed statement to determine the type of result for
   * @return The {@link ResultType} that the given sql string will produce
   */
  protected static ResultType determineResultType(ParsedStatement parsedStatement) {
    if (parsedStatement.isUpdate()) {
      return ResultType.UPDATE_COUNT;
    } else if (parsedStatement.isQuery()) {
      return ResultType.RESULT_SET;
    } else {
      return ResultType.NO_RESULT;
    }
  }

  // Split statements by ';' delimiter, but ignore anything that is nested with '' or "".
  private static ImmutableList<String> splitStatements(String sql) {
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
    boolean quoteEscape = false;
    int index = 0;
    for (int i = 0; i < sql.length(); ++i) {
      if (sql.charAt(i) == SINGLE_QUOTE) {
        quoteEscape = !quoteEscape;
      }
      if (sql.charAt(i) == STATEMENT_DELIMITER && !quoteEscape) {
        String stmt = sql.substring(index, i).trim();
        // Statements with only ';' character are empty and dropped.
        if (stmt.length() > 0) {
          builder.add(stmt);
        }
        index = i + 1;
      }
    }

    if (index < sql.length()) {
      builder.add(sql.substring(index).trim());
    }
    return builder.build();
  }

  protected static ImmutableList<String> parseStatements(String sql) {
    Preconditions.checkNotNull(sql);
    return splitStatements(sql);
  }

  /**
   * Whether this is a bound statement (i.e.: ready to execute)
   *
   * @return True if bound, false otherwise.
   */
  public boolean isBound() {
    return true;
  }

  /**
   * Cleanly close the statement. Does nothing if the statement has not been executed or has no
   * result.
   *
   * @throws Exception if closing fails server-side.
   */
  public void close() throws Exception {
    if (this.getStatementResult() != null) {
      this.getStatementResult().close();
    }
  }

  /** @return True if this is a select statement, false otherwise. */
  public boolean containsResultSet() {
    return this.resultType == ResultType.RESULT_SET;
  }

  /** @return True if this statement was executed, False otherwise. */
  public boolean isExecuted() {
    return executed;
  }

  /** @return The number of items that were modified by this execution. */
  public Long getUpdateCount() {
    return this.updateCount;
  }

  public void addUpdateCount(long count) {
    if (this.updateCount == null) {
      this.updateCount = 0L;
    }
    this.updateCount += count;
  }

  /** @return True if at some point in execution, and exception was thrown. */
  public boolean hasException() {
    return this.exception != null;
  }

  /** @return True if only a subset of the available data has been returned. */
  public boolean isHasMoreData() {
    return this.hasMoreData;
  }

  public void setHasMoreData(boolean hasMoreData) {
    this.hasMoreData = hasMoreData;
  }

  public Connection getConnection() {
    return this.connection;
  }

  public List<String> getStatements() {
    return this.statements;
  }

  public ResultSet getStatementResult() {
    return this.statementResult;
  }

  public ResultType getResultType() {
    return this.resultType;
  }

  public String getSql() {
    return this.parsedStatement.getSqlWithoutComments();
  }

  public Exception getException() {
    Exception e = this.exception;
    this.exception = null;
    return e;
  }

  /**
   * Processes the results from an execute/executeBatch execution, extracting metadata from that
   * execution (including results and update counts). An array of updateCounts is needed in the case
   * of updateBatchResultCount.
   */
  protected void updateResultCount(StatementResult result) {
    switch (result.getResultType()) {
      case RESULT_SET:
        this.statementResult = result.getResultSet();
        this.hasMoreData = this.statementResult.next();
        break;
      case UPDATE_COUNT:
        this.updateCount = result.getUpdateCount();
        this.hasMoreData = false;
        this.statementResult = null;
        break;
      case NO_RESULT:
        this.hasMoreData = false;
        this.statementResult = null;
        break;
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL, "Unknown or unsupported result type: " + result.getResultType());
    }
  }

  protected void updateBatchResultCount(long[] updateCounts) {
    this.updateCount = 0L;
    for (int i = 0; i < updateCounts.length; ++i) {
      this.updateCount += updateCounts[i];
    }
    this.hasMoreData = false;
    this.statementResult = null;
  }

  /**
   * Clean up and save metadata when an exception occurs.
   *
   * @param e The exception to store.
   */
  protected void handleExecutionException(SpannerException e) {
    this.exception = e;
    this.hasMoreData = false;
    this.statementResult = null;
  }

  /** Execute the SQL statement, storing metadata. */
  public void execute() {
    this.executed = true;
    try {
      if (statements.size() > 1) {
        // TODO: Clean this up a little once the statement parsing in the client library has been
        // released: https://github.com/googleapis/java-spanner/pull/1690
        // Also, the restriction that mixed batches are not allowed will be removed in a future PR.
        if (resultType == ResultType.UPDATE_COUNT) {
          connection.startBatchDml();
        } else if (resultType == ResultType.NO_RESULT) {
          connection.startBatchDdl();
        } else {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT, "Statement type is not supported for batching");
        }
        for (String stmt : statements) {
          connection.execute(Statement.of(stmt));
        }
        long[] updateCounts = connection.runBatch();
        updateBatchResultCount(updateCounts);
      } else {
        // Ignore empty statements.
        if (!"".equals(this.parsedStatement.getSqlWithoutComments())) {
          StatementResult result =
              connection.execute(Statement.of(this.parsedStatement.getSqlWithoutComments()));
          updateResultCount(result);
        }
      }
    } catch (SpannerException e) {
      if (statements.size() > 1) {
        SpannerException exception =
            SpannerExceptionFactory.newSpannerException(
                e.getErrorCode(),
                e.getMessage() + " \"" + this.parsedStatement.getSqlWithoutComments() + "\"",
                e);
        handleExecutionException(exception);
      } else {
        handleExecutionException(e);
      }
    }
  }

  /**
   * Moreso meant for inherited classes, allows one to call describe on a statement. Since raw
   * statements cannot be described, throw an error.
   */
  public DescribeMetadata describe() {
    throw new IllegalStateException(
        "Cannot describe a simple statement " + "(only prepared statements and portals)");
  }

  /**
   * Moreso intended for inherited classes (prepared statements et al) which allow the setting of
   * result format codes. Here we dafault to string.
   */
  public short getResultFormatCode(int index) {
    return 0;
  }

  /** @return the extracted command (first word) from the SQL statement. */
  public String getCommand() {
    return this.command;
  }

  /* Used for testing purposes */
  public boolean isBatchedQuery() {
    return (statements.size() > 1);
  }

  public enum ResultType {
    UPDATE_COUNT,
    RESULT_SET,
    NO_RESULT
  }
}
