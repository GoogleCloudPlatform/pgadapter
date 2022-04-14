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
import com.google.cloud.spanner.SpannerBatchUpdateException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.PostgreSQLStatementParser;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
public class IntermediateStatement {
  protected static final PostgreSQLStatementParser PARSER =
      (PostgreSQLStatementParser) AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected final OptionsMetadata options;
  private final ImmutableList<StatementType> statementTypes;
  protected ResultSet[] statementResults;
  protected boolean[] hasMoreData;
  protected SpannerException[] exceptions;
  protected final ParsedStatement parsedStatement;
  protected List<String> commands;
  protected boolean executed = false;
  protected Connection connection;
  protected long[] updateCounts;
  protected List<ParsedStatement> statements;
  private ConnectionHandler connectionHandler;
  private ExecutionStatus executionStatus;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';

  public IntermediateStatement(
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      ConnectionHandler connectionHandler) {
    this(options, parsedStatement, connectionHandler.getSpannerConnection());
    this.connectionHandler = connectionHandler;
  }

  protected IntermediateStatement(
      OptionsMetadata options, ParsedStatement parsedStatement, Connection connection) {
    this.options = options;
    this.parsedStatement = replaceKnownUnsupportedQueries(parsedStatement);
    this.connection = connection;
    this.statements = parseStatements(parsedStatement);
    this.commands = StatementParser.parseCommands(this.statements);
    this.statementTypes = determineStatementTypes(this.statements);
    this.hasMoreData = new boolean[this.statements.size()];
    this.exceptions = new SpannerException[this.statements.size()];
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

  /** @return The number of SQL statements in this {@link IntermediateStatement} */
  public int getStatementCount() {
    return statements.size();
  }

  /**
   * Determines the statement types based on the given statement strings. The statement string must
   * already been stripped of any comments that might precede the actual statement string.
   *
   * @param statements The parsed statements to determine the type for
   * @return The list of {@link StatementType} that the given statement strings will produce
   */
  protected static ImmutableList<StatementType> determineStatementTypes(
      List<ParsedStatement> statements) {
    ImmutableList.Builder<StatementType> builder = ImmutableList.builder();
    for (ParsedStatement stmt : statements) {
      if (stmt.isUpdate()) {
        builder.add(StatementType.UPDATE);
      } else if (stmt.isQuery()) {
        builder.add(StatementType.QUERY);
      } else if (stmt.isDdl()) {
        builder.add(StatementType.DDL);
      } else {
        builder.add(StatementType.CLIENT_SIDE);
      }
    }
    return builder.build();
  }

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
      // skip escaped quotes
      if (sql.charAt(i) == SINGLE_QUOTE && (i == 0 || sql.charAt(i - 1) != '\\')) {
        quoteEscape = !quoteEscape;
      }
      // skip semicolon inside quotes
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

  protected static List<ParsedStatement> parseStatements(ParsedStatement stmt) {
    String sql = stmt.getSqlWithoutComments();
    Preconditions.checkNotNull(sql);
    List<ParsedStatement> statements = new ArrayList<>();
    for (String statement : splitStatements(sql)) {
      statements.add(PARSER.parse(Statement.of(statement)));
    }
    return statements;
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
    for (int index = 0; index < this.getStatementCount(); index++) {
      close(index);
    }
  }

  public void close(int index) throws Exception {
    if (this.getStatementResult(index) != null) {
      this.getStatementResult(index).close();
    }
  }

  /** @return True if this is a select statement, false otherwise. */
  public boolean containsResultSet(int index) {
    return this.statementTypes.get(index) == StatementType.QUERY;
  }

  /** @return True if this statement was executed, False otherwise. */
  public boolean isExecuted() {
    return executed;
  }

  /**
   * @return The number of items that were modified by this execution for DML. 0 for DDL and -1 for
   *     QUERY.
   */
  public long getUpdateCount(int index) {
    if (this.updateCounts == null) {
      return 0;
    }
    return this.updateCounts[index];
  }

  public void setUpdateCount(int index, long updateCount) {
    if (this.updateCounts == null) {
      this.updateCounts = new long[getStatementCount()];
    }
    this.updateCounts[index] = updateCount;
    this.hasMoreData[index] = false;
  }

  /** @return True if at some point in execution, and exception was thrown. */
  public boolean hasException(int index) {
    return this.exceptions[index] != null;
  }

  /** @return True if only a subset of the available data has been returned. */
  public boolean isHasMoreData(int index) {
    return this.hasMoreData[index];
  }

  public void setHasMoreData(int index, boolean hasMoreData) {
    this.hasMoreData[index] = hasMoreData;
  }

  public Connection getConnection() {
    return this.connection;
  }

  public List<ParsedStatement> getStatements() {
    return this.statements;
  }

  public String getStatement(int index) {
    return this.statements.get(index).getSqlWithoutComments();
  }

  public ResultSet getStatementResult(int index) {
    if (this.statementResults == null) {
      return null;
    }
    return this.statementResults[index];
  }

  protected void setStatementResult(int index, ResultSet resultSet) {
    if (this.statementResults == null) {
      this.statementResults = new ResultSet[getStatementCount()];
    }
    this.statementResults[index] = resultSet;
    this.hasMoreData[index] = resultSet.next();
    if (this.updateCounts == null) {
      this.updateCounts = new long[getStatementCount()];
    }
    this.updateCounts[index] = -1;
  }

  public StatementType getStatementType(int index) {
    return this.statementTypes.get(index);
  }

  public String getSql() {
    return this.parsedStatement.getSqlWithoutComments();
  }

  public Exception getException(int index) {
    Exception e = this.exceptions[index];
    this.exceptions[index] = null;
    return e;
  }

  /**
   * Processes the results from an execute/executeBatch execution, extracting metadata from that
   * execution (including results and update counts). An array of updateCounts is needed in the case
   * of updateBatchResultCount.
   */
  protected void updateResultCount(int index, StatementResult result) {
    switch (result.getResultType()) {
      case RESULT_SET:
        setStatementResult(index, result.getResultSet());
        break;
      case UPDATE_COUNT:
        setUpdateCount(index, result.getUpdateCount());
        break;
      case NO_RESULT:
        this.hasMoreData[index] = false;
        break;
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL, "Unknown or unsupported result type: " + result.getResultType());
    }
  }

  protected void updateBatchResultCount(int fromIndex, long[] updateCounts) {
    for (int index = fromIndex; index < fromIndex + updateCounts.length; index++) {
      setUpdateCount(index, updateCounts[index - fromIndex]);
    }
  }

  /**
   * Clean up and save metadata when an exception occurs.
   *
   * @param e The exception to store.
   */
  protected void handleExecutionException(int index, SpannerException e) {
    this.exceptions[index] = e;
    this.hasMoreData[index] = false;
  }

  private void handleExecutionExceptionAndTransactionStatus(int index, SpannerException e) {
    if (executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
      connectionHandler.setStatus(ConnectionStatus.TRANSACTION_ABORTED);
    }
    handleExecutionException(index, e);
  }

  /** Execute the SQL statement, storing metadata. */
  public void execute() {
    executed = true;
    if (connection.isInTransaction()) {
      executionStatus = ExecutionStatus.EXPLICIT_TRANSACTION;
    } else {
      executionStatus = ExecutionStatus.AUTOCOMMIT;
    }
    int index = 0;
    while (index < getStatementCount()) {
      if (connectionHandler.getStatus() == ConnectionStatus.TRANSACTION_ABORTED
          && !"ROLLBACK".equals(getCommand(index))
          && !"COMMIT".equals(getCommand(index))) {
        handleExecutionException(
            index,
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "ERROR: current transaction is aborted, commands ignored until end of transaction block"));
        index++;
        continue;
      }
      if (getStatementType(index) == StatementType.DDL
          && executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
        handleExecutionExceptionAndTransactionStatus(
            index,
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "ERROR: DDL statements are only allowed outside explicit transactions."));
        index++;
        continue;
      }
      boolean canUseBatch = false;
      if (index < (getStatementCount() - 1)) {
        StatementType statementType = getStatementType(index);
        StatementType nextStatementType = getStatementType(index + 1);
        canUseBatch = canBeBatchedTogether(statementType, nextStatementType);
      }
      if (canUseBatch) {
        index += executeStatementsInBatch(index);
      } else {
        executeSingleStatement(index);
        index++;
      }
    }
  }

  private void executeSingleStatement(int index) {
    // Before executing the statement, handle specific statements that change the transaction status
    String command = getCommand(index);
    if ("BEGIN".equals(command)) {
      if (executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
        // Executing a BEGIN statement when an explicit transaction is already active is a no-op
        return;
      } else {
        executionStatus = ExecutionStatus.EXPLICIT_TRANSACTION;
      }
    }
    if ("COMMIT".equals(command) || "ROLLBACK".equals(command)) {
      if (executionStatus == ExecutionStatus.AUTOCOMMIT) {
        // Executing a COMMIT/ROLLBACK statement in autocommit mode is a no-op
        return;
      }
      executionStatus = ExecutionStatus.AUTOCOMMIT;
      if (connectionHandler.getStatus() == ConnectionStatus.TRANSACTION_ABORTED) {
        connectionHandler.setStatus(ConnectionStatus.IDLE);
        // COMMIT rollbacks aborted transaction
        statements.set(index, PARSER.parse(Statement.of("ROLLBACK")));
        commands.set(index, "ROLLBACK");
      }
    }
    try {
      StatementResult result = this.connection.execute(Statement.of(getStatement(index)));
      updateResultCount(index, result);
    } catch (SpannerException e) {
      handleExecutionExceptionAndTransactionStatus(index, e);
    }
  }

  /**
   * Executes the statements from fromIndex in a DML/DDL batch. The batch will consist of all
   * statements from fromIndex till the first statement that is of a different type than the
   * statement at fromIndex. That is; If the first statement is a DML statement, the batch will
   * contain all statements that follow until it encounters a statement that is not a DML statement.
   * The same also applies to DDL statements. Query statements and other statements can not be
   * batched.
   *
   * @param fromIndex The index of the statements array where the batch should start
   * @return The number of statements included in the batch.
   */
  private int executeStatementsInBatch(int fromIndex) {
    Preconditions.checkArgument(fromIndex < getStatementCount() - 1);
    Preconditions.checkArgument(
        canBeBatchedTogether(getStatementType(fromIndex), getStatementType(fromIndex + 1)));
    StatementType batchType = getStatementType(fromIndex);
    switch (batchType) {
      case UPDATE:
        connection.startBatchDml();
        break;
      case DDL:
        connection.startBatchDdl();
        break;
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Statement type is not supported for batching");
    }
    int index = fromIndex;
    while (index < getStatementCount()) {
      StatementType statementType = getStatementType(index);
      if (canBeBatchedTogether(batchType, statementType)) {
        connection.execute(Statement.of(getStatement(index)));
        index++;
      } else {
        // End the batch here, as the statement type on this index can not be batched together with
        // the other statements in the batch.
        break;
      }
    }
    try {
      long[] counts = connection.runBatch();
      updateBatchResultCount(fromIndex, counts);
    } catch (SpannerBatchUpdateException e) {
      long[] counts = e.getUpdateCounts();
      updateBatchResultCount(fromIndex, counts);
      handleExecutionExceptionAndTransactionStatus(fromIndex + counts.length, e);
      // Skip remaining statements in DML/DDL batch
      // TODO: May want to start off from the failure in order to be compatible with the
      // behavior of PG.
      for (int i = fromIndex + counts.length + 1; i < index; i++) {
        handleExecutionException(
            i,
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INTERNAL,
                "ERROR: Statement is not executed due to the preceding error in DML/DDL batch."));
      }
    }
    return index - fromIndex;
  }

  private boolean canBeBatchedTogether(StatementType statementType1, StatementType statementType2) {
    if (Objects.equals(statementType1, StatementType.QUERY)
        || Objects.equals(statementType1, StatementType.CLIENT_SIDE)) {
      return false;
    }
    return Objects.equals(statementType1, statementType2);
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
  public String getCommand(int index) {
    return this.commands.get(index);
  }

  /* Used for testing purposes */
  public boolean isBatchedQuery() {
    return (statements.size() > 1);
  }

  enum ExecutionStatus {
    EXPLICIT_TRANSACTION,
    AUTOCOMMIT
  }
}
