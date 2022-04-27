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
import com.google.cloud.spanner.connection.TransactionMode;
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
@InternalApi
public class IntermediateStatement {
  public static final String TRANSACTION_ABORTED_ERROR =
      "current transaction is aborted, commands ignored until end of transaction block";
  protected static final PostgreSQLStatementParser PARSER =
      (PostgreSQLStatementParser) AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected final OptionsMetadata options;
  private final ImmutableList<StatementType> statementTypes;
  protected ResultSet[] statementResults;
  protected final boolean[] hasMoreData;
  protected final SpannerException[] exceptions;
  protected final ParsedStatement parsedStatement;
  protected final ImmutableList<String> commands;
  protected final List<String> commandTags;
  protected int executedCount = 0;
  protected final Connection connection;
  protected long[] updateCounts;
  protected final ImmutableList<ParsedStatement> statements;
  protected final ConnectionHandler connectionHandler;
  private ExecutionStatus executionStatus;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';

  public IntermediateStatement(
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      ConnectionHandler connectionHandler) {
    this(connectionHandler, options, parsedStatement);
  }

  protected IntermediateStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      ParsedStatement parsedStatement) {
    this.connectionHandler = connectionHandler;
    this.options = options;
    this.parsedStatement = replaceKnownUnsupportedQueries(parsedStatement);
    this.connection = connectionHandler.getSpannerConnection();
    this.statements = parseStatements(parsedStatement);
    this.commands = StatementParser.parseCommands(this.statements);
    this.commandTags = new ArrayList<>(this.commands);
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
    boolean insideQuotes = false;
    boolean currentCharacterIsEscaped = false;
    int index = 0;
    for (int i = 0; i < sql.length(); ++i) {
      // ignore escaped quotes
      if (sql.charAt(i) == SINGLE_QUOTE && !currentCharacterIsEscaped) {
        insideQuotes = !insideQuotes;
      }
      // skip semicolon inside quotes
      if (sql.charAt(i) == STATEMENT_DELIMITER && !insideQuotes) {
        String stmt = sql.substring(index, i).trim();
        // Statements with only ';' character are empty and dropped.
        if (stmt.length() > 0) {
          builder.add(stmt);
        }
        index = i + 1;
      }
      if (currentCharacterIsEscaped) {
        currentCharacterIsEscaped = false;
      } else if (sql.charAt(i) == '\\') {
        currentCharacterIsEscaped = true;
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
    if (this.statementResults != null && this.statementResults[index] != null) {
      this.statementResults[index].close();
      this.statementResults[index] = null;
    }
  }

  /** @return True if this is a select statement, false otherwise. */
  public boolean containsResultSet(int index) {
    return this.statementTypes.get(index) == StatementType.QUERY;
  }

  /** @return True if this statement was executed, False otherwise. */
  public boolean isExecuted() {
    return executedCount > 0;
  }

  public int getExecutedCount() {
    return this.executedCount;
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
    if (connection.isInTransaction()) {
      executionStatus = ExecutionStatus.EXPLICIT_TRANSACTION;
    } else {
      executionStatus = ExecutionStatus.IMPLICIT_TRANSACTION;
    }
    while (executedCount < getStatementCount()) {
      maybeStartImplicitTransaction(executedCount);

      if (connectionHandler.getStatus() == ConnectionStatus.TRANSACTION_ABORTED
          && !"ROLLBACK".equals(getCommand(executedCount))
          && !"COMMIT".equals(getCommand(executedCount))) {
        handleExecutionException(
            executedCount,
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT, TRANSACTION_ABORTED_ERROR));
        executedCount++;
        continue;
      }
      if (getStatementType(executedCount) == StatementType.DDL) {
        try {
          prepareExecuteDdl(executedCount);
        } catch (SpannerException e) {
          handleExecutionExceptionAndTransactionStatus(executedCount, e);
          executedCount++;
          break;
        }
      }
      boolean canUseBatch = false;
      if (executedCount < (getStatementCount() - 1)) {
        StatementType statementType = getStatementType(executedCount);
        StatementType nextStatementType = getStatementType(executedCount + 1);
        canUseBatch = canBeBatchedTogether(statementType, nextStatementType);
      }
      if (canUseBatch) {
        executedCount += executeStatementsInBatch(executedCount);
      } else {
        executeSingleStatement(executedCount);
        executedCount++;
      }
      // Return directly after any exception.
      if (hasException(executedCount - 1)) {
        break;
      }
    }
    if (executionStatus == ExecutionStatus.IMPLICIT_TRANSACTION && connection.isInTransaction()) {
      if (hasException(executedCount - 1)) {
        connection.rollback();
      } else {
        connection.commit();
      }
    }
  }

  /**
   * Prepares the connection for executing a DDL statement. This can include committing the current
   * transaction.
   *
   * <p>Executing the DDL statement may or may not be allowed depending on the state of the
   * transaction and the selected {@link
   * com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode}. The method
   * will throw a {@link SpannerException} if executing a DDL statement at this point is not
   * allowed.
   */
  protected void prepareExecuteDdl(int index) {
    // Single statements are simpler to check, so we do that in a separate check.
    if (statements.size() == 1) {
      switch (options.getDdlTransactionMode()) {
        case Single:
        case Batch:
        case AutocommitImplicitTransaction:
          // Single DDL statements outside explicit transactions are always allowed. For a single
          // statement, there can also not be an implicit transaction that needs to be committed.
          if (connection.isInTransaction()) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "DDL statements are only allowed outside explicit transactions.");
          }
          break;
        case AutocommitExplicitTransaction:
          // DDL statements are allowed even in explicit transactions. Commit any transaction that
          // might be active.
          if (connection.isInTransaction()) {
            connection.commit();
          }
      }
      return;
    }

    // We are in a batch of statements.
    switch (options.getDdlTransactionMode()) {
      case Single:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.FAILED_PRECONDITION,
            "DDL statements are only allowed outside batches and transactions.");
      case Batch:
        if (connection.isInTransaction()
            || statements.stream().anyMatch(statement -> !statement.isDdl())) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.FAILED_PRECONDITION,
              "DDL statements are not allowed in mixed batches or transactions.");
        }
        break;
      case AutocommitImplicitTransaction:
        if (connection.isInTransaction()
            && executionStatus != ExecutionStatus.IMPLICIT_TRANSACTION) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.FAILED_PRECONDITION,
              "DDL statements are only allowed outside explicit transactions.");
        }
        if (connection.isInTransaction()) {
          // Commit the implicit transaction before we continue.
          connection.commit();
        }
        break;
      case AutocommitExplicitTransaction:
        // Commit any transaction that might be active and allow executing the statement.
        // Switch the execution state to implicit transaction.
        if (connection.isInTransaction()) {
          connection.commit();
          executionStatus = ExecutionStatus.IMPLICIT_TRANSACTION;
        }
    }
  }

  private void maybeStartImplicitTransaction(int index) {
    if (connection.isInTransaction()) {
      return;
    }
    if (executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
      return;
    }
    // No need to start a transaction for the last statement.
    if (index == statements.size() - 1) {
      return;
    }
    // No need to start a transaction for DDL or client side statements.
    if (statementTypes.get(index) == StatementType.DDL
        || statementTypes.get(index) == StatementType.CLIENT_SIDE) {
      return;
    }
    // If there are only DML statements left, those can be executed as an auto-commit dml batch.
    if (hasOnlyDmlStatementsAfter(index)) {
      return;
    }
    // We need to start an implicit transaction.
    // Check if a read-only transaction suffices.
    connection.beginTransaction();
    if (!hasDmlStatementsAfter(index)) {
      connection.setTransactionMode(TransactionMode.READ_ONLY_TRANSACTION);
    }
  }

  private boolean hasDmlStatementsAfter(int index) {
    return statementTypes.subList(index, statementTypes.size()).stream()
        .anyMatch(type -> type == StatementType.UPDATE);
  }

  private boolean hasOnlyDmlStatementsAfter(int index) {
    return statementTypes.subList(index, statementTypes.size()).stream()
        .allMatch(type -> type == StatementType.UPDATE);
  }

  private void executeSingleStatement(int index) {
    // Before executing the statement, handle specific statements that change the transaction status
    String command = getCommand(index);
    String statement = getStatement(index);
    if ("BEGIN".equals(command)) {
      // Executing a BEGIN statement when a transaction is already active will set the execution
      // mode to EXPLICIT_TRANSACTION. The current transaction is not committed.
      executionStatus = ExecutionStatus.EXPLICIT_TRANSACTION;
      if (!connection.isInTransaction()) {
        connection.execute(Statement.of(statement));
      }
      return;
    }
    if ("COMMIT".equals(command) || "ROLLBACK".equals(command)) {
      if (connectionHandler.getStatus() == ConnectionStatus.TRANSACTION_ABORTED) {
        connectionHandler.setStatus(ConnectionStatus.IDLE);
        // COMMIT rollbacks aborted transaction
        statement = "ROLLBACK";
        commandTags.set(index, "ROLLBACK");
      }
      // Executing ROLLBACK or COMMIT when there is no active transaction is a no-op, but will
      // change the transaction mode to implicit.
      executionStatus = ExecutionStatus.IMPLICIT_TRANSACTION;
      if (!connection.isInTransaction()) {
        return;
      }
    }
    // Ignore empty statements.
    if (!"".equals(statement)) {
      try {
        StatementResult result = this.connection.execute(Statement.of(statement));
        updateResultCount(index, result);
      } catch (SpannerException e) {
        handleExecutionExceptionAndTransactionStatus(index, e);
      }
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
      return counts.length + 1;
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

  /** @return the extracted command (first word) from the really executed SQL statement. */
  public String getCommandTag(int index) {
    return this.commandTags.get(index);
  }

  /* Used for testing purposes */
  public boolean isBatchedQuery() {
    return (statements.size() > 1);
  }

  enum ExecutionStatus {
    EXPLICIT_TRANSACTION,
    IMPLICIT_TRANSACTION
  }
}
