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
  private final List<StatementType> statementTypes;
  protected ResultSet[] statementResults;
  protected boolean[] hasMoreData;
  protected SpannerException[] exceptions;
  protected String sql;
  protected List<String> commands;
  protected int lastExecutedIndex = -1;
  protected Connection connection;
  protected long[] updateCounts;
  protected List<String> statements;
  private ConnectionHandler connectionHandler;
  private ExecutionStatus executionStatus;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';

  public IntermediateStatement(
      OptionsMetadata options, String sql, ConnectionHandler connectionHandler) {
    this.options = options;
    this.sql = replaceKnownUnsupportedQueries(sql);
    this.statements = parseStatements(sql);
    this.commands = StatementParser.parseCommands(this.statements);
    this.connectionHandler = connectionHandler;
    this.connection = connectionHandler.getSpannerConnection();
    // Note: This determines the result type based on the first statement in the SQL statement. That
    // means that it assumes that if this is a batch of statements, all the statements in the batch
    // will have the same type of result (that is; they are all DML statements, all DDL statements,
    // all queries, etc.). That is a safe assumption for now, as PgAdapter currently only supports
    // all-DML and all-DDL batches.
    this.statementTypes = determineStatementTypes(this.statements);
    this.hasMoreData = new boolean[this.statements.size()];
    this.exceptions = new SpannerException[this.statements.size()];
  }

  protected IntermediateStatement(OptionsMetadata options, List<String> statements) {
    this.statements = statements;
    this.options = options;
    this.statementTypes = determineStatementTypes(this.statements);
    this.hasMoreData = new boolean[this.statements.size()];
    this.exceptions = new SpannerException[this.statements.size()];
  }

  protected String replaceKnownUnsupportedQueries(String sql) {
    if (this.options.isReplaceJdbcMetadataQueries()
        && JdbcMetadataStatementHelper.isPotentialJdbcMetadataStatement(sql)) {
      return JdbcMetadataStatementHelper.replaceJdbcMetadataStatement(sql);
    }
    return sql;
  }

  /** @return The number of SQL statements in this {@link IntermediateStatement} */
  public int getStatementCount() {
    return statements.size();
  }

  /**
   * Determines the statement types based on the given statement strings. The statement string must
   * already been stripped of any comments that might precede the actual statement string.
   *
   * @param statements The statement strings to determine the type for
   * @return The list of {@link StatementType} that the given statement strings will produce
   */
  protected static List<StatementType> determineStatementTypes(List<String> statements) {
    List<StatementType> statementTypes = new ArrayList<>();
    for (String sql : statements) {
      if (PARSER.isUpdateStatement(sql)) {
        statementTypes.add(StatementType.UPDATE);
      } else if (PARSER.isQuery(sql)) {
        statementTypes.add(StatementType.QUERY);
      } else if (PARSER.isDdlStatement(sql)) {
        statementTypes.add(StatementType.DDL);
      } else {
        statementTypes.add(StatementType.CLIENT_SIDE);
      }
    }
    return statementTypes;
  }

  // Split statements by ';' delimiter, but ignore anything that is nested with '' or "".
  private List<String> splitStatements(String sql) {
    List<String> statements = new ArrayList<>();
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
          statements.add(stmt);
        }
        index = i + 1;
      }
    }

    if (index < sql.length()) {
      statements.add(sql.substring(index).trim());
    }
    return statements;
  }

  protected List<String> parseStatements(String sql) {
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
    return lastExecutedIndex >= 0;
  }

  public int getLastExecutedIndex() {
    return lastExecutedIndex;
  }

  /** @return The number of items that were modified by this execution. */
  public long getUpdateCount(int index) {
    if (this.updateCounts == null) {
      return 0;
    }
    return this.updateCounts[index];
  }

  public void addUpdateCount(int index, long count) {
    if (this.updateCounts == null) {
      this.updateCounts = new long[getStatementCount()];
    }
    this.updateCounts[index] += count;
  }

  private void setUpdateCount(int index, long updateCount) {
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

  public List<String> getStatements() {
    return this.statements;
  }

  public ResultSet getStatementResult(int index) {
    if (this.statementResults == null) {
      return null;
    }
    return this.statementResults[index];
  }

  private void setStatementResult(int index, ResultSet resultSet) {
    if (this.statementResults == null) {
      this.statementResults = new ResultSet[getStatementCount()];
    }
    this.statementResults[index] = resultSet;
    this.hasMoreData[index] = resultSet.next();
  }

  public StatementType getStatementType(int index) {
    return this.statementTypes.get(index);
  }

  public String getSql() {
    return this.sql;
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

  private void handleExecutionExceptionInTransaction(int index, SpannerException e) {
    if (executionStatus == ExecutionStatus.IMPLICIT_TRANSACTION) {
      connection.rollback();
      executionStatus = ExecutionStatus.HALTED;
    } else if (executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
      connectionHandler.setStatus(ConnectionStatus.ABORTED);
      executionStatus = ExecutionStatus.HALTED;
    }
    handleExecutionException(index, e);
  }

  /** Execute the SQL statement, storing metadata. */
  public void execute() {
    if (connectionHandler.getStatus() == ConnectionStatus.ABORTED
        && !"ROLLBACK".equals(getCommand(0))) {
      lastExecutedIndex = 0;
      handleExecutionException(
          0,
          SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              "Transaction is aborted and must be rolled back prior to further execution."));
      return;
    }
    if (connection.isInTransaction()) {
      executionStatus = ExecutionStatus.EXPLICIT_TRANSACTION;
    } else if (statements.size() == 1 || "BEGIN".equals(getCommand(0))) {
      // Do not begin an implicit transaction either if
      // 1. There is only a single statement. Use autocommit mode instead.
      // 2. The first statement is BEGIN. Directly begin an explicit transaction later.
      executionStatus = ExecutionStatus.AUTOCOMMIT;
    } else {
      executionStatus = ExecutionStatus.IMPLICIT_TRANSACTION;
      connection.beginTransaction();
    }
    int index = 0;
    while (index < getStatementCount() && executionStatus != ExecutionStatus.HALTED) {
      if (getStatementType(index) == StatementType.DDL) {
        if (executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
          handleExecutionExceptionInTransaction(
              index,
              SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INVALID_ARGUMENT,
                  "DDL statements are only allowed outside explicit transactions."));
          index++;
          break;
        } else if (executionStatus == ExecutionStatus.IMPLICIT_TRANSACTION) {
          connection.commit();
          executionStatus = ExecutionStatus.AUTOCOMMIT;
        }
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
    if (executionStatus == ExecutionStatus.IMPLICIT_TRANSACTION) {
      connection.commit();
    }
    lastExecutedIndex = index - 1;
  }

  private void executeSingleStatement(int index) {
    // Before executing the statement, handle specific statements that change the transaction status
    String command = getCommand(index);
    if ("BEGIN".equals(command)) {
      if (executionStatus == ExecutionStatus.EXPLICIT_TRANSACTION) {
        // Executing a BEGIN statement when an explicit transaction is already active is a no-op
        return;
      } else if (executionStatus == ExecutionStatus.IMPLICIT_TRANSACTION) {
        // Executing a BEGIN statement when an implicit transaction is active causes the implicit
        // transaction to be committed and a new explicit transaction to be started
        connection.commit();
      }
      executionStatus = ExecutionStatus.EXPLICIT_TRANSACTION;
    }
    if ("COMMIT".equals(command) || "ROLLBACK".equals(command)) {
      executionStatus = ExecutionStatus.AUTOCOMMIT;
    }
    if ("ROLLBACK".equals(command) && connectionHandler.getStatus() == ConnectionStatus.ABORTED) {
      connectionHandler.setStatus(ConnectionStatus.IDLE);
    }
    try {
      StatementResult result = this.connection.execute(Statement.of(statements.get(index)));
      updateResultCount(index, result);
    } catch (SpannerException e) {
      handleExecutionExceptionInTransaction(index, e);
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
        connection.execute(Statement.of(statements.get(index)));
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
      handleExecutionExceptionInTransaction(fromIndex + counts.length, e);
      // TODO: May want to continue executing from the failure in order to be compatible with the
      // behavior of PG when there is no transaction.
      executionStatus = ExecutionStatus.HALTED;
    }
    return index - fromIndex;
  }

  private boolean canBeBatchedTogether(StatementType statement1, StatementType statement2) {
    if (Objects.equals(statement1, StatementType.QUERY)
        || Objects.equals(statement1, StatementType.CLIENT_SIDE)) {
      return false;
    }
    return Objects.equals(statement1, statement2);
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
    IMPLICIT_TRANSACTION,
    EXPLICIT_TRANSACTION,
    AUTOCOMMIT,
    HALTED
  }
}
