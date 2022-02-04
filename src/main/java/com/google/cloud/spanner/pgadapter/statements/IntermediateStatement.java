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
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
public class IntermediateStatement {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected Statement statement;
  private final ImmutableList<StatementType> statementTypes;
  protected ResultSet[] statementResults;
  private final boolean[] hasMoreData;
  protected Exception exception;
  protected String sql;
  protected String command;
  protected boolean executed;
  protected Connection connection;
  protected int[] updateCounts;
  private final ImmutableList<String> statements;

  private static final char STATEMENT_DELIMITER = ';';
  private static final char SINGLE_QUOTE = '\'';

  public IntermediateStatement(String sql, Connection connection) throws SQLException {
    this.sql = sql;
    this.statements = parseStatements(sql);
    this.command = parseCommand(sql);
    this.connection = connection;
    this.statement = connection.createStatement();
    // Note: This determines the result type based on the first statement in the SQL statement. That
    // means that it assumes that if this is a batch of statements, all the statements in the batch
    // will have the same type of result (that is; they are all DML statements, all DDL statements,
    // all queries, etc.). That is a safe assumption for now, as PgAdapter currently only supports
    // all-DML and all-DDL batches.
    this.statementTypes = determineResultTypes(this.statements);
    this.hasMoreData = new boolean[this.statements.size()];
  }

  protected IntermediateStatement(ImmutableList<String> statements) {
    this.statements = statements;
    this.statementTypes = determineResultTypes(statements);
    this.hasMoreData = new boolean[this.statements.size()];
  }

  /** @return The number of SQL statements in this {@link IntermediateStatement} */
  public int getStatementCount() {
    return statements.size();
  }

  /**
   * Determines the result types based on the given sql string. The sql string must already been
   * stripped of any comments that might precede the actual sql string.
   *
   * @param statements The statement strings to determine the type of result for
   * @return The {@link StatementType} that the given sql string will produce
   */
  protected static ImmutableList<StatementType> determineResultTypes(List<String> statements) {
    ImmutableList.Builder<StatementType> builder = ImmutableList.builder();
    for (String sql : statements) {
      if (PARSER.isUpdateStatement(sql)) {
        builder.add(StatementType.DML);
      } else if (PARSER.isQuery(sql)) {
        builder.add(StatementType.QUERY);
      } else {
        builder.add(StatementType.DDL);
      }
    }
    return builder.build();
  }

  // Split statements by ';' delimiter, but ignore anything that is nested with '' or "".
  private ImmutableList<String> splitStatements(String sql) {
    ImmutableList.Builder<String> statements = ImmutableList.builder();
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
    return statements.build();
  }

  protected ImmutableList<String> parseStatements(String sql) {
    return splitStatements(Preconditions.checkNotNull(sql));
  }

  /** Determines the (update) command that was received from the sql string. */
  protected static String parseCommand(String sql) {
    Preconditions.checkNotNull(sql);
    String[] tokens = sql.split("\\s+", 2);
    if (tokens.length > 0) {
      return tokens[0].toUpperCase();
    }
    return null;
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
    if (this.statementResults != null) {
      for (ResultSet resultSet : this.statementResults) {
        if (resultSet != null) {
          resultSet.close();
        }
      }
    }
  }

  /** @return True if this is a select statement, false otherwise. */
  public boolean isResultSet(int index) {
    return statementTypes.get(index) == StatementType.QUERY;
  }

  /** @return True if this statement was executed, False otherwise. */
  public boolean isExecuted() {
    return executed;
  }

  /** @return The number of items that were modified by this execution. */
  public int getUpdateCount(int index) {
    return this.updateCounts[index];
  }

  /** @return True if at some point in execution, and exception was thrown. */
  public boolean hasException() {
    return this.exception != null;
  }

  /** @return True if only a subset of the available data has been returned. */
  public boolean isHasMoreData(int index) {
    return this.hasMoreData[index];
  }

  public void setHasMoreData(int index, boolean hasMoreData) {
    this.hasMoreData[index] = hasMoreData;
  }

  public Statement getStatement() {
    return this.statement;
  }

  public List<String> getStatements() {
    return this.statements;
  }

  public ResultSet getStatementResult(int index) {
    if (statementResults == null) {
      return null;
    }
    return this.statementResults[index];
  }

  public ImmutableList<StatementType> getResultTypes() {
    return this.statementTypes;
  }

  public StatementType getStatementType(int index) {
    return this.statementTypes.get(index);
  }

  public String getSql() {
    return this.sql;
  }

  public Exception getException() {
    Exception e = this.exception;
    this.exception = null;
    return e;
  }

  private void setResultSet(int index, ResultSet resultSet) throws SQLException {
    if (this.statementResults == null) {
      this.statementResults = new ResultSet[getStatementCount()];
    }
    this.statementResults[index] = resultSet;
    this.hasMoreData[index] = resultSet.next();
  }

  private void setUpdateCount(int index, int updateCount) {
    if (this.updateCounts == null) {
      this.updateCounts = new int[getStatementCount()];
    }
    this.updateCounts[index] = updateCount;
    this.hasMoreData[index] = false;
  }

  /**
   * Processes the results from an execute/executeBatch execution, extracting metadata from that
   * execution (including results and update counts). An array of updateCounts is needed in the case
   * of updateBatchResultCount.
   *
   * @throws SQLException If an issue occurred in extracting result metadata.
   */
  protected void updateResultCount(int index) throws SQLException {
    if (this.isResultSet(index)) {
      setResultSet(index, this.statement.getResultSet());
    } else {
      setUpdateCount(index, this.statement.getUpdateCount());
    }
  }

  protected void updateBatchResultCount(int fromIndex, int[] updateCounts) throws SQLException {
    for (int index = fromIndex; index < fromIndex + updateCounts.length; index++) {
      setUpdateCount(index, updateCounts[index - fromIndex]);
    }
  }

  /**
   * Clean up and save metadata when an exception occurs.
   *
   * @param e The exception to store.
   */
  protected void handleExecutionException(SQLException e) {
    this.exception = e;
    // TODO: Add more tests for error handling when in a batch. We should probably return the
    // results that we already have if an error occurs halfway.
    // this.hasMoreData = false;
    this.statementResults = null;
    this.updateCounts = null;
  }

  /** Execute the SQL statement, storing metadata. */
  public void execute() {
    this.executed = true;
    try {
      if (statements.size() > 1) {
        executeBatch();
      } else {
        this.statement.execute(this.sql);
        this.updateResultCount(0);
      }
    } catch (SQLException e) {
      if (statements.size() > 1) {
        SQLException exception = new SQLException(e.getMessage() + " \"" + this.sql + "\"", e);
        handleExecutionException(exception);
      } else {
        handleExecutionException(e);
      }
    }
  }

  /**
   * Executes all the statements in this {@link IntermediateStatement}. This method tries to batch
   * statements together when that is possible (i.e. multiple DML statements in a row), and executes
   * other statements individually.
   *
   * @throws SQLException If a database error occurs
   */
  private void executeBatch() throws SQLException {
    Preconditions.checkState(this.statements.size() > 1);
    int index = 0;
    while (index < getStatementCount()) {
      boolean canUseBatch = false;
      if (index < (getStatementCount() - 1)) {
        StatementType statementType = getStatementType(index);
        StatementType nextStatementType = getStatementType(index + 1);
        canUseBatch = canBeBatchedTogether(statementType, nextStatementType);
      }
      if (canUseBatch) {
        index += executeStatementsInBatch(index);
      } else {
        this.statement.execute(statements.get(index));
        updateResultCount(index);
        index++;
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
  private int executeStatementsInBatch(int fromIndex) throws SQLException {
    Preconditions.checkArgument(fromIndex < getStatementCount() - 1);
    Preconditions.checkArgument(
        canBeBatchedTogether(getStatementType(fromIndex), getStatementType(fromIndex + 1)));
    StatementType batchType = getResultTypes().get(fromIndex);
    // Add the first statement to the batch, as we know that this statement is compatible with this
    // batch.
    this.statement.addBatch(statements.get(fromIndex));
    int index = fromIndex + 1;
    while (index < getStatementCount()) {
      StatementType statementType = getStatementType(index);
      if (canBeBatchedTogether(batchType, statementType)) {
        statement.addBatch(statements.get(index));
        index++;
      } else {
        // End the batch here, as the statement type on this index can not be batched together with
        // the other statements in the batch.
        break;
      }
    }
    int[] updateCounts = this.statement.executeBatch();
    updateBatchResultCount(fromIndex, updateCounts);

    return updateCounts.length;
  }

  private boolean canBeBatchedTogether(StatementType statement1, StatementType statement2) {
    if (Objects.equals(statement1, StatementType.QUERY)
        || Objects.equals(statement1, StatementType.OTHER)) {
      return false;
    }
    return Objects.equals(statement1, statement2);
  }

  /**
   * Moreso meant for inherited classes, allows one to call describe on a statement. Since raw
   * statements cannot be described, throw an error.
   */
  public DescribeMetadata describe() throws Exception {
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

  // TODO: Replace with StatementType in the Connection API.
  public enum StatementType {
    DML,
    QUERY,
    DDL,
    /** Other includes statements like 'BEGIN', 'SHOW transaction_isolation_level', etc. */
    OTHER
  }
}
