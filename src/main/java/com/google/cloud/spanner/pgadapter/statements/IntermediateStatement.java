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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
public class IntermediateStatement {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected Statement statement;
  private final ResultType resultType;
  protected ResultSet statementResult;
  protected boolean hasMoreData;
  protected Exception exception;
  protected String sql;
  protected String command;
  protected boolean executed;
  protected Connection connection;
  protected Integer updateCount;
  protected List<String> statements;

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
    this.resultType = determineResultType(sql);
  }

  protected IntermediateStatement(String sql) {
    this.resultType = determineResultType(sql);
  }

  /**
   * Determines the result type based on the given sql string. The sql string must already been
   * stripped of any comments that might precede the actual sql string.
   *
   * @param sql The sql string to determine the type of result for
   * @return The {@link ResultType} that the given sql string will produce
   */
  protected static ResultType determineResultType(String sql) {
    if (PARSER.isUpdateStatement(sql)) {
      return ResultType.UPDATE_COUNT;
    } else if (PARSER.isQuery(sql)) {
      return ResultType.RESULT_SET;
    } else {
      return ResultType.NO_RESULT;
    }
  }

  // Split statements by ';' delimiter, but ignore anything that is nested with '' or "".
  private List<String> splitStatements(String sql) {
    List<String> statements = new ArrayList<>();
    boolean quoteEsacpe = false;
    int index = 0;
    for (int i = 0; i < sql.length(); ++i) {
      if (sql.charAt(i) == SINGLE_QUOTE) {
        quoteEsacpe = !quoteEsacpe;
      }
      if (sql.charAt(i) == STATEMENT_DELIMITER && !quoteEsacpe) {
        String stmt = sql.substring(index, i + 1).trim();
        // Statements with only ';' character are empty and dropped.
        if (stmt.length() > 1) {
          statements.add(stmt);
        }
        index = i + 1;
      }
    }

    if (index < sql.length()) {
      statements.add(sql.substring(index, sql.length()).trim());
    }
    return statements;
  }

  protected List<String> parseStatements(String sql) {
    Preconditions.checkNotNull(sql);
    List<String> statements = splitStatements(sql);
    return statements;
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
  public Integer getUpdateCount() {
    return this.updateCount;
  }

  public void addUpdateCount(int count) {
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

  public Statement getStatement() {
    return this.statement;
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
    return this.sql;
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
   *
   * @throws SQLException If an issue occurred in extracting result metadata.
   */
  protected void updateResultCount() throws SQLException {
    if (this.containsResultSet()) {
      this.statementResult = this.statement.getResultSet();
      this.hasMoreData = this.statementResult.next();
    } else {
      this.updateCount = this.statement.getUpdateCount();
      this.hasMoreData = false;
      this.statementResult = null;
    }
  }

  protected void updateBatchResultCount(int[] updateCounts) throws SQLException {
    this.updateCount = 0;
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
  protected void handleExecutionException(SQLException e) {
    this.exception = e;
    this.hasMoreData = false;
    this.statementResult = null;
  }

  /** Execute the SQL statement, storing metadata. */
  public void execute() {
    this.executed = true;
    int[] updateCounts = null;
    try {
      if (statements.size() > 1) {
        for (String stmt : statements) {
          this.statement.addBatch(stmt);
        }
        updateCounts = this.statement.executeBatch();
        this.updateBatchResultCount(updateCounts);
      } else {
        this.statement.execute(this.sql);
        this.updateResultCount();
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

  public enum ResultType {
    UPDATE_COUNT,
    RESULT_SET,
    NO_RESULT
  }
}
