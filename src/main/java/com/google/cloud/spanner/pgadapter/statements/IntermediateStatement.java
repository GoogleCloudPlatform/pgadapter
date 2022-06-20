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

import static com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage.COPY;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.PostgreSQLStatementParser;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
@InternalApi
public class IntermediateStatement {
  public enum ResultNotReadyBehavior {
    FAIL,
    BLOCK;
  }

  protected static final PostgreSQLStatementParser PARSER =
      (PostgreSQLStatementParser) AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  protected final OptionsMetadata options;
  protected StatementResult statementResult;
  protected boolean hasMoreData;
  protected Future<StatementResult> futureStatementResult;
  protected SpannerException exception;
  protected final ParsedStatement parsedStatement;
  protected final Statement originalStatement;
  protected final String command;
  protected String commandTag;
  protected boolean executed;
  protected final Connection connection;
  protected final ConnectionHandler connectionHandler;

  public IntermediateStatement(
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      Statement originalStatement,
      ConnectionHandler connectionHandler) {
    this(connectionHandler, options, parsedStatement, originalStatement);
  }

  protected IntermediateStatement(
      ConnectionHandler connectionHandler,
      OptionsMetadata options,
      ParsedStatement parsedStatement,
      Statement originalStatement) {
    this.connectionHandler = connectionHandler;
    this.options = options;
    ParsedStatement potentiallyReplacedStatement =
        SimpleQueryStatement.replaceKnownUnsupportedQueries(
            this.connectionHandler.getWellKnownClient(), this.options, parsedStatement);
    // Check if we need to create a new 'original' statement. The original statement is what will be
    // sent to Cloud Spanner, as the statement might include query hints in comments.
    if (potentiallyReplacedStatement == parsedStatement) {
      this.originalStatement = originalStatement;
    } else {
      this.originalStatement = Statement.of(potentiallyReplacedStatement.getSqlWithoutComments());
    }
    this.parsedStatement = potentiallyReplacedStatement;
    this.connection = connectionHandler.getSpannerConnection();
    this.command = StatementParser.parseCommand(this.parsedStatement.getSqlWithoutComments());
    this.commandTag = this.command;
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
    if (statementResult != null && statementResult.getResultType() == ResultType.RESULT_SET) {
      statementResult.getResultSet().close();
      statementResult = null;
    }
  }

  /** @return True if this is a select statement, false otherwise. */
  public boolean containsResultSet() {
    return this.parsedStatement.isQuery();
  }

  /** @return True if this statement was executed, False otherwise. */
  public boolean isExecuted() {
    return executed;
  }

  /**
   * @return The number of items that were modified by this execution for DML. 0 for DDL and -1 for
   *     QUERY.
   */
  public long getUpdateCount() {
    return getUpdateCount(ResultNotReadyBehavior.FAIL);
  }

  public long getUpdateCount(ResultNotReadyBehavior resultNotReadyBehavior) {
    initFutureResult(resultNotReadyBehavior);
    switch (this.parsedStatement.getType()) {
      case QUERY:
        return -1L;
      case UPDATE:
        return this.statementResult.getUpdateCount();
      case CLIENT_SIDE:
      case DDL:
      case UNKNOWN:
        if (StatementParser.isCommand(COPY, parsedStatement.getSqlWithoutComments())) {
          return this.statementResult.getUpdateCount();
        }
      default:
        return 0L;
    }
  }

  /** @return True if at some point in execution, and exception was thrown. */
  public boolean hasException() {
    return hasException(ResultNotReadyBehavior.FAIL);
  }

  boolean hasException(ResultNotReadyBehavior resultNotReadyBehavior) {
    initFutureResult(resultNotReadyBehavior);
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

  public String getStatement() {
    return this.parsedStatement.getSqlWithoutComments();
  }

  private void initFutureResult(ResultNotReadyBehavior resultNotReadyBehavior) {
    if (this.futureStatementResult != null) {
      if (resultNotReadyBehavior == ResultNotReadyBehavior.FAIL && !this.futureStatementResult.isDone()) {
        throw new IllegalStateException("Statement result cannot be retrieved before flush/sync");
      }
      try {
        setStatementResult(this.futureStatementResult.get());
      } catch (ExecutionException executionException) {
        this.exception = SpannerExceptionFactory.asSpannerException(executionException.getCause());
      } catch (InterruptedException interruptedException) {
        this.exception = SpannerExceptionFactory.propagateInterrupt(interruptedException);
      } finally {
        this.futureStatementResult = null;
      }
    }
  }

  public StatementResult getStatementResult() {
    return getStatementResult(ResultNotReadyBehavior.FAIL);
  }

  StatementResult getStatementResult(ResultNotReadyBehavior resultNotReadyBehavior) {
    initFutureResult(resultNotReadyBehavior);
    return this.statementResult;
  }

  public void setStatementResult(StatementResult statementResult) {
    this.statementResult = statementResult;
    if (statementResult.getResultType() == ResultType.RESULT_SET) {
      this.hasMoreData = statementResult.getResultSet().next();
    } else if (statementResult instanceof NoResult
        && ((NoResult) statementResult).hasCommandTag()) {
      this.commandTag = ((NoResult) statementResult).getCommandTag();
    }
  }

  protected void setFutureStatementResult(Future<StatementResult> result) {
    this.futureStatementResult = result;
  }

  public StatementType getStatementType() {
    return this.parsedStatement.getType();
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
   * Clean up and save metadata when an exception occurs.
   *
   * @param exception The exception to store.
   */
  protected void handleExecutionException(SpannerException exception) {
    this.exception = exception;
    this.hasMoreData = false;
  }

  public void executeAsync(BackendConnection backendConnection) {
    throw new UnsupportedOperationException();
  }

  /**
   * Moreso meant for inherited classes, allows one to call describe on a statement. Since raw
   * statements cannot be described, throw an error.
   */
  public DescribeMetadata<?> describe() {
    throw new IllegalStateException(
        "Cannot describe a simple statement " + "(only prepared statements and portals)");
  }

  public Future<? extends DescribeMetadata<?>> describeAsync(BackendConnection backendConnection) {
    throw new UnsupportedOperationException();
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

  /** @return the extracted command (first word) from the really executed SQL statement. */
  public String getCommandTag() {
    return this.commandTag;
  }
}
