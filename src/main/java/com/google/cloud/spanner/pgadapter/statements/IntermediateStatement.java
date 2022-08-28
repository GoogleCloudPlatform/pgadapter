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

import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.parseCommand;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
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
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.wireoutput.DataRowResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.WireOutput;
import java.io.DataOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
@InternalApi
public class IntermediateStatement {
  private static final WireOutput[] EMPTY_WIRE_OUTPUT_ARRAY = new WireOutput[0];

  /**
   * Indicates whether an attempt to get the result of a statement should block or fail if the
   * result is not yet available. Normal SQL commands that can be executed directly on Cloud Spanner
   * should always have their results available when a sync/flush message is received. COPY
   * statements do not have that, as they require additional messages after a flush/sync has been
   * received. Attempts to get the result of a COPY statement should therefore block until it is
   * available, which is after a CopyDone or CopyFail message has been received.
   */
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
  protected final DataOutputStream outputStream;

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
    this.command = parseCommand(this.parsedStatement.getSqlWithoutComments());
    this.commandTag = this.command;
    this.outputStream = connectionHandler.getConnectionMetadata().peekOutputStream();
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
   *     QUERY. Fails if the result is not yet available.
   */
  public long getUpdateCount() {
    return getUpdateCount(ResultNotReadyBehavior.FAIL);
  }

  /**
   * @return The number of items that were modified by this execution for DML. 0 for DDL and -1 for
   *     QUERY. Will block or fail depending on the given {@link ResultNotReadyBehavior} if the
   *     result is not yet available.
   */
  public long getUpdateCount(ResultNotReadyBehavior resultNotReadyBehavior) {
    initFutureResult(resultNotReadyBehavior);
    if (hasException()) {
      throw getException();
    }
    // Note: getStatementType() returns UPDATE for COPY statements.
    switch (getStatementType()) {
      case QUERY:
        return -1L;
      case UPDATE:
        return this.statementResult.getUpdateCount();
      case CLIENT_SIDE:
      case DDL:
      case UNKNOWN:
      default:
        return 0L;
    }
  }

  /**
   * @return True if at some point in execution an exception was thrown. Fails if execution has not
   *     yet finished.
   */
  public boolean hasException() {
    return hasException(ResultNotReadyBehavior.FAIL);
  }

  /**
   * @return True if at some point in execution an exception was thrown. Fails or blocks depending
   *     on the given {@link ResultNotReadyBehavior} if execution has not yet finished.
   */
  public boolean hasException(ResultNotReadyBehavior resultNotReadyBehavior) {
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
      if (resultNotReadyBehavior == ResultNotReadyBehavior.FAIL
          && !this.futureStatementResult.isDone()) {
        throw new IllegalStateException("Statement result cannot be retrieved before flush/sync");
      }
      try {
        setStatementResult(this.futureStatementResult.get());
      } catch (ExecutionException executionException) {
        setException(SpannerExceptionFactory.asSpannerException(executionException.getCause()));
      } catch (InterruptedException interruptedException) {
        setException(SpannerExceptionFactory.propagateInterrupt(interruptedException));
      } finally {
        this.futureStatementResult = null;
      }
    }
  }

  /**
   * Returns the result of this statement as a {@link StatementResult}. Fails if the result is not
   * yet available.
   */
  public StatementResult getStatementResult() {
    initFutureResult(ResultNotReadyBehavior.FAIL);
    return this.statementResult;
  }

  public void setStatementResult(StatementResult statementResult) {
    this.statementResult = statementResult;
    if (statementResult != null) {
      if (statementResult.getResultType() == ResultType.RESULT_SET) {
        this.hasMoreData = statementResult.getResultSet().next();
      } else if (statementResult instanceof NoResult
          && ((NoResult) statementResult).hasCommandTag()) {
        this.commandTag = ((NoResult) statementResult).getCommandTag();
      }
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

  /** Returns any execution exception registered for this statement. */
  public SpannerException getException() {
    return this.exception;
  }

  void setException(SpannerException exception) {
    // Do not override any exception that has already been registered. COPY statements can receive
    // multiple errors as they execute asynchronously while receiving a stream of data from the
    // client. We always return the first exception that we encounter.
    if (this.exception == null) {
      this.exception = exception;
    }
  }

  /**
   * Clean up and save metadata when an exception occurs.
   *
   * @param exception The exception to store.
   */
  public void handleExecutionException(SpannerException exception) {
    setException(exception);
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

  public WireOutput[] createResultPrefix(ResultSet resultSet) {
    // This is a no-op for a normal query. COPY uses this to send a CopyOutResponse.
    // COPY table_name TO STDOUT BINARY also uses this to add the binary copy header.
    return EMPTY_WIRE_OUTPUT_ARRAY;
  }

  public WireOutput createDataRowResponse(ResultSet resultSet, QueryMode mode) {
    return new DataRowResponse(this.outputStream, this, resultSet, this.options, mode);
  }

  public WireOutput[] createResultSuffix() {
    // This is a no-op for a normal query. COPY uses this to send a CopyDoneResponse.
    // COPY table_name TO STDOUT BINARY also uses this to add the binary copy trailer.
    return EMPTY_WIRE_OUTPUT_ARRAY;
  }
}
