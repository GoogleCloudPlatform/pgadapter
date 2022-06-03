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

import static com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.TRANSACTION_ABORTED_ERROR;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerBatchUpdateException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.connection.TransactionMode;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Future;

/** This class emulates a backend PostgreSQL connection. */
@InternalApi
public class BackendConnection {
  public enum ConnectionState {
    IDLE(Status.IDLE),
    TRANSACTION(Status.TRANSACTION),
    ABORTED(Status.FAILED);

    private final ReadyResponse.Status readyResponseStatus;

    ConnectionState(ReadyResponse.Status readyResponseStatus) {
      this.readyResponseStatus = readyResponseStatus;
    }

    public ReadyResponse.Status getReadyResponseStatus() {
      return this.readyResponseStatus;
    }
  }

  enum TransactionMode {
    IMPLICIT,
    EXPLICIT,
  }

  abstract class BufferedStatement<T> {
    final ParsedStatement parsedStatement;
    final Statement statement;
    final SettableFuture<T> result;

    BufferedStatement(ParsedStatement parsedStatement, Statement statement) {
      this.parsedStatement = parsedStatement;
      this.statement = statement;
      this.result = SettableFuture.create();
    }

    abstract void execute();

    void checkConnectionState() {
      // Only COMMIT or ROLLBACK is allowed if we are in an ABORTED transaction.
      if (connectionState == ConnectionState.ABORTED
          && !(isCommit(parsedStatement) || isRollback(parsedStatement))) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, TRANSACTION_ABORTED_ERROR);
      }
    }
  }

  private final class ExecuteQuery extends BufferedStatement<ResultSet> {
    ExecuteQuery(ParsedStatement parsedStatement, Statement statement) {
      super(parsedStatement, statement);
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        result.set(spannerConnection.executeQuery(statement));
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private final class AnalyzeQuery extends BufferedStatement<ResultSet> {
    AnalyzeQuery(ParsedStatement parsedStatement, Statement statement) {
      super(parsedStatement, statement);
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        result.set(spannerConnection.analyzeQuery(statement, QueryAnalyzeMode.PLAN));
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private final class Execute extends BufferedStatement<StatementResult> {
    Execute(ParsedStatement parsedStatement, Statement statement) {
      super(parsedStatement, statement);
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        if (connectionState == ConnectionState.ABORTED
            && !spannerConnection.isInTransaction()
            && (isRollback(parsedStatement) || isCommit(parsedStatement))) {
          result.set(rollbackResult);
        } else {
          result.set(spannerConnection.execute(statement));
        }
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private static final Statement ROLLBACK = Statement.of("ROLLBACK");
  private StatementResult rollbackResult;
  private ConnectionState connectionState = ConnectionState.IDLE;
  private TransactionMode transactionMode = TransactionMode.IMPLICIT;
  private final LinkedList<BufferedStatement<?>> bufferedStatements = new LinkedList<>();
  private final Connection spannerConnection;

  BackendConnection(Connection spannerConnection) {
    this.spannerConnection = spannerConnection;
  }

  /** Returns the current connection state. */
  public ConnectionState getConnectionState() {
    return this.connectionState;
  }

  public Future<ResultSet> executeQuery(ParsedStatement parsedStatement, Statement statement) {
    ExecuteQuery executeQuery = new ExecuteQuery(parsedStatement, statement);
    bufferedStatements.add(executeQuery);
    return executeQuery.result;
  }

  public Future<ResultSet> analyzeQuery(ParsedStatement parsedStatement, Statement statement) {
    AnalyzeQuery analyzeQuery = new AnalyzeQuery(parsedStatement, statement);
    bufferedStatements.add(analyzeQuery);
    return analyzeQuery.result;
  }

  public Future<StatementResult> execute(ParsedStatement parsedStatement, Statement statement) {
    Execute execute = new Execute(parsedStatement, statement);
    bufferedStatements.add(execute);
    return execute.result;
  }

  void flush() {
    flush(false);
  }

  private void flush(boolean isSync) {
    int index = 0;
    try {
      for (BufferedStatement<?> bufferedStatement : bufferedStatements) {
        maybeBeginImplicitTransaction(index, isSync);
        boolean canUseBatch = false;
        if (index < (getStatementCount() - 1)) {
          StatementType statementType = getStatementType(index);
          StatementType nextStatementType = getStatementType(index + 1);
          canUseBatch = canBeBatchedTogether(statementType, nextStatementType);
        }

        if (canUseBatch) {
          index += executeStatementsInBatch(index);
        } else {
          bufferedStatement.execute();

          if (isBegin(index)) {
            transactionMode = TransactionMode.EXPLICIT;
            connectionState = ConnectionState.TRANSACTION;
          } else if (isCommit(index) || isRollback(index)) {
            transactionMode = TransactionMode.IMPLICIT;
            connectionState = ConnectionState.IDLE;
          }
          index++;
        }
      }
    } catch (Exception exception) {
      connectionState = ConnectionState.ABORTED;
      if (spannerConnection.isInTransaction()) {
        rollbackResult = spannerConnection.execute(ROLLBACK);
      }
    } finally {
      bufferedStatements.clear();
    }
  }

  void sync() {
    try {
      flush(true);
    } finally {
      endImplicitTransaction();
    }
  }

  private void maybeBeginImplicitTransaction(int index, boolean isSync) {
    if (connectionState != ConnectionState.IDLE) {
      return;
    }

    // Only start an implicit transaction if we have more than one statement left. Otherwise, just
    // let the Spanner connection execute the statement in auto-commit mode.
    if (isSync && index == bufferedStatements.size() - 1) {
      return;
    }
    // Don't start an implicit transaction if this is already a transaction statement.
    if (isTransactionStatement(index)) {
      return;
    }
    // No need to start a transaction for DDL or client side statements.
    if (bufferedStatements.get(index).parsedStatement.getType() == StatementType.DDL
        || bufferedStatements.get(index).parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      return;
    }
    // If there are only DML statements left, those can be executed as an auto-commit dml batch.
    if (isSync && hasOnlyDmlStatementsAfter(index)) {
      return;
    }

    // We need to start an implicit transaction.
    // Check if a read-only transaction suffices.
    spannerConnection.beginTransaction();
    if (isSync && !hasDmlStatementsAfter(index)) {
      spannerConnection.setTransactionMode(
          com.google.cloud.spanner.connection.TransactionMode.READ_ONLY_TRANSACTION);
    }
    transactionMode = TransactionMode.IMPLICIT;
    connectionState = ConnectionState.TRANSACTION;
  }

  private void endImplicitTransaction() {
    // Only touch the transaction if it is an implicit transaction.
    if (transactionMode != TransactionMode.IMPLICIT) {
      return;
    }

    try {
      if (spannerConnection.isInTransaction()) {
        if (connectionState == ConnectionState.ABORTED) {
          spannerConnection.rollback();
        } else {
          spannerConnection.commit();
        }
      }
    } finally {
      connectionState = ConnectionState.IDLE;
    }
  }

  private boolean isBegin(int index) {
    return isBegin(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isBegin(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.BEGIN;
  }

  private boolean isCommit(int index) {
    return isCommit(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isCommit(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.COMMIT;
  }

  private boolean isRollback(int index) {
    return isRollback(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isRollback(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.ROLLBACK;
  }

  private boolean isTransactionStatement(int index) {
    return isBegin(index) || isCommit(index) || isRollback(index);
  }

  private boolean hasDmlStatementsAfter(int index) {
    return bufferedStatements.subList(index, bufferedStatements.size()).stream()
        .anyMatch(statement -> statement.parsedStatement.getType() == StatementType.UPDATE);
  }

  private boolean hasOnlyDmlStatementsAfter(int index) {
    return bufferedStatements.subList(index, bufferedStatements.size()).stream()
        .allMatch(statement -> statement.parsedStatement.getType() == StatementType.UPDATE);
  }

  private int getStatementCount() {
    return bufferedStatements.size();
  }

  private StatementType getStatementType(int index) {
    return bufferedStatements.get(index).parsedStatement.getType();
  }

  private boolean canBeBatchedTogether(StatementType statementType1, StatementType statementType2) {
    if (Objects.equals(statementType1, StatementType.QUERY)
        || Objects.equals(statementType1, StatementType.CLIENT_SIDE)) {
      return false;
    }
    return Objects.equals(statementType1, statementType2);
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
        spannerConnection.startBatchDml();
        break;
      case DDL:
        spannerConnection.startBatchDdl();
        break;
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Statement type is not supported for batching");
    }
    int index = fromIndex;
    while (index < getStatementCount()) {
      StatementType statementType = getStatementType(index);
      if (canBeBatchedTogether(batchType, statementType)) {
        spannerConnection.execute(bufferedStatements.get(index).statement);
        index++;
      } else {
        // End the batch here, as the statement type on this index can not be batched together with
        // the other statements in the batch.
        break;
      }
    }
    try {
      long[] counts = spannerConnection.runBatch();
      updateBatchResultCount(fromIndex, counts);
    } catch (SpannerBatchUpdateException e) {
      long[] counts = e.getUpdateCounts();
      updateBatchResultCount(fromIndex, counts);
      return counts.length + 1;
    }
    return index - fromIndex;
  }

  private void updateBatchResultCount(int fromIndex, long[] updateCounts) {
    for (int index = fromIndex; index < fromIndex + updateCounts.length; index++) {
      Execute execute = (Execute) bufferedStatements.get(index);
      if (execute.parsedStatement.getType() == StatementType.DDL) {
        execute.result.set(new NoResult());
      } else {
        execute.result.set(new UpdateCount(updateCounts[index - fromIndex]));
      }
    }
  }

  private static final class NoResult implements StatementResult {
    @Override
    public ResultType getResultType() {
      return ResultType.NO_RESULT;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateCount() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class UpdateCount implements StatementResult {
    private final Long updateCount;

    UpdateCount(Long updateCount) {
      this.updateCount = updateCount;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.UPDATE_COUNT;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateCount() {
      return updateCount;
    }
  }
}
