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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import com.google.common.util.concurrent.SettableFuture;
import java.util.LinkedList;
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

  public Future<StatementResult> execute(ParsedStatement parsedStatement, Statement statement) {
    Execute execute = new Execute(parsedStatement, statement);
    bufferedStatements.add(execute);
    return execute.result;
  }

  void flush() {
    int index = 0;
    try {
      for (BufferedStatement<?> bufferedStatement : bufferedStatements) {
        maybeBeginImplicitTransaction(index);
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
      flush();
    } finally {
      endImplicitTransaction();
    }
  }

  private void maybeBeginImplicitTransaction(int index) {
    // Only start an implicit transaction if we have more than one statement left. Otherwise, just
    // let the Spanner connection execute the statement in auto-commit mode.
    if (bufferedStatements.size() > index + 1 && !isTransactionStatement(index)) {
      if (connectionState == ConnectionState.IDLE) {
        spannerConnection.beginTransaction();
        transactionMode = TransactionMode.IMPLICIT;
        connectionState = ConnectionState.TRANSACTION;
      }
    }
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
}
