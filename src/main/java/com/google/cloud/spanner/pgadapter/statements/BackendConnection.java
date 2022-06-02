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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.common.util.concurrent.SettableFuture;
import java.util.LinkedList;
import java.util.concurrent.Future;

@InternalApi
public class BackendConnection {
  abstract static class BufferedStatement<T> {
    final Statement statement;
    final SettableFuture<T> result;

    BufferedStatement(Statement statement) {
      this.statement = statement;
      this.result = SettableFuture.create();
    }

    abstract void execute();
  }

  private final class ExecuteQuery extends BufferedStatement<ResultSet> {
    ExecuteQuery(Statement statement) {
      super(statement);
    }

    @Override
    void execute() {
      try {
        result.set(spannerConnection.executeQuery(statement));
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private final LinkedList<BufferedStatement> bufferedStatements = new LinkedList<>();
  private final Connection spannerConnection;

  BackendConnection(Connection spannerConnection) {
    this.spannerConnection = spannerConnection;
  }

  public Future<ResultSet> executeQuery(Statement statement) {
    ExecuteQuery bufferedStatement = new ExecuteQuery(statement);
    bufferedStatements.add(bufferedStatement);
    return bufferedStatement.result;
  }

  void flush() {
    try {
      for (BufferedStatement<?> bufferedStatement : bufferedStatements) {
        bufferedStatement.execute();
      }
    } catch (Exception exception) {
      // TODO: Set connection in aborted mode
    }
  }

  void sync() {
    flush();
  }
}
