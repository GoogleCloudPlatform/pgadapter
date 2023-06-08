// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.statements.local;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;

/** Simple replacement for statements that start a repeatable-read transaction. */
public class StartTransactionIsolationLevelRepeatableRead implements LocalStatement {
  public static final StartTransactionIsolationLevelRepeatableRead INSTANCE =
      new StartTransactionIsolationLevelRepeatableRead();
  private static final Statement REPLACEMENT_STATEMENT = Statement.of("BEGIN");

  // This is the very specific command string that postgres_fdw uses.
  // https://github.com/postgres/postgres/blob/503b0556d96f2c8df6ed91c5a8cf11b23f37ce6d/contrib/postgres_fdw/connection.c#L742
  private static final String FDW_START_COMMAND =
      "START TRANSACTION ISOLATION LEVEL REPEATABLE READ";

  private StartTransactionIsolationLevelRepeatableRead() {}

  @Override
  public String[] getSql() {
    return new String[] {FDW_START_COMMAND};
  }

  @Override
  public boolean hasReplacementStatement() {
    return true;
  }

  @Override
  public Statement getReplacementStatement(Statement statement) {
    return REPLACEMENT_STATEMENT;
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    // This should not be possible.
    throw new UnsupportedOperationException();
  }
}
