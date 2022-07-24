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

package com.google.cloud.spanner.pgadapter.statements.local.pgdump;

import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;

/** SET statements that are ignored specifically for pg_dump. */
public class NoOpSetStatement implements LocalStatement {

  @Override
  public String[] getSql() {
    return new String[] {
      "SET DATESTYLE = ISO",
      "SET INTERVALSTYLE = POSTGRES",
      "SET extra_float_digits TO 3",
      "SET synchronize_seqscans TO off",
      // TODO: Fix SET statement_timeout = 0 in the Connection API.
      "SET statement_timeout = 0",
      "SET lock_timeout = 0",
      "SET idle_in_transaction_session_timeout = 0",
      "SET row_security = off",
      "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    return new NoResult("SET");
  }
}
