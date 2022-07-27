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

package com.google.cloud.spanner.pgadapter.statements.local.liquibase;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;

/**
 * A no-op SET extra_float_digits implementation. This should be removed once support has been added
 * to the Connection API.
 */
// TODO: Remove this once extra_float_digits support has been added to the Connection API.
@InternalApi
public class SetExtraFloatDigitsStatement implements LocalStatement {
  public static final SetExtraFloatDigitsStatement INSTANCE = new SetExtraFloatDigitsStatement();

  private SetExtraFloatDigitsStatement() {}

  @Override
  public String[] getSql() {
    return new String[] {
      "SET extra_float_digits = 3",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    return new NoResult("SET");
  }
}
