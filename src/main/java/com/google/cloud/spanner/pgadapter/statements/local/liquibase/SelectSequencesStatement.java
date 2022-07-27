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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.common.collect.ImmutableList;

@InternalApi
public class SelectSequencesStatement implements LocalStatement {
  public static final SelectSequencesStatement INSTANCE = new SelectSequencesStatement();

  private SelectSequencesStatement() {}

  @Override
  public String[] getSql() {
    return new String[] {
      "SELECT c.relname AS \"SEQUENCE_NAME\" FROM pg_class c join pg_namespace on c.relnamespace = pg_namespace.oid WHERE c.relkind='S' AND nspname = 'public' AND c.oid not in (select d.objid FROM pg_depend d where d.refobjsubid > 0)"
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("SEQUENCE_NAME", Type.string())), ImmutableList.of());
    return new QueryResult(resultSet);
  }
}
