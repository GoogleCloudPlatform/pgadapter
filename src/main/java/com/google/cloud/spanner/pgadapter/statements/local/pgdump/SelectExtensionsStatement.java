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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.common.collect.ImmutableList;

public class SelectExtensionsStatement implements LocalStatement {

  @Override
  public String[] getSql() {
    return new String[] {
      "SELECT x.tableoid, x.oid, x.extname, n.nspname, x.extrelocatable, x.extversion, x.extconfig, x.extcondition "
          + "FROM pg_extension x JOIN pg_namespace n ON n.oid = x.extnamespace",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(
                StructField.of("tableoid", Type.int64()),
                StructField.of("oid", Type.int64()),
                StructField.of("extname", Type.string()),
                StructField.of("nspname", Type.string()),
                StructField.of("extrelocatable", Type.bool()),
                StructField.of("extversion", Type.string()),
                StructField.of("extconfig", Type.string()),
                StructField.of("extcondition", Type.string())),
            ImmutableList.of());
    return new QueryResult(resultSet);
  }
}
