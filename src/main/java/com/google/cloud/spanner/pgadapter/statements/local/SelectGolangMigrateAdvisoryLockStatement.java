// Copyright 2024 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.ClientSideResultSet;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.TypeCode;

@InternalApi
public class SelectGolangMigrateAdvisoryLockStatement implements LocalStatement {
  public static final SelectGolangMigrateAdvisoryLockStatement INSTANCE =
      new SelectGolangMigrateAdvisoryLockStatement();

  private SelectGolangMigrateAdvisoryLockStatement() {}

  @Override
  public String[] getSql() {
    return new String[] {
      "SELECT pg_advisory_lock($1)",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    ResultSet resultSet =
        ClientSideResultSet.forRows(
            Type.struct(StructField.of("pg_advisory_lock", Type.int64())),
            ImmutableList.of(Struct.newBuilder().set("pg_advisory_lock").to(1L).build()),
            ResultSetMetadata.newBuilder()
                .setUndeclaredParameters(
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setName("p1")
                                .setType(
                                    com.google.spanner.v1.Type.newBuilder()
                                        .setCode(TypeCode.INT64)
                                        .build())
                                .build())
                        .build())
                .build());
    return new QueryResult(resultSet);
  }
}
