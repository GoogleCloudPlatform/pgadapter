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

package com.google.cloud.spanner.pgadapter.statements.local;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.common.collect.ImmutableList;

@InternalApi
public class TempTest implements LocalStatement {
  public static final TempTest INSTANCE =
      new TempTest();

  private TempTest() {}

  @Override
  public String[] getSql() {
    return new String[] {
        "select current_database()",
        "SELECT current_database()",
        "Select current_database()",
        "select CURRENT_DATABASE()",
        "SELECT CURRENT_DATABASE()",
        "Select CURRENT_DATABASE()",
        "select * from current_database()",
        "SELECT * FROM current_database()",
        "Select * FROM current_database()",
        "select * from CURRENT_DATABASE()",
        "SELECT * FROM CURRENT_DATABASE()",
        "Select * FROM CURRENT_DATABASE()",
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("current_database", Type.string())),
            ImmutableList.of(
                Struct.newBuilder()
                    .set("current_database")
                    .to(backendConnection.getCurrentDatabase())
                    .build()));
    return new QueryResult(resultSet);
  }
}
