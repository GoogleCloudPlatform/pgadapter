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
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.common.collect.ImmutableList;

@InternalApi
public class DjangoSpecificStatement implements LocalStatement {
  public static final DjangoSpecificStatement INSTANCE = new DjangoSpecificStatement();

  private DjangoSpecificStatement() {}

  @Override
  public String[] getSql() {
    return new String[] {
      "\n"
          + "            SELECT\n"
          + "                c.relname,\n"
          + "                CASE\n"
          + "                    WHEN c.relispartition THEN 'p'\n"
          + "                    WHEN c.relkind IN ('m', 'v') THEN 'v'\n"
          + "                    ELSE 't'\n"
          + "                END\n"
          + "            FROM pg_catalog.pg_class c\n"
          + "            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
          + "            WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v')\n"
          + "                AND n.nspname NOT IN ('pg_catalog', 'pg_toast')\n"
          + "                AND pg_catalog.pg_table_is_visible(c.oid)\n"
          + "        "
    };
  }

  @Override
  public StatementResult execute(BackendConnection backendConnection) {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("relname", Type.string())), ImmutableList.of());

    return new QueryResult(resultSet);
  }
}
