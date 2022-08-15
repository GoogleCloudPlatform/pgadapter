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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class PgCatalog {
  private static final Map<TableOrIndexName, TableOrIndexName> TABLE_REPLACEMENTS =
      ImmutableMap.of(
          new TableOrIndexName("pg_catalog", "pg_type"), new TableOrIndexName(null, "pg_type"));

  public static Statement replacePgCatalogTables(Statement statement) {
    return new TableParser(statement).replaceTables(TABLE_REPLACEMENTS);
  }
}
