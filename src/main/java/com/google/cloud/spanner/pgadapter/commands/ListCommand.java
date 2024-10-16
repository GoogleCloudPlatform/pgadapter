// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.pgadapter.commands;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.statements.local.ListDatabasesStatement;
import java.util.regex.Pattern;

/** This command is equivalent to the PSQL \l and \l <param> meta-commands. */
@InternalApi
public class ListCommand extends Command {

  private static final Pattern INPUT_REGEX =
      Pattern.compile(
          "^SELECT\\s+d\\.datname as \"Name\",\n"
              + "\\s*pg_catalog\\.pg_get_userbyid\\(d.datdba\\) as \"Owner\",\n"
              + ".*"
              + "\\s*pg_catalog\\.array_to_string\\(d\\.datacl, .*\\)(:? END)? AS \"Access privileges\"\n"
              + "FROM (?:pg_catalog\\.)?pg_database d\n.*\n?"
              + "ORDER BY 1;?$",
          Pattern.DOTALL);

  ListCommand(String sql) {
    super(sql);
  }

  @Override
  public Pattern getPattern() {
    return INPUT_REGEX;
  }

  @Override
  public String translate() {
    return ListDatabasesStatement.LIST_DATABASES_SQL;
  }
}
