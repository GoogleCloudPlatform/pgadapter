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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * This command is equivalent to the PSQL \l and \l <param> meta-commands. The original expectation
 * would be to print database and database attributes, however As we do not have easy access to
 * those, we just print the current database name.
 */
public class ListCommand extends Command {

  private static final Pattern INPUT_REGEX = Pattern.compile("^SELECT d\\.datname as \"Name\",\n" +
      "       pg_catalog\\.pg_get_userbyid\\(d.datdba\\) as \"Owner\",\n" +
      "       pg_catalog\\.pg_encoding_to_char\\(d\\.encoding\\) as \"Encoding\",\n" +
      "       pg_catalog\\.array_to_string\\(d\\.datacl, '\\\\n'\\) AS \"Access privileges\"\n" +
      "FROM pg_catalog\\.pg_database d\n.*\n?" +
      "ORDER BY 1;$");

  private static final String OUTPUT_QUERY = "SELECT '%s' AS Name;";

  private final Connection connection;

  public ListCommand(String sql, Connection connection) {
    super(sql);
    this.connection = connection;
  }

  @Override
  public Pattern getPattern() {
    return INPUT_REGEX;
  }

  @Override
  public String translate() {
    try {
      return String.format(OUTPUT_QUERY, this.connection.getCatalog());
    } catch (SQLException e) {
      return String.format(OUTPUT_QUERY, "UNKNOWN");
    }
  }
}
