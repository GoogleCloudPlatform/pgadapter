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

package com.google.cloud.spanner.pgadapter.statements;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class LiquibaseStatementHelper {
  static final String TAG_STATEMENT_PART = "databasechangelog t SET TAG=";
  private static final Pattern TAG_STATEMENT_PATTERN =
      Pattern.compile(
          "UPDATE (?<schema>.+\\.)?databasechangelog t "
              + "SET TAG='(?<tag>.*)' "
              + "FROM \\(SELECT DATEEXECUTED, ORDEREXECUTED "
              + "FROM (?<schema2>.+\\.)?databasechangelog "
              + "ORDER BY DATEEXECUTED DESC, ORDEREXECUTED DESC LIMIT 1\\) sub "
              + "WHERE t\\.DATEEXECUTED=sub\\.DATEEXECUTED AND t\\.ORDEREXECUTED=sub\\.ORDEREXECUTED");

  static final String MD5SUM_NOT_LIKE = " MD5SUM IS NOT NULL AND MD5SUM NOT LIKE '9:%'";
  static final String MD5SUM_NOT_LIKE_REPLACEMENT = " MD5SUM IS NOT NULL AND NOT MD5SUM LIKE '9:%'";

  static String replaceTagStatement(String input) {
    Matcher matcher = TAG_STATEMENT_PATTERN.matcher(input);
    if (matcher.matches()) {
      return matcher.replaceFirst(
          "UPDATE ${schema}databasechangelog t "
              + "SET TAG='${tag}' "
              + "WHERE ID=("
              + "  SELECT ID "
              + "  FROM ${schema}databasechangelog "
              + "  ORDER BY DATEEXECUTED DESC, ORDEREXECUTED DESC "
              + "  LIMIT 1)");
    }
    return input;
  }
}
