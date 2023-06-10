// Copyright 2023 Google LLC
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

import com.google.common.collect.ImmutableList;

/** Translates Cloud Spanner PostgreSQL DDL statements into PostgreSQL compatible DDL statements. */
public class DdlTranslator {
  /**
   * Translates a Cloud Spanner PostgreSQL DDL statement into a compatible PostgreSQL DDL statement.
   * This means that the following Cloud Spanner extensions are stripped:
   *
   * <ol>
   *   <li>CREATE {TABLE|INDEX} ... INTERLEAVE IN PARENT
   *   <li>CREATE TABLE ... TTL INTERVAL
   *   <li>CREATE VIEW ... SQL SECURITY INVOKER
   *   <li>CREATE CHANGE STREAM
   * </ol>
   */
  public static String translate(String statement) {
    SimpleParser parser = new SimpleParser(statement);
    // We only translate CREATE statements.
    if (!parser.eatKeyword("create")) {
      return statement;
    }
    if (parser.eatKeyword("table")) {
      return translateCreateTable(statement);
    } else if (parser.eatKeyword("index")) {
      return translateCreateIndex(statement);
    } else if (parser.eatKeyword("change", "stream")) {
      return translateCreateChangeStream(statement);
    } else if (parser.eatKeyword("view") || parser.eatKeyword("or", "replace", "view")) {
      return translateCreateView(statement);
    }
    return statement;
  }

  static String translateCreateTable(String statement) {
    SimpleParser parser = new SimpleParser(statement);
    String validDdl =
        parser.parseExpressionUntilKeyword(
            ImmutableList.of("interleave", "ttl"), true, false, false);
    if (parser.hasMoreTokens()) {
      return validDdl + " /* " + parser.getSql().substring(parser.getPos()) + " */";
    }
    return parser.getSql();
  }

  static String translateCreateIndex(String statement) {
    SimpleParser parser = new SimpleParser(statement);
    parser.parseExpressionUntilKeyword(ImmutableList.of("interleave"), true, false, false);
    int startPos = parser.getPos();
    if (parser.eatKeyword("interleave", "in")) {
      if (parser.readTableOrIndexName() != null) {
        return parser.getSql().substring(0, startPos)
            + " /*"
            + parser.getSql().substring(startPos, parser.getPos())
            + " */"
            + parser.getSql().substring(parser.getPos());
      }
    }
    return parser.getSql();
  }

  static String translateCreateView(String statement) {
    SimpleParser parser = new SimpleParser(statement);
    if (!parser.eatKeyword("create")) {
      return statement;
    }
    if (parser.eatKeyword("or")) {
      if (!parser.eatKeyword("replace")) {
        return statement;
      }
    }
    if (!parser.eatKeyword("view")) {
      return statement;
    }
    if (parser.readTableOrIndexName() == null) {
      return statement;
    }
    int startPos = parser.getPos();
    if (parser.eatKeyword("sql", "security", "invoker")) {
      return parser.getSql().substring(0, startPos)
          + " /*"
          + parser.getSql().substring(startPos, parser.getPos())
          + " */"
          + parser.getSql().substring(parser.getPos());
    }

    return statement;
  }

  static String translateCreateChangeStream(String statement) {
    return "/* " + statement + " */";
  }
}
