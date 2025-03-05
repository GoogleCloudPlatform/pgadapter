// Copyright 2025 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.EscapeClauseParser.removeDefaultEscapeClauses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.cloud.spanner.Statement;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LikeEscapeTest {

  @Test
  public void testRemoveEscapeClauses() {
    assertSameAfterRemoveEscape("select 1");
    assertSameAfterRemoveEscape("select col1, col2 from my_table where bar like 'one'");
    assertSameAfterRemoveEscape("update my_table set bar=1 where foo like 'two'");
    assertSameAfterRemoveEscape("select col1 from my_table");
    assertSameAfterRemoveEscape(
        "select col1 from foo where test=1 /* and test like 'one' escape '\\' */");
    assertSameAfterRemoveEscape("select col1, col2 from my_table where bar like 'one' escape '*'");

    assertEquals(
        Statement.of("select col1 from foo where bar like 'test'"),
        internalRemoveEscape("select col1 from foo where bar like 'test' escape '\\'"));
    assertEquals(
        Statement.of("select col1 from foo where bar like 'test' and baz like 'test' escape '*'"),
        internalRemoveEscape(
            "select col1 from foo where bar like 'test' escape '\\' and baz like 'test' escape '*'"));
    assertEquals(
        Statement.of("select col1 from foo where bar like 'test' and baz like 'test'"),
        internalRemoveEscape(
            "select col1 from foo where bar like 'test' escape '\\' and baz like 'test' escape '\\'"));
    assertEquals(
        Statement.of("select col1 from foo where bar like $1"),
        internalRemoveEscape("select col1 from foo where bar like $1 escape '\\'"));
    assertEquals(
        Statement.of(
            "select col1 from foo where bar in (select val from t where id like $1) and id=1"),
        internalRemoveEscape(
            "select col1 from foo where bar in (select val from t where id like $1 escape '\\') and id=1"));
    assertEquals(
        Statement.of("select col1 from foo where bar like 'test' and baz like 'test'"),
        internalRemoveEscape(
            "select col1 from foo where bar like 'test' escape '' and baz like 'test' escape ''"));
  }

  private void assertSameAfterRemoveEscape(String sql) {
    Statement statement = Statement.of(sql);
    assertSame(
        statement,
        removeDefaultEscapeClauses(statement, statement.getSql().toLowerCase(Locale.ENGLISH)));
  }

  private Statement internalRemoveEscape(String sql) {
    return removeDefaultEscapeClauses(Statement.of(sql), sql.toLowerCase(Locale.ENGLISH));
  }

//  @Test
//  public void testRemoveEscapeWithEmptyString() {
//    String inputSql = "SELECT * FROM users WHERE name LIKE 'test%' ESCAPE '//' AND city LIKE 'NY%' ESCAPE '//'";
//    String expectedSql = "SELECT * FROM users WHERE name LIKE 'test%' AND city LIKE 'NY%'";
//
//    // Apply escape removal logic
//    Statement modifiedStatement = removeDefaultEscapeClauses(Statement.of(inputSql));
//
//    // Debug output
//    System.out.println("Input SQL: " + inputSql);
//    System.out.println("Modified SQL: " + modifiedStatement.getSql());
//
//    // Assertion: ESCAPE '' should be removed
//    assertEquals(expectedSql, modifiedStatement.getSql(), "ESCAPE '' should be removed from the query");
//  }
}
