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

import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.replaceForUpdate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SelectForUpdateTest {

  @Test
  public void testRemoveForUpdate() {
    assertSameAfterRemoveForUpdate("select 1");
    assertSameAfterRemoveForUpdate("select col1, col2 from my_table where bar=1");
    assertSameAfterRemoveForUpdate("update my_table set bar=1 where foo=2");
    assertSameAfterRemoveForUpdate("select col1 from my_table for share");
    assertSameAfterRemoveForUpdate("select col1 from foo for update skip locked");
    assertSameAfterRemoveForUpdate("select col1 from foo for update nowait");
    assertSameAfterRemoveForUpdate("select col1 from foo for no key update");
    assertSameAfterRemoveForUpdate("select col1 from my_table for update of my_other_table");

    assertEquals(
        Statement.of("/*@ LOCK_SCANNED_RANGES=exclusive */select col1 from foo"),
        internalReplaceForUpdate("select col1 from foo for update"));
    assertEquals(
        Statement.of("/*@ LOCK_SCANNED_RANGES=exclusive */SELECT col1 FROM foo"),
        internalReplaceForUpdate("SELECT col1 FROM foo for update"));
    assertEquals(
        Statement.of(
            "/*@ LOCK_SCANNED_RANGES=exclusive */select col1 from foo /* this is a comment */"),
        internalReplaceForUpdate("select col1 from foo for update /* this is a comment */"));
    assertEquals(
        Statement.of(
            "/*@ LOCK_SCANNED_RANGES=exclusive */select col1 from foo /* this is a comment */ -- yet another comment\n"),
        internalReplaceForUpdate(
            "select col1 from foo for update /* this is a comment */ -- yet another comment\n"));
  }

  private void assertSameAfterRemoveForUpdate(String sql) {
    Statement statement = Statement.of(sql);
    assertSame(
        statement, replaceForUpdate(statement, statement.getSql().toLowerCase(Locale.ENGLISH)));
  }

  private Statement internalReplaceForUpdate(String sql) {
    return replaceForUpdate(Statement.of(sql), sql.toLowerCase(Locale.ENGLISH));
  }

  @Test
  public void testDetectSelectForUpdate() {
    assertEquals(" for update", parseForClause("select * from foo for update"));
    assertEquals(" for update", parseForClause("select bar, baz, 1 from foo for update"));
    assertEquals(" for update", parseForClause("select * from (select 1 from test) t for update"));
    assertEquals(
        " for update", parseForClause("select * from (select 1 from test for share) t for update"));
  }

  private String parseForClause(String sql) {
    SimpleParser parser = new SimpleParser(sql);
    parser.parseExpressionUntilKeyword(ImmutableList.of("for"), true, false, false);
    return parser.getSql().substring(parser.getPos());
  }
}
