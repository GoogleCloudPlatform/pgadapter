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

import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.parseCommand;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SimpleParserTest {

  /** Helper methods to reuse existing tests */
  public static ImmutableList<String> splitStatements(String sql) {
    return new SimpleParser(sql).splitStatements();
  }

  public static int skipQuotedString(String sql, int startIndex) {
    SimpleParser parser = new SimpleParser(sql);
    parser.setPos(startIndex);
    parser.skipQuotedString();
    return parser.getPos();
  }

  public static int skipSingleLineComment(String sql, int startIndex) {
    SimpleParser parser = new SimpleParser(sql);
    parser.setPos(startIndex);
    parser.skipSingleLineComment();
    return parser.getPos();
  }

  public static int skipMultiLineComment(String sql, int startIndex) {
    SimpleParser parser = new SimpleParser(sql);
    parser.setPos(startIndex);
    parser.skipMultiLineComment();
    return parser.getPos();
  }

  public static int skipDollarQuotedString(String sql, int startIndex) {
    SimpleParser parser = new SimpleParser(sql);
    parser.setPos(startIndex);
    parser.skipDollarQuotedString();
    return parser.getPos();
  }

  @Test
  public void testEatKeyword() {
    assertTrue(new SimpleParser("insert").eatKeyword("insert"));
    assertTrue(new SimpleParser("insert into foo").eatKeyword("insert"));
    assertTrue(new SimpleParser("   insert   into foo").eatKeyword("insert"));
    assertTrue(new SimpleParser("\tinsert   foo").eatKeyword("insert"));

    assertFalse(new SimpleParser("inset into foo").eatKeyword("insert"));
    assertFalse(new SimpleParser("\"insert\" into foo").eatKeyword("insert"));

    assertTrue(new SimpleParser("insert into foo").eatKeyword("insert", "into"));
    assertTrue(new SimpleParser("   insert   into foo").eatKeyword("insert", "into"));

    assertFalse(new SimpleParser("\tinsert   foo").eatKeyword("insert", "into"));
    assertFalse(new SimpleParser("inset into foo").eatKeyword("insert", "into"));
    assertFalse(new SimpleParser("\"insert\" into foo").eatKeyword("insert", "into"));

    assertFalse(new SimpleParser("insertinto foo").eatKeyword("insert", "into"));
    assertFalse(new SimpleParser("insert intofoo").eatKeyword("insert", "into"));
    assertFalse(new SimpleParser("\"insert\"into foo").eatKeyword("insert", "into"));

    assertTrue(new SimpleParser("values (1, 2)").eatKeyword("values"));
    assertTrue(new SimpleParser("values(1, 2)").eatKeyword("values"));
    assertTrue(new SimpleParser("null)").eatKeyword("null"));

    assertTrue(new SimpleParser("select\"id\"from\"foo\"").eatKeyword("select"));
    assertTrue(new SimpleParser("select/*comment*/id from foo").eatKeyword("select"));
    assertFalse(new SimpleParser("select$$foo$$").eatKeyword("select"));
    assertFalse(new SimpleParser("'select").eatKeyword("select"));
  }

  @Test
  public void testEatToken() {
    assertTrue(new SimpleParser("(foo").eatToken("("));
    assertTrue(new SimpleParser("(").eatToken("("));
    assertTrue(new SimpleParser("( ").eatToken("("));

    assertTrue(new SimpleParser("\t(   foo").eatToken("("));
    assertFalse(new SimpleParser("foo(").eatToken("("));
    assertFalse(new SimpleParser("").eatToken("("));
  }

  @Test
  public void testDotOperator() {
    assertTrue(new SimpleParser(".foo").eatDotOperator());
    assertFalse(new SimpleParser(". foo").eatDotOperator());
    assertFalse(new SimpleParser(" .foo").eatDotOperator());
    assertFalse(new SimpleParser(".").eatDotOperator());
    assertFalse(new SimpleParser(". ").eatDotOperator());

    assertTrue(new SimpleParser("\t(   foo").eatToken("("));
    assertFalse(new SimpleParser("foo(").eatToken("("));
    assertFalse(new SimpleParser("").eatToken("("));
  }

  @Test
  public void testReadTableOrIndexNamePart() {
    assertEquals("\"foo\"", new SimpleParser("\"foo\"(id)").readIdentifierPart());

    assertEquals("foo", new SimpleParser("foo bar").readIdentifierPart());
    assertEquals("foo", new SimpleParser("foo").readIdentifierPart());
    assertEquals("\"foo\"", new SimpleParser("\"foo\" bar").readIdentifierPart());
    assertEquals("\"foo\"", new SimpleParser("\"foo\"").readIdentifierPart());
    assertEquals("foo", new SimpleParser(" foo bar").readIdentifierPart());
    assertEquals("foo", new SimpleParser("\tfoo").readIdentifierPart());
    assertEquals("\"foo\"", new SimpleParser("\n\"foo\" bar").readIdentifierPart());
    assertEquals("\"foo\"", new SimpleParser("    \"foo\"").readIdentifierPart());
    assertEquals("\"foo\"\"bar\"", new SimpleParser("\"foo\"\"bar\"").readIdentifierPart());
    assertEquals("foo", new SimpleParser("foo\"bar\"").readIdentifierPart());
    assertEquals("foo", new SimpleParser("foo.bar").readIdentifierPart());
    assertEquals("foo", new SimpleParser("foo").readIdentifierPart());
    assertEquals("\"foo\"", new SimpleParser("\"foo\".bar").readIdentifierPart());
    assertEquals("\"foo\"", new SimpleParser("\"foo\"").readIdentifierPart());
    assertNull(new SimpleParser("\"foo").readIdentifierPart());

    assertEquals("foo", new SimpleParser("foo) bar").readIdentifierPart());
    assertEquals("foo", new SimpleParser("foo- bar").readIdentifierPart());
    assertEquals("foo", new SimpleParser("foo/ bar").readIdentifierPart());
    assertEquals("foo$", new SimpleParser("foo$ bar").readIdentifierPart());
    assertEquals("f$oo", new SimpleParser("f$oo bar").readIdentifierPart());
    assertEquals("_foo", new SimpleParser("_foo bar").readIdentifierPart());
    assertEquals("øfoo", new SimpleParser("øfoo bar").readIdentifierPart());
    assertNull(new SimpleParser("\\foo").readIdentifierPart());
    assertNull(new SimpleParser("1foo").readIdentifierPart());
    assertNull(new SimpleParser("-foo").readIdentifierPart());
    assertNull(new SimpleParser("$foo").readIdentifierPart());
  }

  @Test
  public void testReadTableOrIndexName() {
    assertEquals(new TableOrIndexName("foo"), new SimpleParser("foo .bar").readTableOrIndexName());
    // The following is an invalid name, but the simple parser should just accept this and let the
    // backend return an error.
    assertEquals(
        new TableOrIndexName("foo", ""), new SimpleParser("foo. bar").readTableOrIndexName());
    assertNull(new SimpleParser(".bar").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("foo", ""), new SimpleParser("foo.\"bar").readTableOrIndexName());

    assertEquals(new TableOrIndexName("foo"), new SimpleParser("foo bar").readTableOrIndexName());
    assertEquals(new TableOrIndexName("foo"), new SimpleParser("foo").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\""), new SimpleParser("\"foo\" bar").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\""), new SimpleParser("\"foo\"").readTableOrIndexName());
    assertEquals(new TableOrIndexName("foo"), new SimpleParser(" foo bar").readTableOrIndexName());
    assertEquals(new TableOrIndexName("foo"), new SimpleParser("\tfoo").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\""), new SimpleParser("\n\"foo\" bar").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\""), new SimpleParser("    \"foo\"").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\"\"bar\""),
        new SimpleParser("\"foo\"\"bar\"").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("foo"), new SimpleParser("foo\"bar\"").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("foo", "bar"), new SimpleParser("foo.bar").readTableOrIndexName());
    assertEquals(new TableOrIndexName("foo"), new SimpleParser("foo").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\"", "bar"),
        new SimpleParser("\"foo\".bar").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\"", "\"bar\""),
        new SimpleParser("\"foo\".\"bar\"").readTableOrIndexName());
    assertEquals(
        new TableOrIndexName("\"foo\""), new SimpleParser("\"foo\"").readTableOrIndexName());
    assertNull(new SimpleParser("\"foo").readTableOrIndexName());
  }

  @Test
  public void testParseExpression() {
    assertEquals("foo bar", new SimpleParser("foo bar").parseExpression());
    assertEquals("foo", new SimpleParser("foo").parseExpression());
    assertEquals("\"foo\" bar", new SimpleParser("\"foo\" bar").parseExpression());
    assertEquals("\"foo\"", new SimpleParser("\"foo\"").parseExpression());
    assertEquals("foo bar", new SimpleParser(" foo bar").parseExpression());
    assertEquals("foo", new SimpleParser("\tfoo").parseExpression());
    assertEquals("\"foo\" bar", new SimpleParser("\n\"foo\" bar").parseExpression());
    assertEquals("\"foo\"", new SimpleParser("    \"foo\"").parseExpression());

    assertEquals("foo+bar", new SimpleParser("foo+bar").parseExpression());
    assertEquals("foo + bar", new SimpleParser("foo + bar").parseExpression());

    assertEquals("foo(bar)", new SimpleParser("foo(bar)").parseExpression());
    assertEquals("foo", new SimpleParser("foo, bar").parseExpression());
    assertEquals("\"foo,bar\"", new SimpleParser("\"foo,bar\", bar").parseExpression());
    assertEquals("\"foo\"", new SimpleParser("\"foo\"").parseExpression());
    assertEquals("foo bar", new SimpleParser(" foo bar").parseExpression());
    assertEquals("foo", new SimpleParser("\tfoo").parseExpression());
    assertEquals("\"foo\" bar", new SimpleParser("\n\"foo\" bar").parseExpression());
    assertEquals("\"foo\n\"", new SimpleParser("    \"foo\n\"").parseExpression());
    assertEquals("foo(bar, test)", new SimpleParser("foo(bar, test)").parseExpression());
    assertEquals("(foo(bar, test))", new SimpleParser("(foo(bar, test))  ").parseExpression());
    assertEquals("(foo(bar, test))", new SimpleParser("  (foo(bar, test)),bar").parseExpression());
    assertEquals("(foo(bar, test))", new SimpleParser("  (foo(bar, test)),bar").parseExpression());
    assertEquals(
        "(foo('bar, test'))", new SimpleParser("  (foo('bar, test')),bar").parseExpression());
    assertEquals(
        "(foo('bar\", test'))", new SimpleParser("  (foo('bar\", test')),bar").parseExpression());
    assertEquals(
        "(foo('bar'', test'))", new SimpleParser("  (foo('bar'', test')),bar").parseExpression());
    assertEquals("''", new SimpleParser("''").parseExpression());
    assertEquals("''''", new SimpleParser("''''").parseExpression());
    assertEquals("'\"'", new SimpleParser("'\"'").parseExpression());

    assertNull(new SimpleParser("\"foo").parseExpression());
    assertNull(new SimpleParser("foo(").parseExpression());
    assertEquals("foo(bar, test)", new SimpleParser("foo(bar, test)) bar").parseExpression());
    assertNull(new SimpleParser("foo((bar, test) bar").parseExpression());
    assertEquals("foo", new SimpleParser("foo)(").parseExpression());
  }

  @Test
  public void testParseExpressionList() {
    assertEquals(Arrays.asList("foo", "bar"), new SimpleParser("foo, bar").parseExpressionList());
    assertEquals(Collections.singletonList("foo"), new SimpleParser("foo").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\" bar"),
        new SimpleParser("\"foo\" bar").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\""), new SimpleParser("\"foo\"").parseExpressionList());
    assertEquals(
        Collections.singletonList("foo bar"), new SimpleParser(" foo bar").parseExpressionList());
    assertEquals(Collections.singletonList("foo"), new SimpleParser("\tfoo").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\" bar"),
        new SimpleParser("\n\"foo\" bar").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\""),
        new SimpleParser("    \"foo\"").parseExpressionList());

    assertEquals(
        Collections.singletonList("foo(bar)"), new SimpleParser("foo(bar)").parseExpressionList());
    assertEquals(Arrays.asList("foo", "bar"), new SimpleParser("foo, bar)").parseExpressionList());
    assertEquals(
        Arrays.asList("\"foo,bar\"", "bar"),
        new SimpleParser("\"foo,bar\", bar").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\""), new SimpleParser("\"foo\"").parseExpressionList());
    assertEquals(
        Collections.singletonList("foo bar"), new SimpleParser(" foo bar").parseExpressionList());
    assertEquals(Collections.singletonList("foo"), new SimpleParser("\tfoo").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\" bar"),
        new SimpleParser("\n\"foo\" bar").parseExpressionList());
    assertEquals(
        Collections.singletonList("\"foo\n\""),
        new SimpleParser("    \"foo\n\"").parseExpressionList());
    assertEquals(
        Collections.singletonList("foo(bar, test)"),
        new SimpleParser("foo(bar, test)").parseExpressionList());
    assertEquals(
        Collections.singletonList("(foo(bar, test))"),
        new SimpleParser("(foo(bar, test))  ").parseExpressionList());
    assertEquals(
        Arrays.asList("(foo(bar, test))", "bar"),
        new SimpleParser("  (foo(bar, test)),bar").parseExpressionList());
    assertEquals(
        Arrays.asList("(foo(bar, test))", "bar"),
        new SimpleParser("  (foo(bar, test)),bar").parseExpressionList());
    assertEquals(
        Arrays.asList("(foo('bar, test'))", "bar"),
        new SimpleParser("  (foo('bar, test')),bar").parseExpressionList());
    assertEquals(
        Arrays.asList("(foo('bar\", test'))", "bar"),
        new SimpleParser("  (foo('bar\", test')),bar").parseExpressionList());
    assertEquals(
        Arrays.asList("(foo('bar'', test'))", "bar"),
        new SimpleParser("  (foo('bar'', test')),bar").parseExpressionList());
    assertEquals(Collections.singletonList("''"), new SimpleParser("''").parseExpressionList());
    assertEquals(Collections.singletonList("''''"), new SimpleParser("''''").parseExpressionList());
    assertEquals(Collections.singletonList("'\"'"), new SimpleParser("'\"'").parseExpressionList());

    assertNull(new SimpleParser("\"foo").parseExpressionList());
    assertNull(new SimpleParser("foo(").parseExpressionList());
    assertEquals(
        Collections.singletonList("foo(bar, test)"),
        new SimpleParser("foo(bar, test)) bar").parseExpressionList());
    assertNull(new SimpleParser("foo((bar, test) bar").parseExpressionList());
    assertEquals(Collections.singletonList("foo"), new SimpleParser("foo)(").parseExpressionList());
  }

  @Test
  public void testParseExpressionUntil() {
    assertEquals(
        "insert into foo values ('select')",
        new SimpleParser("insert into foo values ('select')")
            .parseExpressionUntilKeyword(ImmutableList.of("select")));
    assertEquals(
        "insert into foo",
        new SimpleParser("insert into foo select * from bar")
            .parseExpressionUntilKeyword(ImmutableList.of("select")));
    assertEquals(
        "insert into foo values ('''select''')",
        new SimpleParser("insert into foo values ('''select''')")
            .parseExpressionUntilKeyword(ImmutableList.of("select")));
    assertEquals(
        "insert into foo (\"''\")",
        new SimpleParser("insert into foo (\"''\") select * from bar")
            .parseExpressionUntilKeyword(ImmutableList.of("select")));
    assertEquals(
        "insert into foo values ('''select''')",
        new SimpleParser("insert into foo values ('''select''') select 1")
            .parseExpressionUntilKeyword(ImmutableList.of("select")));
    assertEquals(
        "select \"insert\" from bar",
        new SimpleParser("select \"insert\" from bar")
            .parseExpressionUntilKeyword(ImmutableList.of("insert")));
    assertEquals(
        "select \"\"\"insert\"\"\" from bar",
        new SimpleParser("select \"\"\"insert\"\"\" from bar")
            .parseExpressionUntilKeyword(ImmutableList.of("insert")));
    assertEquals(
        "insert into foo (\"\"\"\")",
        new SimpleParser("insert into foo (\"\"\"\") select * from bar")
            .parseExpressionUntilKeyword(ImmutableList.of("select")));
  }

  @Test
  public void testParseCommand() {
    assertEquals("SELECT", parseCommand("select * from foo"));
    assertEquals("INSERT", parseCommand("insert into foo values (1, 'One')"));
    assertEquals("SELECT", parseCommand("/* this is a comment */ select * from foo"));
    assertEquals("CREATE", parseCommand("/* ddl statements are also supported */create table foo"));
    assertEquals(
        "UPDATE", parseCommand("with my_cte as (select * from foo) update bar set col1='one'"));
    assertEquals(
        "UPDATE",
        parseCommand(
            "with my_cte as (select * from foo), my_cte2 as (select * from bar) update bar set col1='one'"));
    assertEquals(
        "UPDATE",
        parseCommand(
            "with my_cte as (select * from foo), my_cte2 as (select * from bar) /* this is a comment*/update bar set col1='one'"));
    assertEquals(
        "UPDATE",
        parseCommand(
            "with my_cte as (select * from foo) -- also a comment\n, my_cte2 as (select * from bar) /* this is a comment*/update bar set col1='one'"));
    assertEquals(
        "UPSERT", parseCommand("with my_cte as (select * from foo) upsert bar set col1='one'"));
    assertEquals(
        "SELECT",
        parseCommand(
            "WITH temp (value) as (SELECT avg(id) FROM users) SELECT id FROM users, temp ORDER BY id"));
    assertEquals(
        "SELECT",
        parseCommand(
            "WITH temp (col1,/*comment*/col2, col3) as (SELECT avg(id) FROM users) SELECT id FROM users, temp ORDER BY id"));

    // The parser returns 'WITH' as the command tag if the CTE appears to be invalid.
    assertEquals(
        "WITH", parseCommand("with my_cte as (select * from foo update bar set col1='one'"));
    assertEquals(
        "WITH", parseCommand("with my_cte as (select * from foo), update bar set col1='one'"));
    assertEquals("WITH", parseCommand("with my_cte (select * from foo) update bar set col1='one'"));
    assertEquals(
        "WITH", parseCommand("with my_cte.bar as (select * from foo) update bar set col1='one'"));
    assertEquals("WITH", parseCommand("with (select * from foo update bar set col1='one'"));
    assertEquals("WITH", parseCommand("with 1 as (select * from foo update bar set col1='one'"));
    assertEquals(
        "WITH", parseCommand("with my_cte as select * from foo update bar set col1='one'"));

    assertEquals("WITH", parseCommand("with my_cte as (select * from foo)"));
    assertEquals("", parseCommand("/*only a comment*/"));
  }
}
