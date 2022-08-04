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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SimpleParserTest {

  @Test
  public void testEat() {
    assertTrue(new SimpleParser("insert into foo").eat("insert"));
    assertTrue(new SimpleParser("   insert   into foo").eat("insert"));
    assertTrue(new SimpleParser("\tinsert   foo").eat("insert"));

    assertFalse(new SimpleParser("inset into foo").eat("insert"));
    assertFalse(new SimpleParser("\"insert\" into foo").eat("insert"));

    assertTrue(new SimpleParser("insert into foo").eat("insert", "into"));
    assertTrue(new SimpleParser("   insert   into foo").eat("insert", "into"));

    assertFalse(new SimpleParser("\tinsert   foo").eat("insert", "into"));
    assertFalse(new SimpleParser("inset into foo").eat("insert", "into"));
    assertFalse(new SimpleParser("\"insert\" into foo").eat("insert", "into"));
  }

  @Test
  public void testReadTableOrIndexNamePart() {
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
  }

  @Test
  public void testReadTableOrIndexName() {
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
        "(foo('bar\\', test'))", new SimpleParser("  (foo('bar\\', test')),bar").parseExpression());
    assertEquals("''", new SimpleParser("''").parseExpression());
    assertEquals("'\\''", new SimpleParser("'\\''").parseExpression());
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
        Arrays.asList("(foo('bar\\', test'))", "bar"),
        new SimpleParser("  (foo('bar\\', test')),bar").parseExpressionList());
    assertEquals(Collections.singletonList("''"), new SimpleParser("''").parseExpressionList());
    assertEquals(
        Collections.singletonList("'\\''"), new SimpleParser("'\\''").parseExpressionList());
    assertEquals(Collections.singletonList("'\"'"), new SimpleParser("'\"'").parseExpressionList());

    assertNull(new SimpleParser("\"foo").parseExpressionList());
    assertNull(new SimpleParser("foo(").parseExpressionList());
    assertEquals(
        Collections.singletonList("foo(bar, test)"),
        new SimpleParser("foo(bar, test)) bar").parseExpressionList());
    assertNull(new SimpleParser("foo((bar, test) bar").parseExpressionList());
    assertEquals(Collections.singletonList("foo"), new SimpleParser("foo)(").parseExpressionList());
  }
}
