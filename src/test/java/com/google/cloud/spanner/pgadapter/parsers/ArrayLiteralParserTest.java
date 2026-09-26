// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.parsers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArrayLiteralParserTest {

  @Test
  public void testEmptyArray() {
    assertEquals(Collections.emptyList(), ArrayLiteralParser.readArrayLiteral("{}"));
    assertEquals(Collections.emptyList(), ArrayLiteralParser.readArrayLiteral("{   }"));
    assertEquals(Collections.emptyList(), ArrayLiteralParser.readArrayLiteral("  {}  "));
  }

  @Test
  public void testSingleElement() {
    assertEquals(ImmutableList.of("1"), ArrayLiteralParser.readArrayLiteral("{1}"));
    assertEquals(ImmutableList.of("foo"), ArrayLiteralParser.readArrayLiteral("{\"foo\"}"));
    assertEquals(ImmutableList.of(""), ArrayLiteralParser.readArrayLiteral("{\"\"}"));
    assertEquals(ImmutableList.of(" "), ArrayLiteralParser.readArrayLiteral("{\" \"}"));
  }

  @Test
  public void testMultipleElements() {
    assertEquals(ImmutableList.of("1", "2", "3"), ArrayLiteralParser.readArrayLiteral("{1, 2, 3}"));
    assertEquals(ImmutableList.of("1", "2", "3"), ArrayLiteralParser.readArrayLiteral("{1,2,3}"));
    assertEquals(
        ImmutableList.of("foo", "bar", "baz"),
        ArrayLiteralParser.readArrayLiteral("{\"foo\", \"bar\", \"baz\"}"));
    assertEquals(
        ImmutableList.of("foo", "bar", "baz"),
        ArrayLiteralParser.readArrayLiteral("{foo, bar, baz}"));
  }

  @Test
  public void testWhitespaceHandling() {
    assertEquals(
        ImmutableList.of("1", "2", "3"), ArrayLiteralParser.readArrayLiteral("{ 1 , 2 , 3 }"));
    assertEquals(
        ImmutableList.of("foo 1", "bar 2"),
        ArrayLiteralParser.readArrayLiteral("{ foo 1 , bar 2 }"));
    assertEquals(
        ImmutableList.of(" foo ", " bar "),
        ArrayLiteralParser.readArrayLiteral("{ \" foo \" , \" bar \" }"));
    assertEquals(
        ImmutableList.of("1", "2"), ArrayLiteralParser.readArrayLiteral("{\t1\r,\u000B2\f}"));
  }

  @Test
  public void testAdjacentQuotesPreserved() {
    // Adjacent quotes inside quoted elements must NOT be collapsed (e.g. empty strings in JSON).
    assertEquals(
        ImmutableList.of("{\"k\":\"\"}"),
        ArrayLiteralParser.readArrayLiteral("{\"{\\\"k\\\":\\\"\\\"}\"}"));
    assertEquals(
        ImmutableList.of("\"foo\"", "\"\""),
        ArrayLiteralParser.readArrayLiteral("{\"\\\"foo\\\"\", \"\\\"\\\"\"}"));
  }

  @Test
  public void testCommentsPreservedInUnquoted() {
    // Unquoted elements must not be tokenized as SQL comments.
    assertEquals(ImmutableList.of("--foo"), ArrayLiteralParser.readArrayLiteral("{--foo}"));
    assertEquals(
        ImmutableList.of("a", "--", "b"), ArrayLiteralParser.readArrayLiteral("{a, --, b}"));
    assertEquals(
        ImmutableList.of("/*comment*/"), ArrayLiteralParser.readArrayLiteral("{/*comment*/}"));
    assertEquals(
        ImmutableList.of("a", "/*comment*/", "b"),
        ArrayLiteralParser.readArrayLiteral("{a, /*comment*/, b}"));
  }

  @Test
  public void testEscapesAndWindowsPaths() {
    // PostgreSQL array syntax escapes \c to c; backslash does not trigger Java unicode unescaping.
    assertEquals(
        ImmutableList.of("C:\\user\\test"),
        ArrayLiteralParser.readArrayLiteral("{\"C:\\\\user\\\\test\"}"));
    assertEquals(ImmutableList.of("C:\\user"), ArrayLiteralParser.readArrayLiteral("{C:\\\\user}"));
    assertEquals(
        ImmutableList.of("a\\n", "b\\u1234"),
        ArrayLiteralParser.readArrayLiteral("{\"a\\\\n\", \"b\\\\u1234\"}"));
  }

  @Test
  public void testEscapedDelimiters() {
    assertEquals(ImmutableList.of(",", "}"), ArrayLiteralParser.readArrayLiteral("{\\,, \\}}"));
    assertEquals(ImmutableList.of("{", "}"), ArrayLiteralParser.readArrayLiteral("{\\{, \\}}"));
    assertEquals(
        ImmutableList.of(",", "}"), ArrayLiteralParser.readArrayLiteral("{\"\\,\", \"\\}\"}"));
  }

  @Test
  public void testNullHandling() {
    assertEquals(
        Arrays.asList("foo", "null", null, null, null, "null"),
        ArrayLiteralParser.readArrayLiteral("{\"foo\", \\null, null, NULL, Null, \"null\"}"));
    assertEquals(Collections.singletonList(null), ArrayLiteralParser.readArrayLiteral("{null}"));
    assertEquals(ImmutableList.of("null"), ArrayLiteralParser.readArrayLiteral("{\\null}"));
    assertEquals(ImmutableList.of("null"), ArrayLiteralParser.readArrayLiteral("{\"null\"}"));
    assertEquals(ImmutableList.of("nullify"), ArrayLiteralParser.readArrayLiteral("{nullify}"));
    assertEquals(ImmutableList.of("notnull"), ArrayLiteralParser.readArrayLiteral("{notnull}"));
  }

  @Test
  public void testDelimitersAndBracesInQuotedElements() {
    assertEquals(
        ImmutableList.of("foo,bar", "baz"),
        ArrayLiteralParser.readArrayLiteral("{\"foo,bar\", \"baz\"}"));
    assertEquals(
        ImmutableList.of("{foo}", "}bar{"),
        ArrayLiteralParser.readArrayLiteral("{\"\\{foo\\}\", \"\\}bar\\{\"}"));
    assertEquals(ImmutableList.of("{", "}"), ArrayLiteralParser.readArrayLiteral("{\"{\", \"}\"}"));
  }

  @Test
  public void testEscapedQuotesInUnquotedElements() {
    // In PostgreSQL, \" in unquoted element unescapes to literal "
    assertEquals(ImmutableList.of("foo\"bar"), ArrayLiteralParser.readArrayLiteral("{foo\\\"bar}"));
  }

  @Test
  public void testEscapedSpaces() {
    assertEquals(ImmutableList.of(" "), ArrayLiteralParser.readArrayLiteral("{\\  }"));
    assertEquals(ImmutableList.of("    "), ArrayLiteralParser.readArrayLiteral("{\\   \\  }"));
    assertEquals(ImmutableList.of(" a "), ArrayLiteralParser.readArrayLiteral("{\\ a\\  }"));
  }

  @Test
  public void testUnquotedByteaHex() {
    assertEquals(
        ImmutableList.of("\\x1234", "\\x5678"),
        ArrayLiteralParser.readArrayLiteral("{\\\\x1234, \\\\x5678}"));
  }

  @Test
  public void testDimensionDecorations() {
    assertEquals(ImmutableList.of("1", "2"), ArrayLiteralParser.readArrayLiteral("[1:2]={1, 2}"));
    assertEquals(
        ImmutableList.of("1", "2", "3"), ArrayLiteralParser.readArrayLiteral("[3]={1, 2, 3}"));
    assertEquals(
        ImmutableList.of("1", "2", "3", "4", "5"),
        ArrayLiteralParser.readArrayLiteral("[-2:2]={1, 2, 3, 4, 5}"));
    assertEquals(
        ImmutableList.of("1", "2"), ArrayLiteralParser.readArrayLiteral("  [1:2] = {1, 2}  "));
  }

  @Test
  public void testAdditionalFeatures() {
    assertEquals(
        Arrays.asList("1 year", null), ArrayLiteralParser.readArrayLiteral("{1 year, NULL}"));
    assertEquals(
        ImmutableList.of("2026-09-25 18:30:00+02"),
        ArrayLiteralParser.readArrayLiteral("{2026-09-25 18:30:00+02}"));
    assertEquals(
        ImmutableList.of("{\"k\": [1, 2]}"),
        ArrayLiteralParser.readArrayLiteral("{\"{\\\"k\\\": [1, 2]}\"}"));
  }

  @Test
  public void testUnicodeWhitespacePreserved() {
    // Unicode spaces (e.g. \u3000 CJK space) are not ASCII whitespace and must be preserved as
    // unquoted element content according to PostgreSQL arrayfuncs.c.
    assertEquals(ImmutableList.of("foo\u3000"), ArrayLiteralParser.readArrayLiteral("{foo\u3000}"));
    assertEquals(ImmutableList.of("\u3000"), ArrayLiteralParser.readArrayLiteral("{\u3000}"));
    assertEquals(ImmutableList.of("\u3000foo"), ArrayLiteralParser.readArrayLiteral("{\u3000foo}"));
  }

  @Test
  public void testUnquotedFourCharacterWords() {
    assertEquals(
        ImmutableList.of("word", "true", "test", "abcd"),
        ArrayLiteralParser.readArrayLiteral("{word, true, test, abcd}"));
  }

  @Test
  public void testUnquotedSqlTokens() {
    // Array unquoted elements are not SQL tokens; apostrophes, parentheses, and dollar signs must
    // be
    // preserved.
    assertEquals(
        ImmutableList.of("O'Reilly", "it's"),
        ArrayLiteralParser.readArrayLiteral("{O'Reilly, it's}"));
    assertEquals(
        ImmutableList.of("foo(bar", "baz)qux"),
        ArrayLiteralParser.readArrayLiteral("{foo(bar, baz)qux}"));
    assertEquals(
        ImmutableList.of("$tag$foo", "$$bar"),
        ArrayLiteralParser.readArrayLiteral("{$tag$foo, $$bar}"));
    assertEquals(
        ImmutableList.of("null-1", "null:2"),
        ArrayLiteralParser.readArrayLiteral("{null-1, null:2}"));
  }

  @Test
  public void testQuotedInvalidJavaUnicodeEscapes() {
    // In PostgreSQL array grammar, \c unescapes to c; non-Java escape sequences like \\uZZZZ must
    // not crash with IllegalArgumentException.
    assertEquals(
        ImmutableList.of("\\uZZZZ"), ArrayLiteralParser.readArrayLiteral("{\"\\\\uZZZZ\"}"));
    assertEquals(
        ImmutableList.of("C:\\users\\admin"),
        ArrayLiteralParser.readArrayLiteral("{\"C:\\\\users\\\\admin\"}"));
    assertEquals(ImmutableList.of("\\u12"), ArrayLiteralParser.readArrayLiteral("{\"\\\\u12\"}"));
  }

  @Test
  public void testUnquotedWithInternalSpacesAndEscapes() {
    assertEquals(
        ImmutableList.of("foo bar baz"), ArrayLiteralParser.readArrayLiteral("{foo bar\\ baz   }"));
  }

  @Test
  public void testErrorConditions() {
    assertInvalidParameter(null);
    assertInvalidParameter("");
    assertInvalidParameter("   ");
    assertInvalidParameter("1, 2");
    assertInvalidParameter("{1, 2");
    assertInvalidParameter("1, 2}");
    assertInvalidParameter("{");
    assertInvalidParameter("{ ");
    assertInvalidParameter("{1, 2} extra token");

    // Missing elements or unclosed delimiters
    assertInvalidParameter("{1,}");
    assertInvalidParameter("{,1}");
    assertInvalidParameter("{1,,2}");
    assertInvalidParameter("{,}");
    assertInvalidParameter("{1,");

    // Misquoted elements
    assertInvalidParameter("{\"foo\"bar}");
    assertInvalidParameter("{foo\"bar}");
    assertInvalidParameter("{\"foo}");
    assertInvalidParameter("{\"foo\\");
    assertInvalidParameter("{\"foo\\nbar");
    assertInvalidParameter("{\"foo\"");
    assertInvalidParameter("{\"foo\\\"bar\"baz}");
    assertInvalidParameter("{\"foo\\\"bar");

    // Unquoted elements with invalid characters or unclosed states
    assertInvalidParameter("{foo{bar}");
    assertInvalidParameter("{foo\\bar\\");
    assertInvalidParameter("{foo\\bar\"baz}");
    assertInvalidParameter("{foo\\bar{baz}");
    assertInvalidParameter("{foo\\bar");

    // Multidimensional arrays not supported
    assertInvalidParameter("{{1, 2}, {3, 4}}");

    // Malformed dimension headers
    assertInvalidParameter("[1:2{1, 2}");
    assertInvalidParameter("[1:2] {1, 2}");
    assertInvalidParameter("[1:2]=");
    assertInvalidParameter("[1:2]");
    assertInvalidParameter("[1:2] 123");
    assertInvalidParameter("[1:2][1:2]={{1, 2}, {3, 4}}");
    assertInvalidParameter("[1:2] [1:2]={1, 2}");
    assertInvalidParameter("[]={1}");
    assertInvalidParameter("[abc]={1}");
    assertInvalidParameter("[2:1]={1}");
    assertInvalidParameter("[1:2:3]={1}");
    assertInvalidParameter("[-1]={1}");
    assertInvalidParameter("[0]={1}");
  }

  private static void assertInvalidParameter(String expression) {
    PGException exception =
        assertThrows(PGException.class, () -> ArrayLiteralParser.readArrayLiteral(expression));
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());
  }
}
