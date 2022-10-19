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

import static com.google.cloud.spanner.pgadapter.statements.CopyStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.Format;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.ParsedCopyStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopyStatementTest {

  @Test
  public void testParseTableName() {
    ParsedCopyStatement statement = parse("copy my_table from stdin");

    assertEquals(new TableOrIndexName(null, "my_table"), statement.table);
    assertNull(statement.columns);
    assertNull(statement.query);
  }

  @Test
  public void testParseColumnNames() {
    ParsedCopyStatement statement = parse("copy my_table (col1, col2) from stdin");

    assertEquals(new TableOrIndexName(null, "my_table"), statement.table);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        statement.columns);

    PGException exception;
    exception = assertThrows(PGException.class, () -> parse("copy my_table col1, col2 from stdin"));
    assertEquals(
        "missing 'FROM' or 'TO' keyword: copy my_table col1, col2 from stdin",
        exception.getMessage());
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table (col1, col2 from stdin"));
    assertEquals("missing closing parentheses for columns column list", exception.getMessage());
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table (col1, 'col2') from stdin"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseQuery() {
    assertEquals(
        "select * from my_table where my_column=1",
        parse("copy (select * from my_table where my_column=1) from stdin").query);
    assertEquals(
        "(select * from my_table where my_column in (select 1))",
        parse("copy ((select * from my_table where my_column in (select 1))) from stdin").query);

    PGException exception;
    exception =
        assertThrows(PGException.class, () -> parse("copy (select foo col1, col2 from stdin"));
    assertEquals("missing closing parentheses after query", exception.getMessage());
  }

  @Test
  public void testParseFormat() {
    assertEquals(Format.CSV, parse("copy my_table from stdin (format csv)").format);
    assertEquals(Format.TEXT, parse("copy my_table from stdin (format text)").format);
    assertEquals(Format.BINARY, parse("copy my_table from stdin (format binary)").format);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (format foo)"));
    assertEquals("Invalid format option: format foo", exception.getMessage());
  }

  @Test
  public void testParseFreeze() {
    assertTrue(parse("copy my_table from stdin (freeze)").freeze);
    assertTrue(parse("copy my_table from stdin (freeze true)").freeze);
    assertTrue(parse("copy my_table from stdin (freeze on)").freeze);
    assertFalse(parse("copy my_table from stdin (freeze f)").freeze);
    assertFalse(parse("copy my_table from stdin (freeze off)").freeze);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (freeze foo)"));
    assertEquals("foo is not a valid boolean value", exception.getMessage());
  }

  @Test
  public void testParseDelimiter() {
    assertEquals(',', parse("copy my_table from stdin (delimiter ',')").delimiter.charValue());
    assertEquals('\t', parse("copy my_table from stdin (delimiter e'\\t')").delimiter.charValue());
    assertEquals('\\', parse("copy my_table from stdin (delimiter e'\\\\')").delimiter.charValue());
    assertEquals('\b', parse("copy my_table from stdin (delimiter e'\\b')").delimiter.charValue());
    assertEquals('\f', parse("copy my_table from stdin (delimiter e'\\f')").delimiter.charValue());
    assertEquals('\n', parse("copy my_table from stdin (delimiter e'\\n')").delimiter.charValue());
    assertEquals('\r', parse("copy my_table from stdin (delimiter e'\\r')").delimiter.charValue());
    assertEquals('\1', parse("copy my_table from stdin (delimiter e'\\1')").delimiter.charValue());
    assertEquals(
        '\14', parse("copy my_table from stdin (delimiter e'\\14')").delimiter.charValue());
    assertEquals(
        '\111', parse("copy my_table from stdin (delimiter e'\\111')").delimiter.charValue());
    assertEquals(
        '\u0011', parse("copy my_table from stdin (delimiter e'\\u0011')").delimiter.charValue());

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (delimiter '\\t')"));
    assertEquals("COPY delimiter must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to create a delimiter with a special character, like a tab.\nExample: copy my_table to stdout (delimiter e'\\t')",
        exception.getHints());

    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (delimiter e'\\x7')"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseHeader() {
    assertTrue(parse("copy my_table from stdin (header)").header);
    assertTrue(parse("copy my_table from stdin (header true)").header);
    assertTrue(parse("copy my_table from stdin (header t)").header);
    assertTrue(parse("copy my_table from stdin (header on)").header);

    assertFalse(parse("copy my_table from stdin (header false)").header);
    assertFalse(parse("copy my_table from stdin (header f)").header);
    assertFalse(parse("copy my_table from stdin (header off)").header);

    assertTrue(parse("copy my_table from stdin (header match)").header);
    assertTrue(parse("copy my_table from stdin (header match)").headerMatch);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (header foo)"));
    assertEquals("foo is not a valid boolean value", exception.getMessage());
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (header t foo)"));
    assertEquals("Syntax error. Unexpected tokens: foo", exception.getMessage());
  }

  @Test
  public void testParseQuote() {
    assertEquals(',', parse("copy my_table from stdin (quote ',')").quote.charValue());
    assertEquals('\t', parse("copy my_table from stdin (quote e'\\t')").quote.charValue());
    assertEquals('\\', parse("copy my_table from stdin (quote e'\\\\')").quote.charValue());
    assertEquals('\b', parse("copy my_table from stdin (quote e'\\b')").quote.charValue());
    assertEquals('\f', parse("copy my_table from stdin (quote e'\\f')").quote.charValue());
    assertEquals('\n', parse("copy my_table from stdin (quote e'\\n')").quote.charValue());
    assertEquals('\r', parse("copy my_table from stdin (quote e'\\r')").quote.charValue());
    assertEquals('\1', parse("copy my_table from stdin (quote e'\\1')").quote.charValue());
    assertEquals('\14', parse("copy my_table from stdin (quote e'\\14')").quote.charValue());
    assertEquals('\111', parse("copy my_table from stdin (quote e'\\111')").quote.charValue());
    assertEquals('\u0011', parse("copy my_table from stdin (quote e'\\u0011')").quote.charValue());

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (quote '\\t')"));
    assertEquals("COPY quote must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to specify a special quote character, for example using an octal value.\n"
            + "Example: copy my_table to stdout (quote e'\\4', format csv)",
        exception.getHints());

    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (quote e'\\x7')"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseEscape() {
    assertEquals(',', parse("copy my_table from stdin (escape ',')").escape.charValue());
    assertEquals('\t', parse("copy my_table from stdin (escape e'\\t')").escape.charValue());
    assertEquals('\\', parse("copy my_table from stdin (escape e'\\\\')").escape.charValue());
    assertEquals('\b', parse("copy my_table from stdin (escape e'\\b')").escape.charValue());
    assertEquals('\f', parse("copy my_table from stdin (escape e'\\f')").escape.charValue());
    assertEquals('\n', parse("copy my_table from stdin (escape e'\\n')").escape.charValue());
    assertEquals('\r', parse("copy my_table from stdin (escape e'\\r')").escape.charValue());
    assertEquals('\1', parse("copy my_table from stdin (escape e'\\1')").escape.charValue());
    assertEquals('\14', parse("copy my_table from stdin (escape e'\\14')").escape.charValue());
    assertEquals('\111', parse("copy my_table from stdin (escape e'\\111')").escape.charValue());
    assertEquals(
        '\u0011', parse("copy my_table from stdin (escape e'\\u0011')").escape.charValue());

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (escape '\\t')"));
    assertEquals("COPY escape must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to specify a special escape character, for example using an octal value.\n"
            + "Example: copy my_table to stdout (escape e'\\4', format csv)",
        exception.getHints());

    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (escape e'\\x7')"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseForceQuote() {
    assertEquals(ImmutableList.of(), parse("copy my_table from stdin (force_quote *)").forceQuote);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table from stdin (force_quote (col1))").forceQuote);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table from stdin (force_quote (col1, col2))").forceQuote);

    PGException exception;
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (force_quote)"));
    assertEquals("missing opening parentheses for force_quote", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (force_quote * (col1))"));
    assertEquals("Syntax error. Unexpected tokens: (col1)", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (force_quote (col1, 'col2'))"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseForceNotNull() {
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table from stdin (force_not_null (col1))").forceNotNull);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table from stdin (force_not_null (col1, col2))").forceNotNull);

    PGException exception;
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (force_not_null)"));
    assertEquals("missing opening parentheses for force_not_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (force_not_null * (col1))"));
    assertEquals("missing opening parentheses for force_not_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (force_not_null (col1, 'col2'))"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseForceNull() {
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table from stdin (force_null (col1))").forceNull);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table from stdin (force_null (col1, col2))").forceNull);

    PGException exception;
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (force_null)"));
    assertEquals("missing opening parentheses for force_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (force_null * (col1))"));
    assertEquals("missing opening parentheses for force_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (force_null (col1, 'col2'))"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseEncoding() {
    assertEquals("UTF8", parse("copy my_table from stdin (encoding 'UTF8')").encoding);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (encoding foo)"));
    assertEquals("Invalid quote character: f", exception.getMessage());
  }
}
