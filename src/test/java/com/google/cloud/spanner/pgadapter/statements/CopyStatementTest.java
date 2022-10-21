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
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.ParsedCopyStatement.Direction;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopyStatementTest {

  @Test
  public void testParseDirection() {
    assertEquals(Direction.FROM, parse("copy my_table from stdin").direction);
    assertEquals(Direction.TO, parse("copy my_table to stdout").direction);
    assertThrows(PGException.class, () -> parse("copy my_table both stdin"));
  }

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
    assertEquals(
        ImmutableList.of(new TableOrIndexName("\"Col1\""), new TableOrIndexName("col2")),
        parse("copy tbl (\"Col1\", COL2) from stdin").columns);

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
        parse("copy (select * from my_table where my_column=1) to stdout").query);
    assertEquals(
        "(select * from my_table where my_column in (select 1))",
        parse("copy ((select * from my_table where my_column in (select 1))) to stdout").query);

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
    assertThrows(PGException.class, () -> parse("copy my_table from stdin (freeze)"));
    assertThrows(PGException.class, () -> parse("copy my_table from stdin (freeze true)"));
    assertThrows(PGException.class, () -> parse("copy my_table from stdin (freeze on)"));
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
  public void testParseNull() {
    assertEquals("\\N", parse("copy my_table from stdin (null '\\N')").nullString);
    assertEquals("\t", parse("copy my_table from stdin (null e'\\t')").nullString);
    assertEquals(
        "this_is_null", parse("copy my_table from stdin (null 'this_is_null')").nullString);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (null null)"));
    assertEquals("Invalid quote character: n", exception.getMessage());

    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (null e'\\x7')"));
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
    assertEquals(',', parse("copy my_table from stdin (format csv, quote ',')").quote.charValue());
    assertEquals(
        '\t', parse("copy my_table from stdin (format csv, quote e'\\t')").quote.charValue());
    assertEquals(
        '\'', parse("copy my_table from stdin (format csv, quote e'\\'')").quote.charValue());
    assertEquals(
        '\'', parse("copy my_table from stdin (format csv, quote '''')").quote.charValue());
    assertEquals(
        '\\', parse("copy my_table from stdin (format csv, quote e'\\\\')").quote.charValue());
    assertEquals(
        '\b', parse("copy my_table from stdin (format csv, quote e'\\b')").quote.charValue());
    assertEquals(
        '\f', parse("copy my_table from stdin (format csv, quote e'\\f')").quote.charValue());
    assertEquals(
        '\n', parse("copy my_table from stdin (format csv, quote e'\\n')").quote.charValue());
    assertEquals(
        '\r', parse("copy my_table from stdin (format csv, quote e'\\r')").quote.charValue());
    assertEquals(
        '\1', parse("copy my_table from stdin (format csv, quote e'\\1')").quote.charValue());
    assertEquals(
        '\14', parse("copy my_table from stdin (format csv, quote e'\\14')").quote.charValue());
    assertEquals(
        '\111', parse("copy my_table from stdin (format csv, quote e'\\111')").quote.charValue());
    assertEquals(
        '\u0011',
        parse("copy my_table from stdin (format csv, quote e'\\u0011')").quote.charValue());

    PGException exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (format csv, quote '\\t')"));
    assertEquals("COPY quote must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to specify a special quote character, for example using an octal value.\n"
            + "Example: copy my_table to stdout (quote e'\\4', format csv)",
        exception.getHints());

    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (format csv, quote e'\\x7')"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseEscape() {
    assertEquals(
        ',', parse("copy my_table from stdin (escape ',', format csv)").escape.charValue());
    assertEquals(
        '\t', parse("copy my_table from stdin (format csv, escape e'\\t')").escape.charValue());
    assertEquals(
        '\\', parse("copy my_table from stdin (format csv, escape e'\\\\')").escape.charValue());
    assertEquals(
        '\b', parse("copy my_table from stdin (format csv, escape e'\\b')").escape.charValue());
    assertEquals(
        '\f', parse("copy my_table from stdin (format csv, escape e'\\f')").escape.charValue());
    assertEquals(
        '\n', parse("copy my_table from stdin (format csv, escape e'\\n')").escape.charValue());
    assertEquals(
        '\r', parse("copy my_table from stdin (format csv, escape e'\\r')").escape.charValue());
    assertEquals(
        '\1', parse("copy my_table from stdin (format csv, escape e'\\1')").escape.charValue());
    assertEquals(
        '\14', parse("copy my_table from stdin (format csv, escape e'\\14')").escape.charValue());
    assertEquals(
        '\111', parse("copy my_table from stdin (format csv, escape e'\\111')").escape.charValue());
    assertEquals(
        '\u0011',
        parse("copy my_table from stdin (format csv, escape e'\\u0011')").escape.charValue());

    PGException exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (format csv, escape '\\t')"));
    assertEquals("COPY escape must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to specify a special escape character, for example using an octal value.\n"
            + "Example: copy my_table to stdout (escape e'\\4', format csv)",
        exception.getHints());

    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (format csv, escape e'\\x7')"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseForceQuote() {
    assertEquals(
        ImmutableList.of(),
        parse("copy my_table to stdout (format csv, force_quote *)").forceQuote);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table to stdout (format csv, force_quote (col1))").forceQuote);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table to stdout (format csv, force_quote (col1, col2))").forceQuote);

    PGException exception;
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table to stdout (format csv, force_quote)"));
    assertEquals("missing opening parentheses for force_quote", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table to stdout (format csv, force_quote * (col1))"));
    assertEquals("Syntax error. Unexpected tokens: (col1)", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table to stdout (format csv, force_quote (col1, 'col2'))"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseForceNotNull() {
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table from stdin (format csv, force_not_null (col1))").forceNotNull);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table from stdin (format csv, force_not_null (col1, col2))").forceNotNull);

    PGException exception;
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (format csv, force_not_null)"));
    assertEquals("missing opening parentheses for force_not_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (format csv, force_not_null * (col1))"));
    assertEquals("missing opening parentheses for force_not_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (format csv, force_not_null (col1, 'col2'))"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseForceNull() {
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table from stdin (format csv, force_null (col1))").forceNull);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table from stdin (format csv, force_null (col1, col2))").forceNull);

    PGException exception;
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin (format csv, force_null)"));
    assertEquals("missing opening parentheses for force_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (format csv, force_null * (col1))"));
    assertEquals("missing opening parentheses for force_null", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin (format csv, force_null (col1, 'col2'))"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseEncoding() {
    assertEquals("UTF8", parse("copy my_table from stdin (encoding 'UTF8')").encoding);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin (encoding foo)"));
    assertEquals("Invalid quote character: f", exception.getMessage());
  }

  @Test
  public void testParseLegacyBinary() {
    assertEquals(Format.BINARY, parse("copy my_table from stdin binary").format);
    assertEquals(Format.BINARY, parse("copy my_table from stdin with binary").format);
    assertEquals(
        Format.BINARY,
        parse(
                "copy \"all_types\" ( \"col_bigint\", \"col_bool\", \"col_bytea\", \"col_float8\", \"col_int\", \"col_numeric\", \"col_timestamptz\", \"col_date\", \"col_varchar\", \"col_jsonb\" ) from stdin binary")
            .format);
  }

  @Test
  public void testParseLegacyDelimiter() {
    assertEquals(',', parse("copy my_table from stdin delimiter ','").delimiter.charValue());
    assertEquals('\t', parse("copy my_table from stdin delimiter e'\\t'").delimiter.charValue());
    assertEquals('\\', parse("copy my_table from stdin delimiter e'\\\\'").delimiter.charValue());
    assertEquals('\b', parse("copy my_table from stdin delimiter e'\\b'").delimiter.charValue());
    assertEquals('\f', parse("copy my_table from stdin delimiter e'\\f'").delimiter.charValue());
    assertEquals('\n', parse("copy my_table from stdin delimiter e'\\n'").delimiter.charValue());
    assertEquals('\r', parse("copy my_table from stdin delimiter e'\\r'").delimiter.charValue());
    assertEquals('\1', parse("copy my_table from stdin delimiter e'\\1'").delimiter.charValue());
    assertEquals('\14', parse("copy my_table from stdin delimiter e'\\14'").delimiter.charValue());
    assertEquals(
        '\111', parse("copy my_table from stdin delimiter e'\\111'").delimiter.charValue());
    assertEquals(
        '\u0011', parse("copy my_table from stdin delimiter e'\\u0011'").delimiter.charValue());

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin delimiter '\\t'"));
    assertEquals("COPY delimiter must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to create a delimiter with a special character, like a tab.\nExample: copy my_table to stdout (delimiter e'\\t')",
        exception.getHints());

    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin delimiter e'\\x7'"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseLegacyNull() {
    assertEquals("\\N", parse("copy my_table from stdin null '\\N'").nullString);
    assertEquals("\t", parse("copy my_table from stdin delimiter e'\t'\tnull e'\\t'").nullString);
    assertEquals("this_is_null", parse("copy my_table from stdin null 'this_is_null'").nullString);

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin null null"));
    assertEquals("Invalid quote character: n", exception.getMessage());

    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin null e'\\x7'"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseLegacyQuote() {
    assertEquals(',', parse("copy my_table from stdin csv header quote ','").quote.charValue());
    assertEquals('\t', parse("copy my_table from stdin csv quote e'\\t'").quote.charValue());
    assertEquals(
        '\\', parse("copy my_table from stdin null '\\NULL' csv quote e'\\\\'").quote.charValue());
    assertEquals(
        '\b', parse("copy my_table from stdin delimiter '|' csv quote e'\\b'").quote.charValue());

    PGException exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin csv header quote '\\t'"));
    assertEquals("COPY quote must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to specify a special quote character, for example using an octal value.\n"
            + "Example: copy my_table to stdout (quote e'\\4', format csv)",
        exception.getHints());

    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin csv quote e'\\x7'"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseLegacyEscape() {
    assertEquals(',', parse("copy my_table from stdin csv escape ','").escape.charValue());
    assertEquals(
        '\t', parse("copy my_table from stdin csv header escape e'\\t'").escape.charValue());
    assertEquals(
        '\\', parse("copy my_table from stdin csv quote '''' escape e'\\\\'").escape.charValue());
    assertEquals('\b', parse("copy my_table from stdin csv escape e'\\b'").escape.charValue());
    assertEquals(
        '\f', parse("copy my_table from stdin null '\\NULL' csv escape e'\\f'").escape.charValue());
    assertEquals(
        '\n',
        parse("copy my_table from stdin null '\\NULL' csv header quote e'\\'' escape e'\\n'")
            .escape
            .charValue());

    PGException exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin csv escape '\\t'"));
    assertEquals("COPY escape must be a single one-byte character", exception.getMessage());
    assertEquals(
        "Use an escaped string to specify a special escape character, for example using an octal value.\n"
            + "Example: copy my_table to stdout (escape e'\\4', format csv)",
        exception.getHints());

    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin csv header escape e'\\x7'"));
    assertEquals(
        "PGAdapter does not support hexadecimal byte values in string literals",
        exception.getMessage());
  }

  @Test
  public void testParseLegacyForceNotNull() {
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table from stdin csv force not null col1").forceNotNull);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table from stdin csv force not null col1, col2").forceNotNull);

    PGException exception;
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table from stdin csv force not null"));
    assertEquals("empty force not null columns list", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table from stdin csv force not null * col1"));
    assertEquals("Invalid column name: * col1", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () -> parse("copy my_table from stdin csv force not null col1, 'col2'"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseLegacyForceQuote() {
    assertEquals(ImmutableList.of(), parse("copy my_table to stdout csv force quote *").forceQuote);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1")),
        parse("copy my_table to stdout csv force quote col1").forceQuote);
    assertEquals(
        ImmutableList.of(new TableOrIndexName("col1"), new TableOrIndexName("col2")),
        parse("copy my_table to stdout csv force quote col1, col2").forceQuote);

    PGException exception;
    exception =
        assertThrows(PGException.class, () -> parse("copy my_table to stdout csv force quote"));
    assertEquals("empty force quote columns list", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table to stdout csv force quote * col1"));
    assertEquals("Syntax error. Unexpected tokens: col1", exception.getMessage());
    exception =
        assertThrows(
            PGException.class, () -> parse("copy my_table to stdout csv force quote col1, 'col2'"));
    assertEquals("Invalid column name: 'col2'", exception.getMessage());
  }

  @Test
  public void testParseLegacyOutOfOrder() {
    assertEquals(
        ImmutableList.of(),
        parse("copy my_table to stdout csv force quote * quote '\"'").forceQuote);
    assertEquals('"', (char) parse("copy my_table to stdout csv force quote * quote '\"'").quote);
    assertEquals(
        "null",
        parse("copy my_table to stdout csv force quote * quote '\"' null 'null'").nullString);

    assertThrows(
        PGException.class,
        () -> parse("copy my_table to stdout csv force quote * quote '\"' null 'null' escape '['"));
  }
}
