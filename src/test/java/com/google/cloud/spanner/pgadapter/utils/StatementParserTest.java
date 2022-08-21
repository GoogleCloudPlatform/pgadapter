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

package com.google.cloud.spanner.pgadapter.utils;

import static com.google.cloud.spanner.pgadapter.commands.DynamicCommand.singleQuoteEscape;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParserTest.skipDollarQuotedString;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParserTest.skipMultiLineComment;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParserTest.skipQuotedString;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParserTest.skipSingleLineComment;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParserTest.splitStatements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StatementParserTest {
  private final AbstractStatementParser parser =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  @Test
  public void testRemoveCommentsAndTrim() {

    String sqlStatement = "-- This is a one line comment\nSELECT * FROM FOO";
    String expectedResult = "SELECT * FROM FOO";
    String result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    sqlStatement = "/* This is a simple multi line comment */\nSELECT * FROM FOO";
    expectedResult = "SELECT * FROM FOO";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    sqlStatement = "/* This is a \nmulti line comment */\nSELECT * FROM FOO";
    expectedResult = "SELECT * FROM FOO";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    sqlStatement = "/* This\nis\na\nmulti\nline\ncomment */\nSELECT * FROM FOO";
    expectedResult = "SELECT * FROM FOO";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    sqlStatement =
        "/*\n"
            + " * Script for testing invalid/unrecognized statements\n"
            + " */\n"
            + "\n"
            + "-- MERGE into test comment MERGE -- \n"
            + "@EXPECT EXCEPTION INVALID_ARGUMENT 'INVALID_ARGUMENT: Unknown statement'\n"
            + "MERGE INTO Singers s\n"
            + "/*** test ****/"
            + "USING (VALUES (1, 'John', 'Doe')) v\n"
            + "ON v.column1 = s.SingerId\n"
            + "WHEN NOT MATCHED \n"
            + "  INSERT VALUES (v.column1, v.column2, v.column3)\n"
            + "WHEN MATCHED\n"
            + "  UPDATE SET FirstName = v.column2,\n"
            + "             LastName = v.column3;";
    expectedResult =
        "@EXPECT EXCEPTION INVALID_ARGUMENT 'INVALID_ARGUMENT: Unknown statement'\n"
            + "MERGE INTO Singers s\n"
            + "USING (VALUES (1, 'John', 'Doe')) v\n"
            + "ON v.column1 = s.SingerId\n"
            + "WHEN NOT MATCHED \n"
            + "  INSERT VALUES (v.column1, v.column2, v.column3)\n"
            + "WHEN MATCHED\n"
            + "  UPDATE SET FirstName = v.column2,\n"
            + "             LastName = v.column3";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    // Dollar Quoted
    sqlStatement = "$$--foo$$";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "$$\nline 1\n--line2$$";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "$bar$--foo$bar$";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "SELECT FOO$BAR FROM SOME_TABLE";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "SELECT FOO$BAR -- This is a comment\nFROM SOME_TABLE";
    expectedResult = "SELECT FOO$BAR \nFROM SOME_TABLE";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    // Embedded Comments
    sqlStatement =
        "/* This is a comment /* This is an embedded comment */ This is after the embedded comment */ SELECT 1";
    expectedResult = "SELECT 1";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, expectedResult);

    // No effect HashTag Comment
    sqlStatement = "# this is a comment\nselect * from foo";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "select *\nfrom foo # this is a comment\nwhere bar=1";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    // When parameters are mixed with dollar-quoted string
    sqlStatement = "$1 $$?it$?s$$ $2";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "$1 $tag$?it$$?s$tag$ $2";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);

    sqlStatement = "$1 $$?it\\'?s \n ?it\\'?s$$ $2";
    result = parser.removeCommentsAndTrim(sqlStatement);
    assertEquals(result, sqlStatement);
  }

  @Test
  public void testRemoveCommentsAndTrimWithUnterminatedComment() {
    String sqlStatement =
        "/* This is a comment /* This is still a comment */ this is unterminated SELECT 1";
    SpannerException exception =
        assertThrows(SpannerException.class, () -> parser.removeCommentsAndTrim(sqlStatement));
    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
    assertEquals(
        "INVALID_ARGUMENT: SQL statement contains an unterminated block comment: /* This is a comment /* This is still a comment */ this is unterminated SELECT 1",
        exception.getMessage());
  }

  @Test
  public void testEscapes() {
    String sql = "Bobby\\'O\\'Bob'; DROP TABLE USERS; select'";
    String expectedSql = "Bobby\\''O\\''Bob''; DROP TABLE USERS; select''";
    assertEquals(expectedSql, singleQuoteEscape(sql));

    assertEquals("''test''", singleQuoteEscape("'test'"));
    assertEquals("''''test''''", singleQuoteEscape("''test''"));
  }

  @Test
  public void testSkipQuotedString() {
    assertEquals(5, skipQuotedString("'foo', bar", 0));
    assertEquals(5, skipQuotedString("'foo'", 0));
    assertEquals(4, skipQuotedString("'foo", 0));
    assertEquals(10, skipQuotedString("bar, 'foo'", 5));
    assertEquals(10, skipQuotedString("bar, 'foo', bar", 5));
    assertEquals(9, skipQuotedString("bar, 'foo", 5));
  }

  @Test
  public void testSkipDollarQuotedString() {
    assertEquals(7, skipDollarQuotedString("$$foo$$, bar", 0));
    assertEquals(7, skipDollarQuotedString("$$foo$$", 0));
    assertEquals(5, skipDollarQuotedString("$$foo", 0));
    assertEquals(12, skipDollarQuotedString("bar, $$foo$$", 5));
    assertEquals(12, skipDollarQuotedString("bar, $$foo$$, bar", 5));
    assertEquals(10, skipDollarQuotedString("bar, $$foo", 5));

    assertEquals(13, skipDollarQuotedString("$bar$foo$bar$, bar", 0));
    assertEquals(13, skipDollarQuotedString("$bar$foo$bar$", 0));
    assertEquals(8, skipDollarQuotedString("$bar$foo", 0));
    assertEquals(18, skipDollarQuotedString("bar, $bar$foo$bar$", 5));
    assertEquals(18, skipDollarQuotedString("bar, $bar$foo$bar$, bar", 5));
    assertEquals(13, skipDollarQuotedString("bar, $bar$foo", 5));

    assertEquals(18, skipDollarQuotedString("$bar$foo$baz$, bar", 0));
    assertEquals(15, skipDollarQuotedString("$$foo$baz$, bar", 0));
    assertEquals(15, skipDollarQuotedString("$bar$foo$$, bar", 0));

    assertEquals(11, skipDollarQuotedString("$$foo$bar$$, bar", 0));
  }

  @Test
  public void testSkipSingleLineComment() {
    assertEquals(7, skipSingleLineComment("-- foo\n", 0));
    assertEquals(7, skipSingleLineComment("-- foo\nbar", 0));
    assertEquals(6, skipSingleLineComment("-- foo", 0));
    assertEquals(11, skipSingleLineComment("bar -- foo\n", 4));
    assertEquals(11, skipSingleLineComment("bar -- foo\nbar", 4));
    assertEquals(10, skipSingleLineComment("bar -- foo", 4));
  }

  @Test
  public void testSkipMultiLineComment() {
    assertEquals(9, skipMultiLineComment("/* foo */", 0));
    assertEquals(9, skipMultiLineComment("/* foo */ bar", 0));
    assertEquals(6, skipMultiLineComment("/* foo", 0));
    assertEquals(8, skipMultiLineComment("/* foo *", 0));
    assertEquals(9, skipMultiLineComment("/* foo **", 0));
    assertEquals(10, skipMultiLineComment("/* foo **/ ", 0));
    assertEquals(13, skipMultiLineComment("bar /* foo */", 4));
    assertEquals(13, skipMultiLineComment("bar /* foo */bar", 4));
    assertEquals(10, skipMultiLineComment("bar /* foo", 4));

    assertEquals(
        "/* foo /* inner comment */ not in inner comment */".length(),
        skipMultiLineComment("/* foo /* inner comment */ not in inner comment */ bar", 0));
  }

  @Test
  public void testSplitStatements() {
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1"));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1;"));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1; "));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1;\n"));

    assertEquals(ImmutableList.of("select 1", "select 2"), splitStatements("select 1; select 2"));
    assertEquals(ImmutableList.of("select 1", "select 2"), splitStatements("select 1; select 2;"));

    assertEquals(ImmutableList.of("select 1 -- Comment"), splitStatements("select 1 -- Comment"));
    assertEquals(
        ImmutableList.of("-- Comment \nselect 1"), splitStatements("-- Comment \nselect 1"));
    assertEquals(
        ImmutableList.of("select 1 --; select 2", "select 3"),
        splitStatements("select 1 --; select 2\n;select 3"));
    assertEquals(
        ImmutableList.of("-- select 1;\nselect 2", "select 3"),
        splitStatements("-- select 1;\nselect 2;select 3"));

    assertEquals(
        ImmutableList.of("select 1 /* Comment */"), splitStatements("select 1 /* Comment */"));
    assertEquals(
        ImmutableList.of("/* Comment */ select 1"), splitStatements("/* Comment */ select 1"));
    assertEquals(
        ImmutableList.of("select 1 /*; select 2 */", "select 3"),
        splitStatements("select 1 /*; select 2 */;select 3"));
    assertEquals(
        ImmutableList.of("/* select 1; */select 2", "select 3"),
        splitStatements("/* select 1; */select 2;select 3"));

    assertEquals(
        ImmutableList.of("select 'Hello World!'"), splitStatements("select 'Hello World!'"));
    assertEquals(
        ImmutableList.of("select 'Hello;World!'"), splitStatements("select 'Hello;World!'"));
    assertEquals(
        ImmutableList.of("select 'Hello;World!'"), splitStatements("select 'Hello;World!';"));
    assertEquals(
        ImmutableList.of("select 'Hello;World!'", "select 1"),
        splitStatements("select 'Hello;World!'; select 1"));

    assertEquals(
        ImmutableList.of("select * from \"my table\""),
        splitStatements("select * from \"my table\""));
    assertEquals(
        ImmutableList.of("select * from \"my;table\""),
        splitStatements("select * from \"my;table\""));
    assertEquals(
        ImmutableList.of("select * from \"my;table\""),
        splitStatements("select * from \"my;table\";"));
    assertEquals(
        ImmutableList.of("select * from \"my;table\"", "select 1"),
        splitStatements("select * from \"my;table\"; select 1"));

    assertEquals(
        ImmutableList.of("select 'Hello''World!'"), splitStatements("select 'Hello''World!'"));
    assertEquals(
        ImmutableList.of("select 'Hello'';World!'"), splitStatements("select 'Hello'';World!'"));
    assertEquals(
        ImmutableList.of("select 'Hello\"World!'"), splitStatements("select 'Hello\"World!'"));
    assertEquals(
        ImmutableList.of("select 'Hello\";World!'"), splitStatements("select 'Hello\";World!'"));

    assertEquals(
        ImmutableList.of("select * from \"my\"\"table\""),
        splitStatements("select * from \"my\"\"table\""));
    assertEquals(
        ImmutableList.of("select * from \"my\"\";table\""),
        splitStatements("select * from \"my\"\";table\""));
    assertEquals(
        ImmutableList.of("select * from \"my'table\""),
        splitStatements("select * from \"my'table\";"));
    assertEquals(
        ImmutableList.of("select * from \"my';table\"", "select 1"),
        splitStatements("select * from \"my';table\"; select 1"));
    assertEquals(
        ImmutableList.of(
            "/* This block comment surrounds a query which itself has a block comment...\n"
                + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
                + "*/\n"
                + "SELECT 1"),
        splitStatements(
            "/* This block comment surrounds a query which itself has a block comment...\n"
                + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
                + "*/\n"
                + "SELECT 1;"));
    assertEquals(
        ImmutableList.of(
            "/* This block comment surrounds a query which itself has a block comment...\n"
                + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
                + "*/\n"
                + "SELECT 1",
            "SELECT 2"),
        splitStatements(
            "/* This block comment surrounds a query which itself has a block comment...\n"
                + "SELECT /* embedded single line */ 'embedded' AS x2;\n"
                + "*/\n"
                + "SELECT 1; SELECT 2;"));

    assertEquals(
        ImmutableList.of("select $$Hello World!$$"), splitStatements("select $$Hello World!$$"));
    assertEquals(
        ImmutableList.of("select $$Hello;World!$$"), splitStatements("select $$Hello;World!$$"));
    assertEquals(
        ImmutableList.of("select $$Hello;World!$$"), splitStatements("select $$Hello;World!$$;"));
    assertEquals(
        ImmutableList.of("select $$Hello;World!$$", "select 1"),
        splitStatements("select $$Hello;World!$$; select 1"));
    assertEquals(
        ImmutableList.of("select $$Hello$;World!$$", "select 1"),
        splitStatements("select $$Hello$;World!$$; select 1"));
    assertEquals(
        ImmutableList.of("select $bar$Hello$;World!$bar$", "select 1"),
        splitStatements("select $bar$Hello$;World!$bar$; select 1"));
    assertEquals(
        ImmutableList.of("select $bar$Hello$$;World!$bar$", "select 1"),
        splitStatements("select $bar$Hello$$;World!$bar$; select 1"));
    assertEquals(
        ImmutableList.of("select $bar$Hello$baz$;World!$bar$", "select 1"),
        splitStatements("select $bar$Hello$baz$;World!$bar$; select 1"));
  }
}
