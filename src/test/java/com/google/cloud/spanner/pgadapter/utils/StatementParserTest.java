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

import static com.google.cloud.spanner.pgadapter.utils.StatementParser.skipDollarQuotedString;
import static com.google.cloud.spanner.pgadapter.utils.StatementParser.skipMultiLineComment;
import static com.google.cloud.spanner.pgadapter.utils.StatementParser.skipQuotedString;
import static com.google.cloud.spanner.pgadapter.utils.StatementParser.skipSingleLineComment;
import static com.google.cloud.spanner.pgadapter.utils.StatementParser.splitStatements;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StatementParserTest {

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
  }

  @Test
  public void testSplitStatements() {
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1"));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1;"));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1; "));
    assertEquals(ImmutableList.of("select 1"), splitStatements("select 1;\n"));

    assertEquals(ImmutableList.of("select 1", "select 2"), splitStatements("select 1; select 2"));
    assertEquals(ImmutableList.of("select 1", "select 2"), splitStatements("select 1; select 2;"));

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
