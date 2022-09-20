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
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.ResetStatement;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SetStatement;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.ShowStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SessionStatementParserTest {
  private static final AbstractStatementParser PG_PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  @Test
  public void testParseShowAll() {
    assertEquals(
        ShowStatement.createShowAll(),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show all"))));
  }

  @Test
  public void testParseShow() {
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show foo.bar"))));

    assertEquals(
        new ShowStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show foo"))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show \"foo\""))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show\t\"foo\""))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show FOO"))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show \"FOO\""))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show\t\"FOO\""))));

    assertEquals(
        new ShowStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show foo.bar"))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show \"FOO\".bar"))));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show\t\"FOO\".\"Bar\""))));

    assertThrows(
        SpannerException.class,
        () -> SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show"))));
    assertThrows(
        SpannerException.class,
        () -> SessionStatementParser.parse(PG_PARSER.parse(Statement.of("show foo bar"))));
  }

  @Test
  public void testParseReset() {
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset foo"))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset \"foo\""))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset\t\"foo\""))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset FOO"))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset \"FOO\""))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset\t\"FOO\""))));

    assertEquals(
        new ResetStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset foo.bar"))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset \"FOO\".bar"))));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo", "bar")),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset\t\"FOO\".\"Bar\""))));

    assertThrows(
        SpannerException.class,
        () -> SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset"))));
    assertThrows(
        SpannerException.class,
        () -> SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset foo bar"))));
  }

  @Test
  public void testParseResetAll() {
    assertEquals(
        ResetStatement.createResetAll(),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("reset all"))));
  }

  @Test
  public void testParseSetTo() {
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set foo to bar"))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set session foo to bar"))));
    assertEquals(
        new SetStatement(true, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set local foo to bar"))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set \"foo\" to 'bar'"))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set \"foo\" to \"bar\""))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), null),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set foo to default"))));
  }

  @Test
  public void testParseSetEquals() {
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set foo = bar"))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set session foo = bar"))));
    assertEquals(
        new SetStatement(true, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set local foo = bar"))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set \"foo\" = 'bar'"))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set \"foo\" = \"bar\""))));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), null),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set foo = default"))));
  }

  @Test
  public void testTimeZone(){
    assertEquals(
        new SetStatement(false, new TableOrIndexName("TIMEZONE"), "'UTC'"),
        SessionStatementParser.parse(PG_PARSER.parse(Statement.of("set time zone 'UTC'"))));
  }

}
