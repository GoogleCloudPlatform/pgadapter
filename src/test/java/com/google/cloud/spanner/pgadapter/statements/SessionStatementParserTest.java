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
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SessionStatement;
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

  static SessionStatement parse(String sql) {
    return SessionStatementParser.parse(PG_PARSER.parse(Statement.of(sql)), sql);
  }

  @Test
  public void testParseShowAll() {
    assertEquals(ShowStatement.createShowAll(), parse("show all"));
  }

  @Test
  public void testParseShow() {
    assertEquals(new ShowStatement(new TableOrIndexName("foo", "bar")), parse("show foo.bar"));

    assertEquals(new ShowStatement(new TableOrIndexName("foo")), parse("show foo"));
    assertEquals(new ShowStatement(new TableOrIndexName("foo")), parse("show \"foo\""));
    assertEquals(new ShowStatement(new TableOrIndexName("foo")), parse("show\t\"foo\""));
    assertEquals(new ShowStatement(new TableOrIndexName("foo")), parse("show FOO"));
    assertEquals(new ShowStatement(new TableOrIndexName("foo")), parse("show \"FOO\""));
    assertEquals(new ShowStatement(new TableOrIndexName("foo")), parse("show\t\"FOO\""));

    assertEquals(new ShowStatement(new TableOrIndexName("foo", "bar")), parse("show foo.bar"));
    assertEquals(new ShowStatement(new TableOrIndexName("foo", "bar")), parse("show \"FOO\".bar"));
    assertEquals(
        new ShowStatement(new TableOrIndexName("foo", "bar")), parse("show\t\"FOO\".\"Bar\""));

    assertThrows(SpannerException.class, () -> parse("show"));
    assertThrows(SpannerException.class, () -> parse("show foo bar"));
  }

  @Test
  public void testParseReset() {
    assertEquals(new ResetStatement(new TableOrIndexName("foo")), parse("reset foo"));
    assertEquals(new ResetStatement(new TableOrIndexName("foo")), parse("reset \"foo\""));
    assertEquals(new ResetStatement(new TableOrIndexName("foo")), parse("reset\t\"foo\""));
    assertEquals(new ResetStatement(new TableOrIndexName("foo")), parse("reset FOO"));
    assertEquals(new ResetStatement(new TableOrIndexName("foo")), parse("reset \"FOO\""));
    assertEquals(new ResetStatement(new TableOrIndexName("foo")), parse("reset\t\"FOO\""));

    assertEquals(new ResetStatement(new TableOrIndexName("foo", "bar")), parse("reset foo.bar"));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo", "bar")), parse("reset \"FOO\".bar"));
    assertEquals(
        new ResetStatement(new TableOrIndexName("foo", "bar")), parse("reset\t\"FOO\".\"Bar\""));

    assertThrows(SpannerException.class, () -> parse("reset"));
    assertThrows(SpannerException.class, () -> parse("reset foo bar"));
  }

  @Test
  public void testParseResetAll() {
    assertEquals(ResetStatement.createResetAll(), parse("reset all"));
  }

  @Test
  public void testParseSetTo() {
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"), parse("set foo to bar"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        parse("set session foo to bar"));
    assertEquals(
        new SetStatement(true, new TableOrIndexName("foo"), "bar"), parse("set local foo to bar"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"), parse("set \"foo\" to 'bar'"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        parse("set \"foo\" to \"bar\""));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), null), parse("set foo to default"));
  }

  @Test
  public void testParseSetEquals() {
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"), parse("set foo = bar"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        parse("set session foo = bar"));
    assertEquals(
        new SetStatement(true, new TableOrIndexName("foo"), "bar"), parse("set local foo = bar"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"), parse("set \"foo\" = 'bar'"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), "bar"),
        parse("set \"foo\" = \"bar\""));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("foo"), null), parse("set foo = default"));
  }

  @Test
  public void testSetTimeZone() {
    assertEquals(
        new SetStatement(false, new TableOrIndexName("TIMEZONE"), "'UTC'"),
        parse("set time zone 'UTC'"));
    assertEquals(
        new SetStatement(false, new TableOrIndexName("TIMEZONE"), "'UTC'"),
        parse("set TIME ZONE 'UTC'"));
  }

  @Test
  public void testShowTimeZone() {
    assertEquals(new ShowStatement(new TableOrIndexName("timezone")), parse("show time zone"));
    assertEquals(new ShowStatement(new TableOrIndexName("timezone")), parse("show TIME ZONE"));
  }
}
