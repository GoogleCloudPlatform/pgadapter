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

import static com.google.cloud.spanner.pgadapter.statements.VacuumStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.statements.VacuumStatement.ParsedVacuumStatement;
import com.google.cloud.spanner.pgadapter.statements.VacuumStatement.ParsedVacuumStatement.IndexCleanup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VacuumStatementTest {

  @Test
  public void testParseNoOptions() {
    ParsedVacuumStatement statement = parse("vacuum");
    assertFalse(statement.analyze);
    assertFalse(statement.disablePageSkipping);
    assertFalse(statement.freeze);
    assertFalse(statement.full);
    assertFalse(statement.processToast);
    assertFalse(statement.skipLocked);
    assertFalse(statement.truncate);
    assertFalse(statement.verbose);
    assertEquals(0, statement.parallel);
    assertEquals(IndexCleanup.OFF, statement.indexCleanup);
    assertTrue(statement.tables.isEmpty());
    assertTrue(statement.columns.isEmpty());
  }

  @Test
  public void testParseAnalyze() {
    assertTrue(parse("vacuum (analyze)").analyze);
    assertTrue(parse("vacuum (analyze t)").analyze);
    assertFalse(parse("vacuum (analyze off)").analyze);
    assertTrue(parse("vacuum analyze").analyze);
  }

  @Test
  public void testParseDisablePageSkipping() {
    assertTrue(parse("vacuum (disable_page_skipping)").disablePageSkipping);
    assertTrue(parse("vacuum (disable_page_skipping on)").disablePageSkipping);
    assertFalse(parse("vacuum (disable_page_skipping f)").disablePageSkipping);

    assertFalse(parse("vacuum disable_page_skipping").disablePageSkipping);
    assertEquals("disable_page_skipping", parse("vacuum disable_page_skipping").tables.get(0).name);
  }

  @Test
  public void testParseFreeze() {
    assertTrue(parse("vacuum (freeze)").freeze);
    assertTrue(parse("vacuum (freeze 1)").freeze);
    assertFalse(parse("vacuum (freeze 0)").freeze);
    assertTrue(parse("vacuum freeze").freeze);
  }

  @Test
  public void testParseFull() {
    assertTrue(parse("vacuum (full)").full);
    assertTrue(parse("vacuum (full 1)").full);
    assertFalse(parse("vacuum (full 0)").full);
    assertTrue(parse("vacuum full").full);
  }

  @Test
  public void testParseProcessToast() {
    assertTrue(parse("vacuum (process_toast)").processToast);
    assertTrue(parse("vacuum (process_toast true)").processToast);
    assertFalse(parse("vacuum (process_toast false)").processToast);

    assertFalse(parse("vacuum process_toast").processToast);
    assertEquals("process_toast", parse("vacuum process_toast").tables.get(0).name);
  }

  @Test
  public void testParseSkipLocked() {
    assertTrue(parse("vacuum (skip_locked)").skipLocked);
    assertTrue(parse("vacuum (skip_locked true)").skipLocked);
    assertFalse(parse("vacuum (skip_locked false)").skipLocked);

    assertFalse(parse("vacuum skip_locked").skipLocked);
    assertEquals("skip_locked", parse("vacuum skip_locked").tables.get(0).name);
  }

  @Test
  public void testParseTruncate() {
    assertTrue(parse("vacuum (truncate)").truncate);
    assertTrue(parse("vacuum (truncate 1)").truncate);
    assertFalse(parse("vacuum (truncate 0)").truncate);

    // TODO: Add support for keyword detection in the parser, so this is seen as an invalid table
    // name.
    assertFalse(parse("vacuum truncate").truncate);
    assertEquals("truncate", parse("vacuum truncate").tables.get(0).name);
  }

  @Test
  public void testParseVerbose() {
    assertTrue(parse("vacuum (verbose)").verbose);
    assertTrue(parse("vacuum (verbose 1)").verbose);
    assertFalse(parse("vacuum (verbose 0)").verbose);
    assertTrue(parse("vacuum verbose").verbose);
  }

  @Test
  public void testParseParallel() {
    assertEquals(1, parse("vacuum (parallel 1)").parallel);
    assertEquals(100, parse("vacuum (parallel 100)").parallel);

    assertThrows(PGException.class, () -> parse("vacuum (parallel)"));
    assertThrows(PGException.class, () -> parse("vacuum (parallel foo)"));
    assertThrows(PGException.class, () -> parse("vacuum (parallel '1')"));
    assertThrows(PGException.class, () -> parse("vacuum (parallel [1])"));
    assertThrows(PGException.class, () -> parse("vacuum parallel 2"));
    assertThrows(PGException.class, () -> parse("vacuum (parallel 0)"));
    assertThrows(PGException.class, () -> parse("vacuum (parallel -1)"));
    assertEquals(0, parse("vacuum parallel").parallel);
    assertEquals("parallel", parse("vacuum parallel").tables.get(0).name);
  }

  @Test
  public void testParseIndexCleanup() {
    assertEquals(IndexCleanup.ON, parse("vacuum (index_cleanup)").indexCleanup);
    assertEquals(IndexCleanup.ON, parse("vacuum (index_cleanup on)").indexCleanup);
    assertEquals(IndexCleanup.ON, parse("vacuum (index_cleanup t)").indexCleanup);
    assertEquals(IndexCleanup.ON, parse("vacuum (index_cleanup true)").indexCleanup);
    assertEquals(IndexCleanup.ON, parse("vacuum (index_cleanup 1)").indexCleanup);
    assertEquals(IndexCleanup.OFF, parse("vacuum (index_cleanup off)").indexCleanup);
    assertEquals(IndexCleanup.OFF, parse("vacuum (index_cleanup f)").indexCleanup);
    assertEquals(IndexCleanup.OFF, parse("vacuum (index_cleanup false)").indexCleanup);
    assertEquals(IndexCleanup.OFF, parse("vacuum (index_cleanup 0)").indexCleanup);
    assertEquals(IndexCleanup.AUTO, parse("vacuum (index_cleanup auto)").indexCleanup);

    assertThrows(PGException.class, () -> parse("vacuum (index_cleanup foo)"));
    assertEquals(IndexCleanup.OFF, parse("vacuum index_cleanup").indexCleanup);
    assertEquals("index_cleanup", parse("vacuum index_cleanup").tables.get(0).name);
  }

  @Test
  public void testParseTables() {
    assertEquals(0, parse("vacuum").tables.size());
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("vacuum foo").tables);
    assertEquals(
        ImmutableList.of(TableOrIndexName.of("foo"), TableOrIndexName.of("bar")),
        parse("vacuum foo, bar").tables);
    assertEquals(
        ImmutableList.of(
            TableOrIndexName.of("foo"),
            TableOrIndexName.of("\"bar\""),
            TableOrIndexName.of("\"my_schema\"", "baz")),
        parse("vacuum foo, \"bar\", \"my_schema\".baz ").tables);
    assertEquals(
        ImmutableList.of(
            TableOrIndexName.of("public", "\"foo\""),
            TableOrIndexName.of("\"bar\""),
            TableOrIndexName.of("\"my_schema\"", "baz")),
        parse("vacuum public.\"foo\", \"bar\", \"my_schema\".baz ").tables);

    assertThrows(
        PGException.class,
        () -> parse("vacuum public.\"foo\", \"bar\", \"my_schema\".baz garbage"));
  }

  @Test
  public void testParseColumns() {
    assertEquals(0, parse("vacuum").columns.size());
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("vacuum foo(col1)").tables);
    assertEquals(
        ImmutableMap.of(TableOrIndexName.of("foo"), ImmutableList.of(TableOrIndexName.of("col1"))),
        parse("vacuum foo(col1)").columns);

    assertEquals(
        ImmutableList.of(TableOrIndexName.of("foo"), TableOrIndexName.of("bar")),
        parse("vacuum foo(col1), bar (col1, col2)").tables);
    assertEquals(
        ImmutableMap.of(
            TableOrIndexName.of("foo"),
            ImmutableList.of(TableOrIndexName.of("col1")),
            TableOrIndexName.of("bar"),
            ImmutableList.of(TableOrIndexName.of("col1"), TableOrIndexName.of("col2"))),
        parse("vacuum foo(col1), bar (col1, col2)").columns);

    assertEquals(
        ImmutableList.of(
            TableOrIndexName.of("foo"),
            TableOrIndexName.of("\"bar\""),
            TableOrIndexName.of("\"my_schema\"", "baz")),
        parse(
                "vacuum foo(col1), \"bar\" (col1, col2), \"my_schema\".baz (\"col1\", col2, \"col3\")")
            .tables);
    assertEquals(
        ImmutableMap.of(
            TableOrIndexName.of("foo"), ImmutableList.of(TableOrIndexName.of("col1")),
            TableOrIndexName.of("\"bar\""),
                ImmutableList.of(TableOrIndexName.of("col1"), TableOrIndexName.of("col2")),
            TableOrIndexName.of("\"my_schema\"", "baz"),
                ImmutableList.of(
                    TableOrIndexName.of("\"col1\""),
                    TableOrIndexName.of("col2"),
                    TableOrIndexName.of("\"col3\""))),
        parse(
                "vacuum foo(col1), \"bar\" (col1, col2), \"my_schema\".baz (\"col1\", col2, \"col3\")")
            .columns);

    assertEquals(
        ImmutableList.of(
            TableOrIndexName.of("public", "\"foo\""),
            TableOrIndexName.of("\"bar\""),
            TableOrIndexName.of("\"my_schema\"", "baz")),
        parse("vacuum public.\"foo\"(col1), \"bar\" (col1, col2), \"my_schema\".baz\n(col1)")
            .tables);
    assertEquals(
        ImmutableMap.of(
            TableOrIndexName.of("public", "\"foo\""), ImmutableList.of(TableOrIndexName.of("col1")),
            TableOrIndexName.of("\"bar\""),
                ImmutableList.of(TableOrIndexName.of("col1"), TableOrIndexName.of("col2")),
            TableOrIndexName.of("\"my_schema\"", "baz"),
                ImmutableList.of(TableOrIndexName.of("col1"))),
        parse("vacuum public.\"foo\"(col1), \"bar\" (col1, col2), \"my_schema\".baz\n(col1)")
            .columns);
  }
}
