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

import static com.google.cloud.spanner.pgadapter.statements.TruncateStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TruncateStatementTest {

  @Test
  public void testNoOptions() {
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("truncate foo").tables);
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("truncate table foo").tables);
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("truncate only foo").tables);
    assertEquals(
        ImmutableList.of(TableOrIndexName.of("foo")), parse("truncate table only foo").tables);
    assertEquals(
        ImmutableList.of(TableOrIndexName.of("foo"), TableOrIndexName.of("bar")),
        parse("truncate foo, bar").tables);
    assertEquals(
        ImmutableList.of(TableOrIndexName.of("public", "foo"), TableOrIndexName.of("other", "bar")),
        parse("truncate public.foo, other.bar").tables);
    assertEquals(
        ImmutableList.of(
            TableOrIndexName.of("\"public\"", "foo"), TableOrIndexName.of("other", "\"bar\"")),
        parse("truncate \"public\".foo, other.\"bar\"").tables);
    assertEquals(
        ImmutableList.of(
            TableOrIndexName.of("\"public\"", "foo"), TableOrIndexName.of("\"other,bar\"")),
        parse("truncate \"public\".foo, \"other,bar\"").tables);
  }

  @Test
  public void testStar() {
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("truncate foo *").tables);
    assertTrue(parse("truncate foo *").star);
    assertEquals(ImmutableList.of(TableOrIndexName.of("foo")), parse("truncate foo*").tables);
    assertTrue(parse("truncate foo*").star);
    assertEquals(
        ImmutableList.of(TableOrIndexName.of("public", "foo")),
        parse("truncate public.foo*").tables);
    assertTrue(parse("truncate public.foo*").star);

    assertEquals(
        ImmutableList.of(TableOrIndexName.of("foo"), TableOrIndexName.of("bar")),
        parse("truncate foo *, bar").tables);
    assertTrue(parse("truncate foo *, bar").star);

    assertThrows(PGException.class, () -> parse("truncate foo *, bar *"));
    assertThrows(PGException.class, () -> parse("truncate foo *, bar*"));
  }

  @Test
  public void testRestartIdentity() {
    assertFalse(parse("truncate foo").restartIdentity);
    assertTrue(parse("truncate foo restart identity").restartIdentity);
    assertTrue(parse("truncate foo*restart identity").restartIdentity);
    assertTrue(parse("truncate foo, bar restart identity").restartIdentity);
    assertFalse(parse("truncate foo, bar, \"restart identity\"").restartIdentity);
    assertTrue(parse("truncate foo, bar restart identity cascade").restartIdentity);
    assertTrue(parse("truncate foo, bar restart identity restrict").restartIdentity);

    assertThrows(PGException.class, () -> parse("truncate foo restart identity continue identity"));
    assertThrows(PGException.class, () -> parse("truncate foo continue identity restart identity"));
  }

  @Test
  public void testContinueIdentity() {
    assertFalse(parse("truncate foo").restartIdentity);
    assertFalse(parse("truncate foo continue identity").restartIdentity);
    assertFalse(parse("truncate foo*continue identity").restartIdentity);
    assertFalse(parse("truncate foo, bar continue identity").restartIdentity);
    assertFalse(parse("truncate foo, bar, \"continue identity\"").restartIdentity);
    assertFalse(parse("truncate foo, bar continue identity cascade").restartIdentity);
    assertFalse(parse("truncate foo, bar continue identity restrict").restartIdentity);

    assertThrows(PGException.class, () -> parse("truncate foo restart identity continue identity"));
    assertThrows(PGException.class, () -> parse("truncate foo continue identity restart identity"));
  }

  @Test
  public void testCascade() {
    assertFalse(parse("truncate foo").cascade);
    assertTrue(parse("truncate foo cascade").cascade);
    assertTrue(parse("truncate foo*cascade").cascade);
    assertTrue(parse("truncate foo, bar cascade").cascade);
    assertFalse(parse("truncate foo, bar, \"cascade\"").cascade);
    assertTrue(parse("truncate foo, bar restart identity cascade").cascade);
    assertTrue(parse("truncate foo, bar continue identity cascade").cascade);

    assertThrows(PGException.class, () -> parse("truncate foo restrict cascade"));
    assertThrows(PGException.class, () -> parse("truncate foo cascade restrict"));
  }

  @Test
  public void testRestrict() {
    assertFalse(parse("truncate foo restrict").cascade);
    assertFalse(parse("truncate foo*restrict").cascade);
    assertFalse(parse("truncate foo, bar restrict").cascade);
    assertFalse(parse("truncate foo, bar, \"restrict\"").cascade);
    assertFalse(parse("truncate foo, bar restart identity restrict").cascade);
    assertFalse(parse("truncate foo, bar continue identity restrict").cascade);

    assertThrows(PGException.class, () -> parse("truncate foo restrict cascade"));
    assertThrows(PGException.class, () -> parse("truncate foo cascade restrict"));
  }

  @Test
  public void testInvalidTruncateStatement() {
    PGException exception;

    exception = assertThrows(PGException.class, () -> parse("trncate foo"));
    assertEquals("not a valid TRUNCATE statement: trncate foo", exception.getMessage());

    exception = assertThrows(PGException.class, () -> parse("truncate"));
    assertEquals("invalid or missing table name", exception.getMessage());

    exception = assertThrows(PGException.class, () -> parse("truncate 'foo"));
    assertEquals("invalid or missing table name", exception.getMessage());

    exception = assertThrows(PGException.class, () -> parse("truncate foo, "));
    assertEquals("invalid or missing table name", exception.getMessage());
  }
}
