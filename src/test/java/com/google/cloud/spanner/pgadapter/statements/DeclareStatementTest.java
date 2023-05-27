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

import static com.google.cloud.spanner.pgadapter.statements.DeclareStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.DeclareStatement.Holdability;
import com.google.cloud.spanner.pgadapter.statements.DeclareStatement.ParsedDeclareStatement;
import com.google.cloud.spanner.pgadapter.statements.DeclareStatement.Scroll;
import com.google.cloud.spanner.pgadapter.statements.DeclareStatement.Sensitivity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeclareStatementTest {
  @Test
  public void testParse() {
    ParsedDeclareStatement statement;

    statement = parse("declare foo cursor for select 1");
    assertEquals("foo", statement.name);
    assertFalse(statement.binary);
    assertNull(statement.scroll);
    assertNull(statement.sensitivity);
    assertNull(statement.holdability);
    assertEquals("select 1", statement.originalPreparedStatement.getSql());

    statement = parse("declare \"my-cursor\" binary cursor for select * from foo where bar=1");
    assertEquals("my-cursor", statement.name);
    assertTrue(statement.binary);
    assertEquals("select * from foo where bar=1", statement.originalPreparedStatement.getSql());

    assertFalse(parse("declare foo cursor for select 1").binary);
    assertTrue(parse("declare foo binary cursor for select 1").binary);

    assertNull(parse("declare foo cursor for select 1").sensitivity);
    assertEquals(
        Sensitivity.ASENSITIVE, parse("declare foo asensitive cursor for select 1").sensitivity);
    assertEquals(
        Sensitivity.INSENSITIVE, parse("declare foo insensitive cursor for select 1").sensitivity);

    assertNull(parse("declare foo cursor for select 1").scroll);
    assertEquals(Scroll.NO_SCROLL, parse("declare foo no scroll cursor for select 1").scroll);
    assertEquals(Scroll.SCROLL, parse("declare foo scroll cursor for select 1").scroll);

    assertNull(parse("declare foo cursor for select 1").holdability);
    assertEquals(Holdability.HOLD, parse("declare foo cursor with hold for select 1").holdability);
    assertEquals(
        Holdability.NO_HOLD, parse("declare foo cursor without hold for select 1").holdability);

    statement = parse("declare foo scroll binary cursor for select 1");
    assertEquals(Scroll.SCROLL, statement.scroll);
    assertTrue(statement.binary);
    statement = parse("declare foo no scroll insensitive cursor for select 1");
    assertEquals(Scroll.NO_SCROLL, statement.scroll);
    assertEquals(Sensitivity.INSENSITIVE, statement.sensitivity);
    statement = parse("declare foo no scroll no scroll insensitive cursor for select 1");
    assertEquals(Scroll.NO_SCROLL, statement.scroll);
    assertEquals(Sensitivity.INSENSITIVE, statement.sensitivity);

    assertThrows(PGException.class, () -> parse("foo"));
    assertThrows(PGException.class, () -> parse("declare foo cursor for "));
    assertThrows(PGException.class, () -> parse("declare foo cursor select 1"));
    assertThrows(PGException.class, () -> parse("declare foo for select 1"));
    assertThrows(PGException.class, () -> parse("declare cursor for select 1"));
    assertThrows(
        PGException.class, () -> parse("declare foo insensitive asensitive cursor for select 1"));
    assertThrows(
        PGException.class, () -> parse("declare foo scroll no scroll cursor for select 1"));
  }
}
