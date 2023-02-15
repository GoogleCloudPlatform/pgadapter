// Copyright 2023 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.RollbackToStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RollbackToStatementTest {

  @Test
  public void testParse() {
    assertEquals("foo", parse("rollback to foo").name);
    assertEquals("foo", parse("rollback to FOO").name);
    assertEquals("foo", parse("rollback\tto\tfoo").name);
    assertEquals("foo", parse("rollback\nto\nfoo").name);
    assertEquals("foo", parse("rollback/*comment*/to foo").name);
    assertEquals("foo", parse("rollback to \"foo\"").name);
    assertEquals("Foo", parse("rollback to \"Foo\"").name);
    assertEquals("savepoint", parse("rollback to \"savepoint\"").name);
    assertEquals("foo", parse("/* this will rollback a savepoint */ rollback to foo").name);

    assertEquals("foo", parse("rollback transaction to foo").name);
    assertEquals("foo", parse("rollback transaction to SAVEPOINT FOO").name);
    assertEquals("foo", parse("rollback work to \tSavepoint\tfoo").name);
    assertEquals("foo", parse("rollback transaction to \nsavepoint/**/foo").name);
    assertEquals("foo", parse("rollback/*comment*/work to foo").name);
    assertEquals("foo", parse("rollback work to savepoint \"foo\"").name);
    assertEquals("Foo", parse("rollback to savepoint \"Foo\"").name);
    assertEquals("savepoint", parse("rollback to \"savepoint\"").name);
    assertEquals(
        "foo", parse("/* this will rollback a savepoint */ rollback to savepoint foo").name);

    assertThrows(PGException.class, () -> parse("rollback"));
    assertThrows(PGException.class, () -> parse("rollback to"));
    assertThrows(PGException.class, () -> parse("rollback to my_savepoint foo"));
    assertThrows(PGException.class, () -> parse("rollback to foo.bar"));
    assertThrows(PGException.class, () -> parse("rollback to (foo)"));

    assertThrows(PGException.class, () -> parse("rollback work to savepoint"));
    assertThrows(PGException.class, () -> parse("rollback transaction to my_savepoint foo"));
    assertThrows(PGException.class, () -> parse("rollback work to foo.bar"));
    assertThrows(PGException.class, () -> parse("rollback to savepoint (foo)"));
  }
}
