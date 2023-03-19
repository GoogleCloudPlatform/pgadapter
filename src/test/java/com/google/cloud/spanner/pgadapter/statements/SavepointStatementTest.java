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

import static com.google.cloud.spanner.pgadapter.statements.SavepointStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SavepointStatementTest {

  @Test
  public void testParse() {
    assertEquals("foo", parse("savepoint foo").name);
    assertEquals("foo", parse("savepoint FOO").name);
    assertEquals("foo", parse("savepoint\tfoo").name);
    assertEquals("foo", parse("savepoint\nfoo").name);
    assertEquals("foo", parse("savepoint/*comment*/foo").name);
    assertEquals("foo", parse("savepoint \"foo\"").name);
    assertEquals("Foo", parse("savepoint \"Foo\"").name);
    assertEquals("savepoint", parse("savepoint \"savepoint\"").name);
    assertEquals("foo", parse("/* this will set a savepoint */ savepoint foo").name);

    assertThrows(PGException.class, () -> parse("svepoint foo"));
    assertThrows(PGException.class, () -> parse("savepoint"));
    assertThrows(PGException.class, () -> parse("savepoint my_savepoint foo"));
    assertThrows(PGException.class, () -> parse("savepoint foo.bar"));
    assertThrows(PGException.class, () -> parse("savepoint (foo)"));
  }
}
