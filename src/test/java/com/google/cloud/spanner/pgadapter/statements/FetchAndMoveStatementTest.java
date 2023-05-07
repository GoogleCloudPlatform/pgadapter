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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.AbstractFetchOrMoveStatement.Direction;
import com.google.cloud.spanner.pgadapter.statements.AbstractFetchOrMoveStatement.ParsedFetchOrMoveStatement;
import com.google.cloud.spanner.pgadapter.statements.FetchStatement.ParsedFetchStatement;
import com.google.cloud.spanner.pgadapter.statements.MoveStatement.ParsedMoveStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FetchAndMoveStatementTest {
  @Test
  public void testParse() {
    for (String type : new String[] {"fetch", "move"}) {
      ParsedFetchOrMoveStatement statement = parse(type, "%s foo");
      assertEquals("foo", statement.name);
      assertNull(statement.direction);
      assertNull(statement.count);

      assertEquals("foo", parse(type, "%s foo").name);
      assertEquals("foo", parse(type, "%s \"foo\"").name);
      assertEquals("my-cursor", parse(type, "%s \"my-cursor\"").name);
      assertEquals("foo", parse(type, "%s from foo").name);
      assertEquals("foo", parse(type, "%s in foo").name);
      assertThrows(PGException.class, () -> parse(type, "%s from in foo"));

      assertEquals(Direction.ABSOLUTE, parse(type, "%s absolute 10 foo").direction);
      assertEquals(10L, parse(type, "%s absolute 10 foo").count.longValue());
      assertThrows(PGException.class, () -> parse(type, "%s absolute 99.4 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s absolute foo"));
      assertEquals(Direction.RELATIVE, parse(type, "%s relative 10 foo").direction);
      assertThrows(PGException.class, () -> parse(type, "%s relative 99.4 foo"));
      assertEquals(10L, parse(type, "%s relative 10 foo").count.longValue());
      assertThrows(PGException.class, () -> parse(type, "%s relative foo"));

      assertEquals(1L, parse(type, "%s 1 foo").count.longValue());
      assertEquals(1L, parse(type, "%s +1 foo").count.longValue());
      assertEquals(1L, parse(type, "%s 1 \"my-cursor\"").count.longValue());
      assertEquals(99L, parse(type, "%s   --comment\n99/*comment*/foo").count.longValue());
      assertEquals(Long.MAX_VALUE, parse(type, "%s " + Long.MAX_VALUE + " foo").count.longValue());
      assertEquals(-1L, parse(type, "%s -1 foo").count.longValue());
      assertEquals(-1L, parse(type, "%s relative -1 foo").count.longValue());
      assertEquals(-1L, parse(type, "%s absolute -1 foo").count.longValue());
      assertThrows(PGException.class, () -> parse(type, "%s 99.9 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s +inf foo"));
      assertThrows(PGException.class, () -> parse(type, "%s -inf foo"));

      assertEquals(Direction.ALL, parse(type, "%s all foo").direction);
      assertEquals(Direction.BACKWARD, parse(type, "%s backward foo").direction);
      assertEquals(Direction.FORWARD, parse(type, "%s forward foo").direction);
      assertEquals(Direction.FIRST, parse(type, "%s first foo").direction);
      assertEquals(Direction.NEXT, parse(type, "%s next foo").direction);
      assertEquals(Direction.BACKWARD_ALL, parse(type, "%s backward all foo").direction);
      assertEquals(Direction.FORWARD_ALL, parse(type, "%s forward all foo").direction);
      assertEquals(Direction.LAST, parse(type, "%s last foo").direction);
      assertEquals(Direction.PRIOR, parse(type, "%s prior foo").direction);

      assertThrows(PGException.class, () -> parse(type, "%s all 1 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s first 1 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s next 1 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s last 1 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s prior 1 foo"));
    }
    assertEquals(ParsedFetchStatement.class, parse("fetch", "%s foo").getClass());
    assertEquals(ParsedMoveStatement.class, parse("move", "%s foo").getClass());
  }

  ParsedFetchOrMoveStatement parse(String type, String expression) {
    Class<? extends ParsedFetchOrMoveStatement> clazz =
        "fetch".equalsIgnoreCase(type) ? ParsedFetchStatement.class : ParsedMoveStatement.class;
    return AbstractFetchOrMoveStatement.parse(String.format(expression, type), type, clazz);
  }
}
