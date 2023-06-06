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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
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
      assertEquals(10, parse(type, "%s absolute 10 foo").count.intValue());
      assertThrows(PGException.class, () -> parse(type, "%s absolute 99.4 foo"));
      assertThrows(PGException.class, () -> parse(type, "%s absolute foo"));
      assertEquals(Direction.RELATIVE, parse(type, "%s relative 10 foo").direction);
      assertThrows(PGException.class, () -> parse(type, "%s relative 99.4 foo"));
      assertEquals(10, parse(type, "%s relative 10 foo").count.intValue());
      assertThrows(PGException.class, () -> parse(type, "%s relative foo"));

      assertEquals(1, parse(type, "%s 1 foo").count.intValue());
      assertEquals(1, parse(type, "%s +1 foo").count.intValue());
      assertEquals(1, parse(type, "%s 1 \"my-cursor\"").count.intValue());
      assertEquals(99, parse(type, "%s   --comment\n99/*comment*/foo").count.intValue());
      assertEquals(
          Integer.MAX_VALUE, parse(type, "%s " + Integer.MAX_VALUE + " foo").count.intValue());
      assertEquals(-1, parse(type, "%s -1 foo").count.intValue());
      assertEquals(-1, parse(type, "%s relative -1 foo").count.intValue());
      assertEquals(-1, parse(type, "%s absolute -1 foo").count.intValue());
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
      assertThrows(PGException.class, () -> parse(type, "%s $1"));
      assertThrows(PGException.class, () -> parse(type, "%s foo.bar"));
      assertThrows(
          PGException.class, () -> parse(type, "%s " + ((long) Integer.MAX_VALUE + 1L) + " foo"));
      assertThrows(
          PGException.class, () -> parse(type, "%s " + ((long) Integer.MIN_VALUE - 1L) + " foo"));
    }
    assertEquals(ParsedFetchStatement.class, parse("fetch", "%s foo").getClass());
    assertEquals(ParsedMoveStatement.class, parse("move", "%s foo").getClass());

    assertThrows(PGException.class, () -> parse("fetch", "move foo"));
    assertThrows(PGException.class, () -> parse("move", "fetch foo"));
    assertThrows(PGException.class, () -> parse("fetch", "select foo"));
    assertThrows(PGException.class, () -> parse("fetch", "fetch invalid_direction foo"));
  }

  @Test
  public void testFetchStatement() throws Exception {
    Statement statement = Statement.of("fetch c1");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ConnectionMetadata metadata = mock(ConnectionMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(metadata);
    FetchStatement fetchStatement =
        new FetchStatement(
            connectionHandler, mock(OptionsMetadata.class), "", parsedStatement, statement);
    assertEquals(StatementType.CLIENT_SIDE, fetchStatement.getStatementType());
    assertNull(fetchStatement.describeAsync(mock(BackendConnection.class)).get());
  }

  private static class InvalidFetchOrMoveStatement extends ParsedFetchOrMoveStatement {
    InvalidFetchOrMoveStatement() {
      super(null, null, null);
    }
  }

  @Test
  public void testInvalidParseClass() {
    PGException exception =
        assertThrows(
            PGException.class,
            () ->
                AbstractFetchOrMoveStatement.parse(
                    "fetch foo", "fetch", InvalidFetchOrMoveStatement.class));
    assertEquals(SQLState.InternalError, exception.getSQLState());
  }

  ParsedFetchOrMoveStatement parse(String type, String expression) {
    Class<? extends ParsedFetchOrMoveStatement> clazz =
        "fetch".equalsIgnoreCase(type) ? ParsedFetchStatement.class : ParsedMoveStatement.class;
    return AbstractFetchOrMoveStatement.parse(String.format(expression, type), type, clazz);
  }
}
