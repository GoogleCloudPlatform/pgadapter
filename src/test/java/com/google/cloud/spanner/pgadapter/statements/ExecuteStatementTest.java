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

import static com.google.cloud.spanner.pgadapter.statements.ExecuteStatement.parse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecuteStatementTest {

  @Test
  public void testGetStatementType() {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    BackendConnection backendConnection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(backendConnection.getSessionState()).thenReturn(sessionState);
    assertEquals(
        StatementType.CLIENT_SIDE,
        new ExecuteStatement(
                connectionHandler,
                mock(OptionsMetadata.class),
                "my-statement",
                AbstractStatementParser.getInstance(Dialect.POSTGRESQL)
                    .parse(Statement.of("execute foo")),
                Statement.of("execute foo"))
            .getStatementType());
  }

  @Test
  public void testParse() {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("+01:00"));

    assertEquals("foo", parse("execute foo", sessionState).name);
    assertEquals("foo", parse("execute FOO", sessionState).name);
    assertEquals("foo", parse("execute\tfoo", sessionState).name);
    assertEquals("foo", parse("execute\nfoo", sessionState).name);
    assertEquals("foo", parse("execute/*comment*/foo", sessionState).name);
    assertEquals("foo", parse("execute \"foo\"", sessionState).name);
    assertEquals("Foo", parse("execute \"Foo\"", sessionState).name);
    assertEquals("foo", parse("execute foo (1)", sessionState).name);
    assertEquals("foo", parse("execute foo (1, 'test')", sessionState).name);

    assertArrayEquals(
        new byte[][] {param("1"), param("test")},
        parse("execute foo (1, 'test')", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {param("3.14"), param("\\x55aa")},
        parse("execute foo (3.14, '\\x55aa')", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {null, param("2000-01-01")},
        parse("execute foo (null, '2000-01-01')", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {param("1"), param("test")},
        parse("execute foo (cast(1 as int), varchar 'test')", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {param("1"), param("test")},
        parse("execute foo (1::bigint, varchar 'test')", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {param("1"), param("test")},
        parse("execute foo (1.0::bigint, e'test')", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {param("1"), param("2022-10-09")},
        parse("execute foo (1.0::int, '2022-10-09'::date)", sessionState).parameters);
    assertArrayEquals(
        new byte[][] {param("1"), param("2022-10-09 10:09:18+01")},
        parse("execute foo (1.0::int4, '2022-10-09 10:09:18'::timestamptz)", sessionState)
            .parameters);

    assertThrows(PGException.class, () -> parse("foo", sessionState));
    assertThrows(PGException.class, () -> parse("execute", sessionState));
    assertThrows(PGException.class, () -> parse("execute foo bar", sessionState));
    assertThrows(PGException.class, () -> parse("execute foo.bar", sessionState));
    assertThrows(PGException.class, () -> parse("execute foo ()", sessionState));
    assertThrows(PGException.class, () -> parse("execute foo (1) bar", sessionState));
    assertThrows(PGException.class, () -> parse("execute foo (1", sessionState));
  }

  static byte[] param(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
