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
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecuteStatementTest {

  @Test
  public void testGetStatementType() {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
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
    assertEquals("foo", parse("execute foo").name);
    assertEquals("foo", parse("execute FOO").name);
    assertEquals("foo", parse("execute\tfoo").name);
    assertEquals("foo", parse("execute\nfoo").name);
    assertEquals("foo", parse("execute/*comment*/foo").name);
    assertEquals("foo", parse("execute \"foo\"").name);
    assertEquals("Foo", parse("execute \"Foo\"").name);
    assertEquals("foo", parse("execute foo (1)").name);
    assertEquals("foo", parse("execute foo (1, 'test')").name);

    assertArrayEquals(
        new byte[][] {param("1"), param("test")}, parse("execute foo (1, 'test')").parameters);
    assertArrayEquals(
        new byte[][] {param("3.14"), param("\\x55aa")},
        parse("execute foo (3.14, '\\x55aa')").parameters);
    assertArrayEquals(
        new byte[][] {null, param("2000-01-01")},
        parse("execute foo (null, '2000-01-01')").parameters);

    assertThrows(PGException.class, () -> parse("foo"));
    assertThrows(PGException.class, () -> parse("execute"));
    assertThrows(PGException.class, () -> parse("execute foo bar"));
    assertThrows(PGException.class, () -> parse("execute foo.bar"));
    assertThrows(PGException.class, () -> parse("execute foo ()"));
    assertThrows(PGException.class, () -> parse("execute foo (1) bar"));
    assertThrows(PGException.class, () -> parse("execute foo (1"));
  }

  static byte[] param(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
