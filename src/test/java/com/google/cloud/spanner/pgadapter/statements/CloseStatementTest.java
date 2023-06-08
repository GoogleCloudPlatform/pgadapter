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

import static com.google.cloud.spanner.pgadapter.statements.CloseStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.util.concurrent.Future;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloseStatementTest {

  @Test
  public void testParse() {
    assertEquals("foo", parse("close foo").name);
    assertEquals("foo", parse("close FOO").name);
    assertEquals("foo", parse("close\tfoo").name);
    assertEquals("foo", parse("close\nfoo").name);
    assertEquals("foo", parse("close/*comment*/foo").name);
    assertEquals("foo", parse("close \"foo\"").name);
    assertEquals("Foo", parse("close \"Foo\"").name);
    assertEquals("prepare", parse("close \"prepare\"").name);
    assertNull(parse("close all").name);
    assertEquals("all", parse("close \"all\"").name);

    assertThrows(PGException.class, () -> parse("prepare foo"));
    assertThrows(PGException.class, () -> parse("close"));
    assertThrows(PGException.class, () -> parse("close foo bar"));
    assertThrows(PGException.class, () -> parse("close foo.bar"));
  }

  @Test
  public void testCloseNamed() throws Exception {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    Statement statement = Statement.of("close foo");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    CloseStatement closeStatement =
        new CloseStatement(connectionHandler, optionsMetadata, "", parsedStatement, statement);
    assertFalse(closeStatement.executed);
    closeStatement.executeAsync(mock(BackendConnection.class));
    assertTrue(closeStatement.executed);
    verify(connectionHandler).closePortal("foo");
  }

  @Test
  public void testCloseAll() {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    Statement statement = Statement.of("close all");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    CloseStatement closeStatement =
        new CloseStatement(connectionHandler, optionsMetadata, "", parsedStatement, statement);
    assertFalse(closeStatement.executed);
    closeStatement.executeAsync(mock(BackendConnection.class));
    assertTrue(closeStatement.executed);
    verify(connectionHandler).closeAllPortals();
  }

  @Test
  public void testCloseFails() throws Exception {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    Statement statement = Statement.of("close foo");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    CloseStatement closeStatement =
        new CloseStatement(connectionHandler, optionsMetadata, "", parsedStatement, statement);

    doThrow(
            PGExceptionFactory.newPGException(
                "unrecognized portal name: foo", SQLState.InvalidCursorName))
        .when(connectionHandler)
        .closePortal("foo");
    assertFalse(closeStatement.executed);
    closeStatement.executeAsync(mock(BackendConnection.class));
    assertTrue(closeStatement.hasException());
    assertEquals(SQLState.InvalidCursorName, closeStatement.getException().getSQLState());
    assertTrue(closeStatement.executed);
  }

  @Test
  public void testDescribeIsNoOp() throws Exception {
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    Statement statement = Statement.of("close foo");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    CloseStatement closeStatement =
        new CloseStatement(connectionHandler, optionsMetadata, "", parsedStatement, statement);
    Future<StatementResult> result = closeStatement.describeAsync(mock(BackendConnection.class));
    assertNull(result.get());
  }
}
