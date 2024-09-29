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

import static com.google.cloud.spanner.pgadapter.statements.ShutdownStatement.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.ProxyServer.ShutdownMode;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.ResultNotReadyBehavior;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShutdownStatementTest {

  @Test
  public void testParseShutdown() {
    ProxyServer server = mock(ProxyServer.class);

    assertEquals(ShutdownMode.FAST, parse("shutdown", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("Shutdown", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("SHUTDOWN", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("\t shutdown \n", server).shutdownMode);
    assertEquals(ShutdownMode.IMMEDIATE, parse("shutdown immediate", server).shutdownMode);
    assertEquals(ShutdownMode.SMART, parse("shutdown smart", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("shutdown fast", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("shutdown\tfast", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("shutdown\nfast", server).shutdownMode);
    assertEquals(ShutdownMode.FAST, parse("shutdown\n\t   fast", server).shutdownMode);

    PGException exception;
    exception = assertThrows(PGException.class, () -> parse("shutdown fast immediate", server));
    assertEquals(SQLState.SyntaxError, exception.getSQLState());
    exception = assertThrows(PGException.class, () -> parse("shutdown foo", server));
    assertEquals(SQLState.SyntaxError, exception.getSQLState());
    exception = assertThrows(PGException.class, () -> parse("shutdown foo fast", server));
    assertEquals(SQLState.SyntaxError, exception.getSQLState());
    exception = assertThrows(PGException.class, () -> parse("shutdown fast,", server));
    assertEquals(SQLState.SyntaxError, exception.getSQLState());
    exception = assertThrows(PGException.class, () -> parse("fast shutdown", server));
    assertEquals(SQLState.SyntaxError, exception.getSQLState());
  }

  @Test
  public void testShutdownStatement_usesDefaultShutdownMode() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.isAllowShutdownStatement()).thenReturn(true);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));

    String sql = "shutdown";
    Statement originalStatement = Statement.of(sql);
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(originalStatement);
    ShutdownStatement statement =
        new ShutdownStatement(
            connectionHandler, options, /* name= */ "", parsedStatement, originalStatement);

    statement.executeAsync(mock(BackendConnection.class));
    statement.close();

    // PGAdapter uses FAST shutdown mode by default.
    // We need to use a timeout, as the shutdown handler runs async. The wait time is normally much
    // less than the 1000 milliseconds that is used as the timeout value.
    verify(server, timeout(1000L)).stopServer(ShutdownMode.FAST);
  }

  @Test
  public void testShutdownStatement_usesParsedShutdownMode() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.isAllowShutdownStatement()).thenReturn(true);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));

    String sql = "shutdown smart";
    Statement originalStatement = Statement.of(sql);
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(originalStatement);
    ShutdownStatement statement =
        new ShutdownStatement(
            connectionHandler, options, /* name= */ "", parsedStatement, originalStatement);

    statement.executeAsync(mock(BackendConnection.class));
    statement.close();

    // The shutdown mode from the SQL statement should be used.
    // We need to use a timeout, as the shutdown handler runs async. The wait time is normally much
    // less than the 1000 milliseconds that is used as the timeout value.
    verify(server, timeout(1000L)).stopServer(ShutdownMode.SMART);
  }

  @Test
  public void testShutdownStatement_returnsErrorIfNotEnabled() throws Exception {
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.isAllowShutdownStatement()).thenReturn(false);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));

    String sql = "shutdown";
    Statement originalStatement = Statement.of(sql);
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(originalStatement);
    ShutdownStatement statement =
        new ShutdownStatement(
            connectionHandler, options, /* name= */ "", parsedStatement, originalStatement);

    // Execute the statement and then get the result.
    statement.executeAsync(mock(BackendConnection.class));
    statement.close();
    // The result should be an exception.
    statement.initFutureResult(ResultNotReadyBehavior.FAIL);
    assertNotNull(statement.getException());
    assertEquals(
        "SHUTDOWN [SMART | FAST | IMMEDIATE] statement is not enabled for this server. "
            + "Start PGAdapter with --allow_shutdown_statement to enable the use of the SHUTDOWN statement.",
        statement.getException().getMessage());

    // The server should not be shut down.
    verify(server, never()).stopServer(any(ShutdownMode.class));
  }
}
