// Copyright 2020 Google LLC
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

import static com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.EMPTY_LOCAL_STATEMENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class StatementTest {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  private static ParsedStatement parse(String sql) {
    return PARSER.parse(Statement.of(sql));
  }

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private Connection connection;
  @Mock private ConnectionHandler connectionHandler;
  @Mock private ConnectionMetadata connectionMetadata;
  @Mock private ExtendedQueryProtocolHandler extendedQueryProtocolHandler;
  @Mock private BackendConnection backendConnection;
  @Mock private ProxyServer server;
  @Mock private OptionsMetadata options;
  @Mock private StatementResult statementResult;
  @Mock private ResultSet resultSet;
  @Mock private DataOutputStream outputStream;

  @AfterClass
  public static void cleanup() {
    // TODO: Make error log file configurable and turn off writing to a file during tests.
    File outputFile = new File("output.txt");
    outputFile.delete();
  }

  @Test
  public void testBasicSelectStatement() throws Exception {
    String sql = "SELECT * FROM users";
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sql), Statement.of(sql));

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("SELECT", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);

    verify(backendConnection).execute(parse(sql), Statement.of(sql));
    assertTrue(intermediateStatement.containsResultSet());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.QUERY, intermediateStatement.getStatementType());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();
  }

  @Test
  public void testBasicUpdateStatement() throws Exception {
    String sql = "UPDATE users SET name = someName WHERE id = 10";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sql), Statement.of(sql));

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);

    verify(backendConnection).execute(parse(sql), Statement.of(sql));
    assertFalse(intermediateStatement.containsResultSet());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.UPDATE, intermediateStatement.getStatementType());
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet, never()).close();
  }

  @Test
  public void testBasicZeroUpdateCountResultStatement() throws Exception {
    String sql = "UPDATE users SET name = someName WHERE id = -1";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.execute(Statement.of(sql))).thenReturn(statementResult);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sql), Statement.of(sql));
    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, EMPTY_LOCAL_STATEMENTS);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);
    backendConnection.flush();

    assertFalse(intermediateStatement.containsResultSet());
    assertEquals(0L, intermediateStatement.getUpdateCount());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.UPDATE, intermediateStatement.getStatementType());
    assertNotNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet, never()).close();
  }

  @Test
  public void testBasicNoResultStatement() throws Exception {
    String sql = "CREATE TABLE users (name varchar(100) primary key)";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sql), Statement.of(sql));

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("CREATE", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);

    verify(backendConnection).execute(parse(sql), Statement.of(sql));
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals(0, intermediateStatement.getUpdateCount());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.DDL, intermediateStatement.getStatementType());
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    Mockito.verify(resultSet, never()).close();
  }

  @Test
  public void testDescribeBasicStatementThrowsException() {
    String sql = "SELECT * FROM users";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), Statement.of(sql), connectionHandler);

    assertThrows(IllegalStateException.class, intermediateStatement::describe);
  }

  @Test
  public void testBasicStatementExceptionGetsSetOnExceptedExecution() {
    String sql = "SELECT * FROM users";
    SpannerException thrownException =
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error");

    when(connection.execute(Statement.of(sql))).thenThrow(thrownException);
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sql), Statement.of(sql));
    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, EMPTY_LOCAL_STATEMENTS);

    intermediateStatement.executeAsync(backendConnection);
    backendConnection.flush();

    assertTrue(intermediateStatement.hasException());
    assertEquals(thrownException, intermediateStatement.getException());
  }

  @Test
  public void testPreparedStatement() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    String sqlStatement = "SELECT * FROM users WHERE age > $2 AND age < $3 AND name = $1";
    int[] parameterDataTypes = new int[] {Oid.VARCHAR, Oid.INT8, Oid.INT4};

    Statement statement =
        Statement.newBuilder(sqlStatement)
            .bind("p1")
            .to("userName")
            .bind("p2")
            .to(20L)
            .bind("p3")
            .to(30)
            .build();
    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, EMPTY_LOCAL_STATEMENTS);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(
            connectionHandler, options, "", parse(sqlStatement), Statement.of(sqlStatement));
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    assertEquals(sqlStatement, intermediateStatement.getSql());

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.bind(
            "", parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());
    intermediateStatement.executeAsync(backendConnection);
    backendConnection.flush();

    verify(connection).execute(statement);

    assertEquals(sqlStatement, intermediatePortalStatement.getSql());
    assertEquals("SELECT", intermediatePortalStatement.getCommand());
    assertFalse(intermediatePortalStatement.isExecuted());
    assertTrue(intermediateStatement.isBound());
  }

  @Test
  public void testPreparedStatementIllegalTypeThrowsException() {
    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    int[] parameterDataTypes = new int[] {Oid.JSON};

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(
            connectionHandler, options, "", parse(sqlStatement), Statement.of(sqlStatement));
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    byte[][] parameters = {"{}".getBytes()};

    assertThrows(
        IllegalArgumentException.class,
        () -> intermediateStatement.bind("", parameters, new ArrayList<>(), new ArrayList<>()));
  }

  @Test
  public void testPreparedStatementDescribeDoesNotThrowException() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    String sqlStatement = "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3";
    when(connection.analyzeQuery(Statement.of(sqlStatement), QueryAnalyzeMode.PLAN))
        .thenReturn(resultSet);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(
            connectionHandler, options, "", parse(sqlStatement), Statement.of(sqlStatement));
    int[] parameters = new int[3];
    Arrays.fill(parameters, Oid.INT8);
    intermediateStatement.setParameterDataTypes(parameters);

    intermediateStatement.describe();
  }

  @Test
  public void testPortalStatement() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sqlStatement), Statement.of(sqlStatement));
    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, EMPTY_LOCAL_STATEMENTS);

    intermediateStatement.describeAsync(backendConnection);
    backendConnection.flush();

    verify(connection).execute(Statement.of(sqlStatement));

    assertEquals(0, intermediateStatement.getParameterFormatCode(0));
    assertEquals(0, intermediateStatement.getParameterFormatCode(1));
    assertEquals(0, intermediateStatement.getParameterFormatCode(2));
    assertEquals(0, intermediateStatement.getResultFormatCode(0));
    assertEquals(0, intermediateStatement.getResultFormatCode(1));
    assertEquals(0, intermediateStatement.getResultFormatCode(2));

    intermediateStatement.setParameterFormatCodes(Collections.singletonList((short) 1));
    intermediateStatement.setResultFormatCodes(Collections.singletonList((short) 1));

    assertEquals(1, intermediateStatement.getParameterFormatCode(0));
    assertEquals(1, intermediateStatement.getParameterFormatCode(1));
    assertEquals(1, intermediateStatement.getParameterFormatCode(2));
    assertEquals(1, intermediateStatement.getResultFormatCode(0));
    assertEquals(1, intermediateStatement.getResultFormatCode(1));
    assertEquals(1, intermediateStatement.getResultFormatCode(2));

    intermediateStatement.setParameterFormatCodes(Arrays.asList((short) 0, (short) 1, (short) 0));
    intermediateStatement.setResultFormatCodes(Arrays.asList((short) 0, (short) 1, (short) 0));

    assertEquals(0, intermediateStatement.getParameterFormatCode(0));
    assertEquals(1, intermediateStatement.getParameterFormatCode(1));
    assertEquals(0, intermediateStatement.getParameterFormatCode(2));
    assertEquals(0, intermediateStatement.getResultFormatCode(0));
    assertEquals(1, intermediateStatement.getResultFormatCode(1));
    assertEquals(0, intermediateStatement.getResultFormatCode(2));
  }

  @Test
  public void testPortalStatementDescribePropagatesFailure() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sqlStatement), Statement.of(sqlStatement));
    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, EMPTY_LOCAL_STATEMENTS);

    when(connection.execute(Statement.of(sqlStatement)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error"));

    intermediateStatement.describeAsync(backendConnection);
    backendConnection.flush();

    assertTrue(intermediateStatement.hasException());
    SpannerException exception = (SpannerException) intermediateStatement.getException();
    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
  }

  @Test
  public void testBatchStatementsWithComments() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) 132};
    String payload =
        "INSERT INTO users (name) VALUES (';;test;;'); /* Comment;; Comment; */INSERT INTO users (name1, name2) VALUES ('''''', ';'';');\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    assertEquals(QueryMessage.class, message.getClass());
    SimpleQueryStatement simpleQueryStatement = ((QueryMessage) message).getSimpleQueryStatement();

    assertEquals(2, simpleQueryStatement.getStatements().size());
    assertEquals(
        "INSERT INTO users (name) VALUES (';;test;;')", simpleQueryStatement.getStatement(0));
    assertEquals(
        "/* Comment;; Comment; */INSERT INTO users (name1, name2) VALUES ('''''', ';'';')",
        simpleQueryStatement.getStatement(1));
  }

  @Test
  public void testCopyInvalidBuildMutation() throws Exception {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    setupQueryInformationSchemaResults();

    String sql = "COPY keyvalue FROM STDIN;";
    CopyStatement statement =
        new CopyStatement(
            connectionHandler, mock(OptionsMetadata.class), parse(sql), Statement.of(sql));
    statement.execute();

    byte[] payload = "2 3\n".getBytes();
    MutationWriter mutationWriter = statement.getMutationWriter();
    mutationWriter.addCopyData(payload);

    SpannerException thrown = assertThrows(SpannerException.class, statement::getUpdateCount);
    assertEquals(ErrorCode.INVALID_ARGUMENT, thrown.getErrorCode());
    assertEquals(
        "INVALID_ARGUMENT: Invalid COPY data: Row length mismatched. Expected 2 columns, but only found 1",
        thrown.getMessage());

    statement.close();
    verify(resultSet, never()).close();
  }

  @Test
  public void testIntermediateStatementExecuteAsyncIsUnsupported() {
    String sql = "select * from foo";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), Statement.of(sql), connectionHandler);

    assertThrows(
        UnsupportedOperationException.class,
        () -> intermediateStatement.executeAsync(backendConnection));
  }

  @Test
  public void testIntermediateStatementDescribeAsyncIsUnsupported() {
    String sql = "select * from foo";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), Statement.of(sql), connectionHandler);

    assertThrows(
        UnsupportedOperationException.class,
        () -> intermediateStatement.describeAsync(backendConnection));
  }

  @Test
  public void testGetStatementResultBeforeFlushFails() {
    String sql = "select * from foo";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            connectionHandler, options, "", parse(sql), Statement.of(sql));
    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, EMPTY_LOCAL_STATEMENTS);

    intermediateStatement.executeAsync(backendConnection);

    assertThrows(IllegalStateException.class, intermediateStatement::getStatementResult);
  }

  private void setupQueryInformationSchemaResults() {
    ResultSet spannerType = mock(ResultSet.class);
    when(spannerType.getString("column_name")).thenReturn("key", "value");
    when(spannerType.getString("data_type")).thenReturn("bigint", "character varying");
    when(spannerType.next()).thenReturn(true, true, false);
    when(connection.executeQuery(
            ArgumentMatchers.argThat(
                statement ->
                    statement != null && statement.getSql().startsWith("SELECT column_name"))))
        .thenReturn(spannerType);

    ResultSet countResult = Mockito.mock(ResultSet.class);
    when(countResult.getLong(0)).thenReturn(2L);
    when(countResult.next()).thenReturn(true, false);
    when(connection.executeQuery(
            ArgumentMatchers.argThat(
                statement ->
                    statement != null && statement.getSql().startsWith("SELECT COUNT(*)"))))
        .thenReturn(countResult);
  }
}
