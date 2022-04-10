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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.ResultType;
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
import java.util.List;
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
    when(connection.execute(Statement.of("SELECT * FROM users"))).thenReturn(statementResult);
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.RESULT_SET);
    when(statementResult.getResultSet()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse("SELECT * FROM users"), connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("SELECT", intermediateStatement.getCommand());

    intermediateStatement.execute();

    verify(connection, Mockito.times(1)).execute(Statement.of("SELECT * FROM users"));
    assertTrue(intermediateStatement.containsResultSet());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(ResultType.RESULT_SET, intermediateStatement.getResultType());
    assertEquals(resultSet, intermediateStatement.getStatementResult());
    assertTrue(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet).close();
  }

  @Test
  public void testBasicUpdateStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(1L);
    when(connection.execute(Statement.of("UPDATE users SET name = someName WHERE id = 10")))
        .thenReturn(statementResult);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, parse("UPDATE users SET name = someName WHERE id = 10"), connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand());

    intermediateStatement.execute();

    verify(connection, Mockito.times(1))
        .execute(Statement.of("UPDATE users SET name = someName WHERE id = 10"));
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals(1L, intermediateStatement.getUpdateCount().longValue());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(ResultType.UPDATE_COUNT, intermediateStatement.getResultType());
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet, never()).close();
  }

  @Test
  public void testBasicZeroUpdateCountResultStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(0L);
    when(connection.execute(Statement.of("UPDATE users SET name = someName WHERE id = -1")))
        .thenReturn(statementResult);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, parse("UPDATE users SET name = someName WHERE id = -1"), connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand());

    intermediateStatement.execute();

    verify(connection).execute(Statement.of("UPDATE users SET name = someName WHERE id = -1"));
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals(0L, intermediateStatement.getUpdateCount().longValue());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(ResultType.UPDATE_COUNT, intermediateStatement.getResultType());
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet, never()).close();
  }

  @Test
  public void testBasicNoResultStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.NO_RESULT);
    when(connection.execute(Statement.of("CREATE TABLE users (name varchar(100) primary key)")))
        .thenReturn(statementResult);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, parse("CREATE TABLE users (name varchar(100) primary key)"), connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("CREATE", intermediateStatement.getCommand());

    intermediateStatement.execute();

    verify(connection).execute(Statement.of("CREATE TABLE users (name varchar(100) primary key)"));
    assertFalse(intermediateStatement.containsResultSet());
    assertNull(intermediateStatement.getUpdateCount());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(ResultType.NO_RESULT, intermediateStatement.getResultType());
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet, never()).close();
  }

  @Test
  public void testDescribeBasicStatementThrowsException() {
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse("SELECT * FROM users"), connection);

    assertThrows(IllegalStateException.class, intermediateStatement::describe);
  }

  @Test
  public void testBasicStatementExceptionGetsSetOnExceptedExecution() {
    SpannerException thrownException =
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error");

    when(connection.execute(Statement.of("SELECT * FROM users"))).thenThrow(thrownException);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse("SELECT * FROM users"), connection);

    intermediateStatement.execute();

    assertTrue(intermediateStatement.hasException());
    assertEquals(thrownException, intermediateStatement.getException());
  }

  @Test
  public void testPreparedStatement() {
    String sqlStatement = "SELECT * FROM users WHERE age > $2 AND age < $3 AND name = $1";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.VARCHAR, Oid.INT8, Oid.INT4);

    Statement statement =
        Statement.newBuilder(sqlStatement)
            .bind("p1")
            .to("userName")
            .bind("p2")
            .to(20L)
            .bind("p3")
            .to(30)
            .build();
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(0L);
    when(connection.execute(statement)).thenReturn(statementResult);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(options, parse(sqlStatement), connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    assertEquals(sqlStatement, intermediateStatement.getSql());

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.bind(
            parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());
    intermediateStatement.execute();
    verify(connection).execute(statement);

    assertEquals(sqlStatement, intermediatePortalStatement.getSql());
    assertEquals("SELECT", intermediatePortalStatement.getCommand());
    assertFalse(intermediatePortalStatement.isExecuted());
    assertTrue(intermediateStatement.isBound());
  }

  @Test
  public void testPreparedStatementIllegalTypeThrowsException() {
    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    List<Integer> parameterDataTypes = Collections.singletonList(Oid.JSON);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(options, parse(sqlStatement), connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    byte[][] parameters = {"{}".getBytes()};

    assertThrows(
        IllegalArgumentException.class,
        () -> intermediateStatement.bind(parameters, new ArrayList<>(), new ArrayList<>()));
  }

  @Test
  public void testPreparedStatementDescribeDoesNotThrowException() {
    String sqlStatement = "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3";
    when(connection.analyzeQuery(Statement.of(sqlStatement), QueryAnalyzeMode.PLAN))
        .thenReturn(resultSet);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(options, parse(sqlStatement), connection);

    intermediateStatement.describe();
  }

  @Test
  public void testPortalStatement() {
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";
    when(connection.executeQuery(Statement.of(sqlStatement))).thenReturn(resultSet);

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(options, parse(sqlStatement), connection);

    intermediateStatement.describe();

    verify(connection).executeQuery(Statement.of(sqlStatement));

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
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(options, parse(sqlStatement), connection);

    when(connection.executeQuery(Statement.of(sqlStatement)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error"));

    SpannerException exception =
        assertThrows(SpannerException.class, intermediateStatement::describe);
    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
  }

  @Test
  public void testBatchStatements() {
    String sql =
        "INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2);INSERT INTO users (id) VALUES (3);";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(3, result.size(), 3);
    assertEquals("INSERT INTO users (id) VALUES (1)", result.get(0));
    assertEquals("INSERT INTO users (id) VALUES (2)", result.get(1));
    assertEquals("INSERT INTO users (id) VALUES (3)", result.get(2));
  }

  @Test
  public void testAdditionalBatchStatements() {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2); INSERT INTO users (id) VALUES (3); COMMIT;";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();

    assertEquals(5, result.size(), 5);
    assertEquals("BEGIN TRANSACTION", result.get(0));
    assertEquals("INSERT INTO users (id) VALUES (1)", result.get(1));
    assertEquals("INSERT INTO users (id) VALUES (2)", result.get(2));
    assertEquals("INSERT INTO users (id) VALUES (3)", result.get(3));
    assertEquals("COMMIT", result.get(4));
  }

  @Test
  public void testBatchStatementsWithEmptyStatements() {
    String sql = "INSERT INTO users (id) VALUES (1); ;;; INSERT INTO users (id) VALUES (2);";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(2, result.size());
    assertEquals("INSERT INTO users (id) VALUES (1)", result.get(0));
    assertEquals("INSERT INTO users (id) VALUES (2)", result.get(1));
  }

  @Test
  public void testBatchStatementsWithQuotes() {
    String sql =
        "INSERT INTO users (name) VALUES (';;test;;'); INSERT INTO users (name1, name2) VALUES ('''''', ';'';');";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(2, result.size());
    assertEquals("INSERT INTO users (name) VALUES (';;test;;')", result.get(0));
    assertEquals("INSERT INTO users (name1, name2) VALUES ('''''', ';'';')", result.get(1));
  }

  @Test
  public void testBatchStatementsWithComments() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) 132};
    String payload =
        "INSERT INTO users (name) VALUES (';;test;;'); /* Comment;; Comment; */INSERT INTO users (name1, name2) VALUES ('''''', ';'';');\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    when(options.requiresMatcher()).thenReturn(false);
    when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    assertEquals(QueryMessage.class, message.getClass());
    IntermediateStatement intermediateStatement = ((QueryMessage) message).getStatement();

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(2, result.size());
    assertEquals("INSERT INTO users (name) VALUES (';;test;;')", result.get(0));
    assertEquals("INSERT INTO users (name1, name2) VALUES ('''''', ';'';')", result.get(1));
  }

  @Test
  public void testCopyInvalidBuildMutation() throws Exception {
    setupQueryInformationSchemaResults();

    CopyStatement statement =
        new CopyStatement(
            mock(OptionsMetadata.class), parse("COPY keyvalue FROM STDIN;"), connection);
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
