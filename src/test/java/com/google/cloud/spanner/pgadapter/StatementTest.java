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
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
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

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse("SELECT * FROM users"), connectionHandler);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("SELECT", intermediateStatement.getCommand(0));

    intermediateStatement.execute();

    verify(connection).execute(Statement.of("SELECT * FROM users"));
    assertTrue(intermediateStatement.containsResultSet(0));
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.QUERY, intermediateStatement.getStatementType(0));
    assertEquals(resultSet, intermediateStatement.getStatementResult(0));
    assertTrue(intermediateStatement.isHasMoreData(0));
    assertFalse(intermediateStatement.hasException(0));
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

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, parse("UPDATE users SET name = someName WHERE id = 10"), connectionHandler);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand(0));

    intermediateStatement.execute();

    verify(connection).execute(Statement.of("UPDATE users SET name = someName WHERE id = 10"));
    assertFalse(intermediateStatement.containsResultSet(0));
    assertEquals(1L, intermediateStatement.getUpdateCount(0));
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.UPDATE, intermediateStatement.getStatementType(0));
    assertNull(intermediateStatement.getStatementResult(0));
    assertFalse(intermediateStatement.isHasMoreData(0));
    assertFalse(intermediateStatement.hasException(0));
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

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, parse("UPDATE users SET name = someName WHERE id = -1"), connectionHandler);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand(0));

    intermediateStatement.execute();

    Mockito.verify(connection)
        .execute(Statement.of("UPDATE users SET name = someName WHERE id = -1"));
    assertFalse(intermediateStatement.containsResultSet(0));
    assertEquals(0L, intermediateStatement.getUpdateCount(0));
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.UPDATE, intermediateStatement.getStatementType(0));
    assertNull(intermediateStatement.getStatementResult(0));
    assertFalse(intermediateStatement.isHasMoreData(0));
    assertFalse(intermediateStatement.hasException(0));
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    verify(resultSet, never()).close();
  }

  @Test
  public void testBasicNoResultStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.NO_RESULT);
    when(connection.execute(Statement.of("CREATE TABLE users (name varchar(100) primary key)")))
        .thenReturn(statementResult);

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options,
            parse("CREATE TABLE users (name varchar(100) primary key)"),
            connectionHandler);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("CREATE", intermediateStatement.getCommand(0));

    intermediateStatement.execute();

    verify(connection).execute(Statement.of("CREATE TABLE users (name varchar(100) primary key)"));
    assertFalse(intermediateStatement.containsResultSet(0));
    assertEquals(0, intermediateStatement.getUpdateCount(0));
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(StatementType.DDL, intermediateStatement.getStatementType(0));
    assertNull(intermediateStatement.getStatementResult(0));
    assertFalse(intermediateStatement.isHasMoreData(0));
    assertFalse(intermediateStatement.hasException(0));
    assertEquals(0, intermediateStatement.getResultFormatCode(0));

    intermediateStatement.close();

    Mockito.verify(resultSet, never()).close();
  }

  @Test
  public void testDescribeBasicStatementThrowsException() {
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse("SELECT * FROM users"), connectionHandler);

    assertThrows(IllegalStateException.class, intermediateStatement::describe);
  }

  @Test
  public void testBasicStatementExceptionGetsSetOnExceptedExecution() {
    SpannerException thrownException =
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error");

    when(connection.execute(Statement.of("SELECT * FROM users"))).thenThrow(thrownException);

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse("SELECT * FROM users"), connectionHandler);

    intermediateStatement.execute();

    assertTrue(intermediateStatement.hasException(0));
    assertEquals(thrownException, intermediateStatement.getException(0));
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
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(0L);
    when(connection.execute(statement)).thenReturn(statementResult);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(connectionHandler, options, "", parse(sqlStatement));
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    assertEquals(sqlStatement, intermediateStatement.getSql());

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.bind(
            "", parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());
    intermediateStatement.execute();
    verify(connection).execute(statement);

    assertEquals(sqlStatement, intermediatePortalStatement.getSql());
    assertEquals("SELECT", intermediatePortalStatement.getCommand(0));
    assertFalse(intermediatePortalStatement.isExecuted());
    assertTrue(intermediateStatement.isBound());
  }

  @Test
  public void testPreparedStatementIllegalTypeThrowsException() {
    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    int[] parameterDataTypes = new int[] {Oid.JSON};

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(connectionHandler, options, "", parse(sqlStatement));
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
        new IntermediatePreparedStatement(connectionHandler, options, "", parse(sqlStatement));
    int[] parameters = new int[3];
    Arrays.fill(parameters, Oid.INT8);
    intermediateStatement.setParameterDataTypes(parameters);

    intermediateStatement.describe();
  }

  @Test
  public void testPortalStatement() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";
    when(connection.executeQuery(Statement.of(sqlStatement))).thenReturn(resultSet);

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(connectionHandler, options, "", parse(sqlStatement));

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
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(connectionHandler, options, "", parse(sqlStatement));

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
        new IntermediateStatement(options, parse(sql), connectionHandler);

    assertTrue(intermediateStatement.isBatchedQuery());
    assertEquals(3, intermediateStatement.getStatements().size(), 3);
    assertEquals("INSERT INTO users (id) VALUES (1)", intermediateStatement.getStatement(0));
    assertEquals("INSERT INTO users (id) VALUES (2)", intermediateStatement.getStatement(1));
    assertEquals("INSERT INTO users (id) VALUES (3)", intermediateStatement.getStatement(2));
  }

  @Test
  public void testAdditionalBatchStatements() {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2); INSERT INTO users (id) VALUES (3); COMMIT;";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connectionHandler);

    assertTrue(intermediateStatement.isBatchedQuery());

    assertEquals(5, intermediateStatement.getStatements().size(), 5);
    assertEquals("BEGIN TRANSACTION", intermediateStatement.getStatement(0));
    assertEquals("INSERT INTO users (id) VALUES (1)", intermediateStatement.getStatement(1));
    assertEquals("INSERT INTO users (id) VALUES (2)", intermediateStatement.getStatement(2));
    assertEquals("INSERT INTO users (id) VALUES (3)", intermediateStatement.getStatement(3));
    assertEquals("COMMIT", intermediateStatement.getStatement(4));
  }

  @Test
  public void testBatchStatementsWithEmptyStatements() {
    String sql = "INSERT INTO users (id) VALUES (1); ;;; INSERT INTO users (id) VALUES (2);";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connectionHandler);

    assertTrue(intermediateStatement.isBatchedQuery());
    assertEquals(2, intermediateStatement.getStatements().size());
    assertEquals("INSERT INTO users (id) VALUES (1)", intermediateStatement.getStatement(0));
    assertEquals("INSERT INTO users (id) VALUES (2)", intermediateStatement.getStatement(1));
  }

  @Test
  public void testBatchStatementsWithQuotes() {
    String sql =
        "INSERT INTO users (name) VALUES (';;test;;'); INSERT INTO users (name1, name2) VALUES ('''''', ';'';');";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, parse(sql), connectionHandler);

    assertTrue(intermediateStatement.isBatchedQuery());
    assertEquals(2, intermediateStatement.getStatements().size());
    assertEquals(
        "INSERT INTO users (name) VALUES (';;test;;')", intermediateStatement.getStatement(0));
    assertEquals(
        "INSERT INTO users (name1, name2) VALUES ('''''', ';'';')",
        intermediateStatement.getStatement(1));
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
    assertEquals(2, intermediateStatement.getStatements().size());
    assertEquals(
        "INSERT INTO users (name) VALUES (';;test;;')", intermediateStatement.getStatement(0));
    assertEquals(
        "INSERT INTO users (name1, name2) VALUES ('''''', ';'';')",
        intermediateStatement.getStatement(1));
  }

  @Test
  public void testCopyInvalidBuildMutation() throws Exception {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    setupQueryInformationSchemaResults();

    CopyStatement statement =
        new CopyStatement(
            connectionHandler, mock(OptionsMetadata.class), parse("COPY keyvalue FROM STDIN;"));
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
