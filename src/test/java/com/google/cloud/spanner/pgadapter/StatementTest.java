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
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
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
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
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
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class StatementTest {

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private Connection connection;
  @Mock private ConnectionHandler connectionHandler;
  @Mock private ConnectionMetadata connectionMetadata;
  @Mock private ProxyServer server;
  @Mock private OptionsMetadata options;
  @Mock private StatementResult statementResult;
  @Mock private ResultSet resultSet;
  @Mock private DataOutputStream outputStream;

  private byte[] longToBytes(int value) {
    byte[] parameters = new byte[8];
    ByteConverter.int8(parameters, 0, value);
    return parameters;
  }

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
        new IntermediateStatement(options, "SELECT * FROM users", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "SELECT");

    intermediateStatement.execute();

    Mockito.verify(connection, Mockito.times(1)).execute(Statement.of("SELECT * FROM users"));
    assertTrue(intermediateStatement.containsResultSet());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.RESULT_SET);
    assertEquals(intermediateStatement.getStatementResult(), resultSet);
    assertTrue(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testBasicUpdateStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(1L);
    when(connection.execute(Statement.of("UPDATE users SET name = someName WHERE id = 10")))
        .thenReturn(statementResult);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, "UPDATE users SET name = someName WHERE id = 10", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "UPDATE");

    intermediateStatement.execute();

    Mockito.verify(connection, Mockito.times(1))
        .execute(Statement.of("UPDATE users SET name = someName WHERE id = 10"));
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals(intermediateStatement.getUpdateCount().longValue(), 1L);
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.UPDATE_COUNT);
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testBasicZeroUpdateCountResultStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(0L);
    when(connection.execute(Statement.of("UPDATE users SET name = someName WHERE id = -1")))
        .thenReturn(statementResult);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, "UPDATE users SET name = someName WHERE id = -1", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "UPDATE");

    intermediateStatement.execute();

    Mockito.verify(connection, Mockito.times(1))
        .execute(Statement.of("UPDATE users SET name = someName WHERE id = -1"));
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals(intermediateStatement.getUpdateCount().longValue(), 0L);
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.UPDATE_COUNT);
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testBasicNoResultStatement() throws Exception {
    when(statementResult.getResultType()).thenReturn(StatementResult.ResultType.NO_RESULT);
    when(statementResult.getUpdateCount()).thenThrow(new IllegalStateException());
    when(connection.execute(Statement.of("CREATE TABLE users (name varchar(100) primary key)")))
        .thenReturn(statementResult);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(
            options, "CREATE TABLE users (name varchar(100) primary key)", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "CREATE");

    intermediateStatement.execute();

    Mockito.verify(connection, Mockito.times(1))
        .execute(Statement.of("CREATE TABLE users (name varchar(100) primary key)"));
    assertFalse(intermediateStatement.containsResultSet());
    assertNull(intermediateStatement.getUpdateCount());
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.NO_RESULT);
    assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test(expected = IllegalStateException.class)
  public void testDescribeBasicStatementThrowsException() throws Exception {
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, "SELECT * FROM users", connection);

    intermediateStatement.describe();
  }

  @Test
  public void testBasicStatementExceptionGetsSetOnExceptedExecution() throws Exception {
    SpannerException thrownException =
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error");

    when(connection.execute(Statement.of("SELECT * FROM users"))).thenThrow(thrownException);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, "SELECT * FROM users", connection);

    intermediateStatement.execute();

    assertTrue(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getException(), thrownException);
  }

  @Test
  public void testPreparedStatement() throws Exception {
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
        new IntermediatePreparedStatement(options, sqlStatement, connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    assertEquals(intermediateStatement.getSql(), sqlStatement);

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.bind(
            parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());
    intermediateStatement.execute();
    Mockito.verify(connection, Mockito.times(1)).execute(statement);

    assertEquals(intermediatePortalStatement.getSql(), sqlStatement);
    assertEquals(intermediatePortalStatement.getCommand(), "SELECT");
    assertFalse(intermediatePortalStatement.isExecuted());
    assertTrue(intermediateStatement.isBound());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPreparedStatementIllegalTypeThrowsException() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.JSON);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(options, sqlStatement, connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    byte[][] parameters = {"{}".getBytes()};

    intermediateStatement.bind(parameters, new ArrayList<>(), new ArrayList<>());
  }

  @Test
  public void testPreparedStatementDescribeDoesNotThrowException() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3";
    when(connection.analyzeQuery(Statement.of(sqlStatement), QueryAnalyzeMode.PLAN))
        .thenReturn(resultSet);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(options, sqlStatement, connection);

    intermediateStatement.describe();
  }

  @Test
  public void testPortalStatement() {
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";
    when(connection.executeQuery(Statement.of(sqlStatement))).thenReturn(resultSet);

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(options, sqlStatement, connection);

    intermediateStatement.describe();

    Mockito.verify(connection, Mockito.times(1)).executeQuery(Statement.of(sqlStatement));

    assertEquals(intermediateStatement.getParameterFormatCode(0), 0);
    assertEquals(intermediateStatement.getParameterFormatCode(1), 0);
    assertEquals(intermediateStatement.getParameterFormatCode(2), 0);
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);
    assertEquals(intermediateStatement.getResultFormatCode(1), 0);
    assertEquals(intermediateStatement.getResultFormatCode(2), 0);

    intermediateStatement.setParameterFormatCodes(Arrays.asList((short) 1));
    intermediateStatement.setResultFormatCodes(Arrays.asList((short) 1));
    assertEquals(intermediateStatement.getParameterFormatCode(0), 1);
    assertEquals(intermediateStatement.getParameterFormatCode(1), 1);
    assertEquals(intermediateStatement.getParameterFormatCode(2), 1);
    assertEquals(intermediateStatement.getResultFormatCode(0), 1);
    assertEquals(intermediateStatement.getResultFormatCode(1), 1);
    assertEquals(intermediateStatement.getResultFormatCode(2), 1);

    intermediateStatement.setParameterFormatCodes(Arrays.asList((short) 0, (short) 1, (short) 0));
    intermediateStatement.setResultFormatCodes(Arrays.asList((short) 0, (short) 1, (short) 0));
    assertEquals(intermediateStatement.getParameterFormatCode(0), 0);
    assertEquals(intermediateStatement.getParameterFormatCode(1), 1);
    assertEquals(intermediateStatement.getParameterFormatCode(2), 0);
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);
    assertEquals(intermediateStatement.getResultFormatCode(1), 1);
    assertEquals(intermediateStatement.getResultFormatCode(2), 0);
  }

  @Test
  public void testPortalStatementDescribePropagatesFailure() {
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(options, sqlStatement, connection);

    when(connection.executeQuery(Statement.of(sqlStatement)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error"));

    SpannerException exception =
        assertThrows(SpannerException.class, intermediateStatement::describe);
    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
  }

  @Test
  public void testBatchStatements() throws Exception {
    String sql =
        "INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2);INSERT INTO users (id) VALUES (3);";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, sql, connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(result.size(), 3);
    assertEquals(result.get(0), "INSERT INTO users (id) VALUES (1);");
    assertEquals(result.get(1), "INSERT INTO users (id) VALUES (2);");
    assertEquals(result.get(2), "INSERT INTO users (id) VALUES (3);");
  }

  @Test
  public void testAdditionalBatchStatements() throws Exception {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2); INSERT INTO users (id) VALUES (3); COMMIT;";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, sql, connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(result.size(), 5);
    assertEquals(result.get(0), "BEGIN TRANSACTION;");
    assertEquals(result.get(1), "INSERT INTO users (id) VALUES (1);");
    assertEquals(result.get(2), "INSERT INTO users (id) VALUES (2);");
    assertEquals(result.get(3), "INSERT INTO users (id) VALUES (3);");
    assertEquals(result.get(4), "COMMIT;");
  }

  @Test
  public void testBatchStatementsWithEmptyStatements() throws Exception {
    String sql = "INSERT INTO users (id) VALUES (1); ;;; INSERT INTO users (id) VALUES (2);";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, sql, connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), "INSERT INTO users (id) VALUES (1);");
    assertEquals(result.get(1), "INSERT INTO users (id) VALUES (2);");
  }

  @Test
  public void testBatchStatementsWithQuotes() throws Exception {
    String sql =
        "INSERT INTO users (name) VALUES (';;test;;'); INSERT INTO users (name1, name2) VALUES ('''''', ';'';');";
    IntermediateStatement intermediateStatement =
        new IntermediateStatement(options, sql, connection);

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), "INSERT INTO users (name) VALUES (';;test;;');");
    assertEquals(result.get(1), "INSERT INTO users (name1, name2) VALUES ('''''', ';'';');");
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
    assertEquals(message.getClass(), QueryMessage.class);
    IntermediateStatement intermediateStatement = ((QueryMessage) message).getStatement();

    assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), "INSERT INTO users (name) VALUES (';;test;;');");
    assertEquals(result.get(1), "INSERT INTO users (name1, name2) VALUES ('''''', ';'';')");
  }

  @Test
  public void testCopyInvalidBuildMutation() throws Exception {
    setupQueryInformationSchemaResults();
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(statementResult.getResultType())
        .thenReturn(StatementResult.ResultType.UPDATE_COUNT);
    Mockito.when(statementResult.getUpdateCount()).thenReturn(1L);

    CopyStatement statement =
        new CopyStatement(mock(OptionsMetadata.class), "COPY keyvalue FROM STDIN;", connection);
    statement.execute();

    byte[] payload = "2 3\n".getBytes();
    MutationWriter mutationWriter = statement.getMutationWriter();
    mutationWriter.addCopyData(payload);

    SpannerException thrown =
        Assert.assertThrows(SpannerException.class, statement::getUpdateCount);
    Assert.assertEquals(ErrorCode.INVALID_ARGUMENT, thrown.getErrorCode());
    Assert.assertEquals(
        "INVALID_ARGUMENT: Invalid COPY data: Row length mismatched. Expected 2 columns, but only found 1",
        thrown.getMessage());

    statement.close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  private void setupQueryInformationSchemaResults() {
    ResultSet spannerType = Mockito.mock(ResultSet.class);
    Mockito.when(spannerType.getString("column_name")).thenReturn("key", "value");
    Mockito.when(spannerType.getString("data_type")).thenReturn("bigint", "character varying");
    Mockito.when(spannerType.next()).thenReturn(true, true, false);
    Mockito.when(
            connection.executeQuery(
                ArgumentMatchers.argThat(
                    statement ->
                        statement != null && statement.getSql().startsWith("SELECT column_name"))))
        .thenReturn(spannerType);

    ResultSet countResult = Mockito.mock(ResultSet.class);
    when(countResult.getLong(0)).thenReturn(2L);
    when(countResult.next()).thenReturn(true, false);
    Mockito.when(
            connection.executeQuery(
                ArgumentMatchers.argThat(
                    statement ->
                        statement != null && statement.getSql().startsWith("SELECT COUNT(*)"))))
        .thenReturn(countResult);
  }
}
