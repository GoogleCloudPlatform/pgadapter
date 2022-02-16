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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.jdbc.JdbcConstants;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.SQLMetadata;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.ResultType;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  @Mock private Statement statement;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ProxyServer server;
  @Mock private OptionsMetadata options;
  @Mock private ResultSet resultSet;
  @Mock private DataOutputStream outputStream;

  private byte[] longToBytes(int value) {
    byte[] parameters = new byte[8];
    ByteConverter.int8(parameters, 0, value);
    return parameters;
  }

  @Test
  public void testBasicSelectStatement() throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.getUpdateCount()).thenReturn(JdbcConstants.STATEMENT_RESULT_SET);
    when(statement.getResultSet()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("SELECT * FROM users", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "SELECT");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1)).execute("SELECT * FROM users");
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
    when(connection.createStatement()).thenReturn(statement);
    when(statement.getUpdateCount()).thenReturn(1);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("UPDATE users SET name = someName WHERE id = 10", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "UPDATE");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1))
        .execute("UPDATE users SET name = someName WHERE id = 10");
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals((int) intermediateStatement.getUpdateCount(), 1);
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.UPDATE_COUNT);
    Assert.assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testBasicZeroUpdateCountResultStatement() throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.getUpdateCount()).thenReturn(0);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("UPDATE users SET name = someName WHERE id = -1", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "UPDATE");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1))
        .execute("UPDATE users SET name = someName WHERE id = -1");
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals((int) intermediateStatement.getUpdateCount(), 0);
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.UPDATE_COUNT);
    Assert.assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testBasicNoResultStatement() throws Exception {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.getUpdateCount()).thenReturn(0);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("CREATE TABLE users (name varchar(100) primary key)", connection);

    assertFalse(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getCommand(), "CREATE");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1))
        .execute("CREATE TABLE users (name varchar(100) primary key)");
    assertFalse(intermediateStatement.containsResultSet());
    assertEquals((int) intermediateStatement.getUpdateCount(), 0);
    assertTrue(intermediateStatement.isExecuted());
    assertEquals(intermediateStatement.getResultType(), ResultType.NO_RESULT);
    Assert.assertNull(intermediateStatement.getStatementResult());
    assertFalse(intermediateStatement.isHasMoreData());
    assertFalse(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test(expected = IllegalStateException.class)
  public void testDescribeBasicStatementThrowsException() throws Exception {
    when(connection.createStatement()).thenReturn(statement);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("SELECT * FROM users", connection);

    intermediateStatement.describe();
  }

  @Test
  public void testBasicStatementExceptionGetsSetOnExceptedExecution() throws Exception {
    SQLException thrownException = new SQLException();

    when(connection.createStatement()).thenReturn(statement);
    when(statement.execute(ArgumentMatchers.anyString())).thenThrow(thrownException);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("SELECT * FROM users", connection);

    intermediateStatement.execute();

    assertTrue(intermediateStatement.hasException());
    assertEquals(intermediateStatement.getException(), thrownException);
  }

  @Test
  public void testPreparedStatement() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE age > $2 AND age < $3 AND name = $1";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.VARCHAR, Oid.INT8, Oid.INT4);

    when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(preparedStatement);
    when(preparedStatement.getResultSet()).thenReturn(mock(ResultSet.class));

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(sqlStatement, connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    String expectedSQL = "SELECT * FROM users WHERE age > ? AND age < ? AND name = ?";

    assertEquals(intermediateStatement.getSql(), expectedSQL);
    assertEquals(intermediateStatement.getParameterCount(), 3);

    intermediateStatement.execute();

    Mockito.verify(preparedStatement, Mockito.times(1)).execute();
    Mockito.verify(connection, Mockito.times(1)).prepareStatement(expectedSQL);

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.bind(
            parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());

    Mockito.verify(preparedStatement, Mockito.times(1)).setObject(1, 20L, Types.BIGINT);
    Mockito.verify(preparedStatement, Mockito.times(1)).setObject(2, 30, Types.INTEGER);
    Mockito.verify(preparedStatement, Mockito.times(1)).setObject(3, "userName", Types.VARCHAR);

    assertEquals(intermediatePortalStatement.getSql(), expectedSQL);
    assertEquals(intermediatePortalStatement.getCommand(), "SELECT");
    assertEquals(intermediatePortalStatement.getStatement(), preparedStatement);
    assertFalse(intermediatePortalStatement.isExecuted());
    assertTrue(intermediateStatement.isBound());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPreparedStatementIllegalTypeThrowsException() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.JSON);

    when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(preparedStatement);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(sqlStatement, connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    byte[][] parameters = {"{}".getBytes()};

    intermediateStatement.bind(parameters, new ArrayList<>(), new ArrayList<>());
  }

  @Test(expected = IllegalStateException.class)
  public void testPreparedStatementDescribeThrowsException() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3";

    when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(preparedStatement);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(sqlStatement, connection);

    intermediateStatement.describe();
  }

  @Test
  public void testPortalStatement() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.INT8, Oid.INT4);
    byte[][] parameters = {"20".getBytes(), "30".getBytes(), "userName".getBytes()};
    SQLMetadata metadata = Converter.toJDBCParams(sqlStatement);

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            preparedStatement,
            metadata.getSqlString(),
            metadata.getParameterCount(),
            metadata.getParameterIndexToPositions(),
            connection);

    intermediateStatement.describe();

    Mockito.verify(preparedStatement, Mockito.times(1)).getMetaData();

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

  @Test(expected = IllegalStateException.class)
  public void testPortalStatementDescribePropagatesFailure() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";
    SQLMetadata metadata = Converter.toJDBCParams(sqlStatement);

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            preparedStatement,
            metadata.getSqlString(),
            metadata.getParameterCount(),
            metadata.getParameterIndexToPositions(),
            connection);

    when(preparedStatement.getMetaData()).thenThrow(new SQLException());

    intermediateStatement.describe();
  }

  @Test
  public void testBatchStatements() throws Exception {
    String sql =
        "INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2);INSERT INTO users (id) VALUES (3);";
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

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
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

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
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

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
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

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

    when(connection.createStatement()).thenReturn(statement);
    when(connectionHandler.getJdbcConnection()).thenReturn(connection);
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
    assertEquals(result.get(1), "INSERT INTO users (name1, name2) VALUES ('''''', ';'';');");
  }

  @Test
  public void testCopyBuildMutation() throws Exception {
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(connection.prepareStatement(ArgumentMatchers.anyString()))
        .thenReturn(preparedStatement);
    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);
    Mockito.when(statement.getUpdateCount()).thenReturn(1);

    ResultSet spannerType = Mockito.mock(ResultSet.class);
    Mockito.when(spannerType.getString("column_name")).thenReturn("key", "value");
    Mockito.when(spannerType.getString("spanner_type")).thenReturn("INT64", "STRING");
    Mockito.when(spannerType.next()).thenReturn(true, true, false);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(spannerType);

    CopyStatement statement = new CopyStatement("COPY keyvalue FROM STDIN;", connection);
    statement.execute();

    byte[] payload = "2\t3\n".getBytes();
    MutationWriter mw = statement.getMutationWriter();
    mw.buildMutation(connectionHandler, payload);

    Assert.assertEquals(statement.getFormatType(), "TEXT");
    Assert.assertEquals(statement.getDelimiterChar(), '\t');
    Assert.assertEquals(
        statement.getMutationWriter().getMutations().toString(),
        "[insert(keyvalue{key=2,value=3})]");

    statement.close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testCopyInvalidBuildMutation() throws Exception {
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(connection.prepareStatement(ArgumentMatchers.anyString()))
        .thenReturn(preparedStatement);
    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);
    Mockito.when(statement.getUpdateCount()).thenReturn(1);

    ResultSet spannerType = Mockito.mock(ResultSet.class);
    Mockito.when(spannerType.getString("column_name")).thenReturn("key", "value");
    Mockito.when(spannerType.getString("spanner_type")).thenReturn("INT64", "STRING");
    Mockito.when(spannerType.next()).thenReturn(true, true, false);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(spannerType);

    CopyStatement statement = new CopyStatement("COPY keyvalue FROM STDIN;", connection);
    statement.execute();

    byte[] payload = "2 3\n".getBytes();
    MutationWriter mw = statement.getMutationWriter();

    Exception thrown =
        Assert.assertThrows(
            SQLException.class,
            () -> {
              mw.buildMutation(connectionHandler, payload);
            });
    Assert.assertEquals(
        thrown.getMessage(),
        "Invalid COPY data: Row length mismatched. Expected 2 columns, but only found 1");

    statement.close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }
}
