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

import com.google.cloud.spanner.jdbc.JdbcConstants;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.SQLMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.ResultType;
import com.google.cloud.spanner.pgadapter.utils.Converter;
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
  @Mock private ProxyServer server;
  @Mock private OptionsMetadata options;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private DataOutputStream outputStream;

  private byte[] longToBytes(int value) {
    byte[] parameters = new byte[8];
    ByteConverter.int8(parameters, 0, value);
    return parameters;
  }

  @Test
  public void testBasicSelectStatement() throws Exception {
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.getUpdateCount()).thenReturn(JdbcConstants.STATEMENT_RESULT_SET);
    Mockito.when(statement.getResultSet()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("SELECT * FROM users", connection);

    Assert.assertFalse(intermediateStatement.isExecuted());
    Assert.assertEquals(intermediateStatement.getCommand(), "SELECT");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1)).execute("SELECT * FROM users");
    Assert.assertTrue(intermediateStatement.containsResultSet());
    Assert.assertTrue(intermediateStatement.isExecuted());
    Assert.assertEquals(intermediateStatement.getResultType(), ResultType.RESULT_SET);
    Assert.assertEquals(intermediateStatement.getStatementResult(), resultSet);
    Assert.assertTrue(intermediateStatement.isHasMoreData());
    Assert.assertFalse(intermediateStatement.hasException());
    Assert.assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testBasicUpdateStatement() throws Exception {
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.getUpdateCount()).thenReturn(1);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("UPDATE users SET name = someName WHERE id = 10", connection);

    Assert.assertFalse(intermediateStatement.isExecuted());
    Assert.assertEquals(intermediateStatement.getCommand(), "UPDATE");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1))
        .execute("UPDATE users SET name = someName WHERE id = 10");
    Assert.assertFalse(intermediateStatement.containsResultSet());
    Assert.assertEquals((int) intermediateStatement.getUpdateCount(), 1);
    Assert.assertTrue(intermediateStatement.isExecuted());
    Assert.assertEquals(intermediateStatement.getResultType(), ResultType.UPDATE_COUNT);
    Assert.assertNull(intermediateStatement.getStatementResult());
    Assert.assertFalse(intermediateStatement.isHasMoreData());
    Assert.assertFalse(intermediateStatement.hasException());
    Assert.assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testBasicZeroUpdateCountStatement() throws Exception {
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.getUpdateCount()).thenReturn(0);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("UPDATE users SET name = someName WHERE id = -1", connection);

    Assert.assertFalse(intermediateStatement.isExecuted());
    Assert.assertEquals(intermediateStatement.getCommand(), "UPDATE");

    intermediateStatement.execute();

    Mockito.verify(statement, Mockito.times(1))
        .execute("UPDATE users SET name = someName WHERE id = -1");
    Assert.assertFalse(intermediateStatement.containsResultSet());
    Assert.assertEquals((int) intermediateStatement.getUpdateCount(), 0);
    Assert.assertTrue(intermediateStatement.isExecuted());
    Assert.assertEquals(intermediateStatement.getResultType(), ResultType.UPDATE_COUNT);
    Assert.assertNull(intermediateStatement.getStatementResult());
    Assert.assertFalse(intermediateStatement.isHasMoreData());
    Assert.assertFalse(intermediateStatement.hasException());
    Assert.assertEquals(intermediateStatement.getResultFormatCode(0), 0);

    intermediateStatement.close();

    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test(expected = IllegalStateException.class)
  public void testDescribeBasicStatementThrowsException() throws Exception {
    Mockito.when(connection.createStatement()).thenReturn(statement);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("SELECT * FROM users", connection);

    intermediateStatement.describe();
  }

  @Test
  public void testBasicStatementExceptionGetsSetOnExceptedExecution() throws Exception {
    SQLException thrownException = new SQLException();

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.execute(ArgumentMatchers.anyString())).thenThrow(thrownException);

    IntermediateStatement intermediateStatement =
        new IntermediateStatement("SELECT * FROM users", connection);

    intermediateStatement.execute();

    Assert.assertTrue(intermediateStatement.hasException());
    Assert.assertEquals(intermediateStatement.getException(), thrownException);
  }

  @Test
  public void testPreparedStatement() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE age > $2 AND age < $3 AND name = $1";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.VARCHAR, Oid.INT8, Oid.INT4);

    Mockito.when(connection.prepareStatement(ArgumentMatchers.anyString()))
        .thenReturn(preparedStatement);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(sqlStatement, connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    String expectedSQL = "SELECT * FROM users WHERE age > ? AND age < ? AND name = ?";

    Assert.assertEquals(intermediateStatement.getSql(), expectedSQL);
    Assert.assertEquals(intermediateStatement.getParameterCount(), 3);

    intermediateStatement.execute();

    Mockito.verify(preparedStatement, Mockito.times(1)).execute();
    Mockito.verify(connection, Mockito.times(1)).prepareStatement(expectedSQL);

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.bind(
            parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());

    Mockito.verify(preparedStatement, Mockito.times(1)).setObject(1, 20L);
    Mockito.verify(preparedStatement, Mockito.times(1)).setObject(2, 30);
    Mockito.verify(preparedStatement, Mockito.times(1)).setObject(3, "userName");

    Assert.assertEquals(intermediatePortalStatement.getSql(), expectedSQL);
    Assert.assertEquals(intermediatePortalStatement.getCommand(), "SELECT");
    Assert.assertEquals(intermediatePortalStatement.getStatement(), preparedStatement);
    Assert.assertFalse(intermediatePortalStatement.isExecuted());
    Assert.assertTrue(intermediateStatement.isBound());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPreparedStatementIllegalTypeThrowsException() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    List<Integer> parameterDataTypes = Arrays.asList(Oid.JSON);

    Mockito.when(connection.prepareStatement(ArgumentMatchers.anyString()))
        .thenReturn(preparedStatement);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(sqlStatement, connection);
    intermediateStatement.setParameterDataTypes(parameterDataTypes);

    byte[][] parameters = {"{}".getBytes()};

    intermediateStatement.bind(parameters, new ArrayList<>(), new ArrayList<>());
  }

  @Test(expected = IllegalStateException.class)
  public void testPreparedStatementDescribeThrowsException() throws Exception {
    String sqlStatement = "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3";

    Mockito.when(connection.prepareStatement(ArgumentMatchers.anyString()))
        .thenReturn(preparedStatement);

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

    Assert.assertEquals(intermediateStatement.getParameterFormatCode(0), 0);
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(1), 0);
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(2), 0);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(0), 0);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(1), 0);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(2), 0);

    intermediateStatement.setParameterFormatCodes(Arrays.asList((short) 1));
    intermediateStatement.setResultFormatCodes(Arrays.asList((short) 1));
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(0), 1);
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(1), 1);
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(2), 1);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(0), 1);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(1), 1);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(2), 1);

    intermediateStatement.setParameterFormatCodes(Arrays.asList((short) 0, (short) 1, (short) 0));
    intermediateStatement.setResultFormatCodes(Arrays.asList((short) 0, (short) 1, (short) 0));
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(0), 0);
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(1), 1);
    Assert.assertEquals(intermediateStatement.getParameterFormatCode(2), 0);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(0), 0);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(1), 1);
    Assert.assertEquals(intermediateStatement.getResultFormatCode(2), 0);
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

    Mockito.when(preparedStatement.getMetaData()).thenThrow(new SQLException());

    intermediateStatement.describe();
  }

  @Test
  public void testBatchStatements() throws Exception {
    String sql =
        "INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2);INSERT INTO users (id) VALUES (3);";
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

    Assert.assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), "INSERT INTO users (id) VALUES (1);");
    Assert.assertEquals(result.get(1), "INSERT INTO users (id) VALUES (2);");
    Assert.assertEquals(result.get(2), "INSERT INTO users (id) VALUES (3);");
  }

  @Test
  public void testAdditionalBatchStatements() throws Exception {
    String sql =
        "BEGIN TRANSACTION; INSERT INTO users (id) VALUES (1); INSERT INTO users (id) VALUES (2); INSERT INTO users (id) VALUES (3); COMMIT;";
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

    Assert.assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    Assert.assertEquals(result.size(), 5);
    Assert.assertEquals(result.get(0), "BEGIN TRANSACTION;");
    Assert.assertEquals(result.get(1), "INSERT INTO users (id) VALUES (1);");
    Assert.assertEquals(result.get(2), "INSERT INTO users (id) VALUES (2);");
    Assert.assertEquals(result.get(3), "INSERT INTO users (id) VALUES (3);");
    Assert.assertEquals(result.get(4), "COMMIT;");
  }

  @Test
  public void testBatchStatementsWithEmptyStatements() throws Exception {
    String sql = "INSERT INTO users (id) VALUES (1); ;;; INSERT INTO users (id) VALUES (2);";
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

    Assert.assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "INSERT INTO users (id) VALUES (1);");
    Assert.assertEquals(result.get(1), "INSERT INTO users (id) VALUES (2);");
  }

  @Test
  public void testBatchStatementsWithQuotes() throws Exception {
    String sql =
        "INSERT INTO users (name) VALUES (';;test;;'); INSERT INTO users (name1, name2) VALUES ('''''', ';'';');";
    IntermediateStatement intermediateStatement = new IntermediateStatement(sql, connection);

    Assert.assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "INSERT INTO users (name) VALUES (';;test;;');");
    Assert.assertEquals(result.get(1), "INSERT INTO users (name1, name2) VALUES ('''''', ';'';');");
  }

  @Test
  public void testBatchStatementsWithComments() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) 132};
    String payload =
        "INSERT INTO users (name) VALUES (';;test;;'); /* Comment;; Comment; */INSERT INTO users (name1, name2) VALUES ('''''', ';'';');\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.requiresMatcher()).thenReturn(false);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), QueryMessage.class);
    IntermediateStatement intermediateStatement = ((QueryMessage) message).getStatement();

    Assert.assertTrue(intermediateStatement.isBatchedQuery());
    List<String> result = intermediateStatement.getStatements();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0), "INSERT INTO users (name) VALUES (';;test;;');");
    Assert.assertEquals(result.get(1), "INSERT INTO users (name1, name2) VALUES ('''''', ';'';');");
  }
}
