// Copyright 2026 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.util.concurrent.SettableFuture;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class IntermediatePreparedStatementTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private ConnectionHandler connectionHandler;
  @Mock private ConnectionMetadata connectionMetadata;
  @Mock private Connection connection;
  @Mock private BackendConnection backendConnection;

  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  @Before
  public void setUp() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connectionMetadata.getOutputStream())
        .thenReturn(new DataOutputStream(new ByteArrayOutputStream()));
  }

  private IntermediatePreparedStatement createStatement(String sql, int[] parameterTypes) {
    Statement statement = Statement.of(sql);
    ParsedStatement parsedStatement = PARSER.parse(statement);
    return new IntermediatePreparedStatement(
        connectionHandler,
        mock(OptionsMetadata.class),
        "test_statement",
        parameterTypes,
        parsedStatement,
        statement);
  }

  @Test
  public void testIsDescribed_initiallyFalse() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    assertFalse(statement.isDescribed());
  }

  @Test
  public void testDescribe_unannouncedDefaultsToGivenParameterDataTypes() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    DescribeResult describeResult = statement.describe();
    assertNotNull(describeResult);
    assertEquals(1, describeResult.getParameters().length);
    assertEquals(Oid.INT8, describeResult.getParameters()[0]);
  }

  @Test
  public void testIsDescribed_pendingFutureReturnsTrue() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> analyzeFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any())).thenReturn(analyzeFuture);

    statement.describeAsync(backendConnection);
    assertTrue(statement.isDescribed());
  }

  @Test
  public void testIsDescribed_completedFutureReturnsTrue() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> analyzeFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any())).thenReturn(analyzeFuture);

    statement.describeAsync(backendConnection);

    StatementResult statementResult = mock(StatementResult.class);
    when(statementResult.getResultType()).thenReturn(ResultType.NO_RESULT);
    analyzeFuture.set(statementResult);

    assertTrue(statement.isDescribed());
    DescribeResult describeResult = statement.describe();
    assertNotNull(describeResult);
  }

  @Test
  public void testIsDescribed_cancelledFutureResetsStateAndReturnsFalse() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> analyzeFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any())).thenReturn(analyzeFuture);

    statement.describeAsync(backendConnection);
    analyzeFuture.cancel(true);

    assertFalse(statement.isDescribed());
  }

  @Test
  public void testDescribe_cancelledFutureResetsStateAndThrowsQueryCanceled() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> analyzeFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any())).thenReturn(analyzeFuture);

    statement.describeAsync(backendConnection);
    analyzeFuture.cancel(true);

    PGException pgException = assertThrows(PGException.class, statement::describe);
    assertEquals(SQLState.QueryCanceled, pgException.getSQLState());
    assertFalse(statement.isDescribed());
  }

  @Test
  public void testIsDescribed_failedFutureResetsStateAndReturnsFalse() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> analyzeFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any())).thenReturn(analyzeFuture);

    statement.describeAsync(backendConnection);
    analyzeFuture.setException(
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "Table not found"));

    assertFalse(statement.isDescribed());
  }

  @Test
  public void testDescribe_failedFutureResetsStateAndThrowsException() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> analyzeFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any())).thenReturn(analyzeFuture);

    statement.describeAsync(backendConnection);
    SpannerException spannerException =
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "Table not found");
    analyzeFuture.setException(spannerException);

    PGException pgException = assertThrows(PGException.class, statement::describe);
    assertEquals(SQLState.RaiseException, pgException.getSQLState());
    assertTrue(pgException.getMessage().contains("Table not found"));
    assertFalse(statement.isDescribed());
  }

  @Test
  public void testRetryDescribeAfterCancellation() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.UNSPECIFIED});
    SettableFuture<StatementResult> initialFuture = SettableFuture.create();
    SettableFuture<StatementResult> retryFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any()))
        .thenReturn(initialFuture)
        .thenReturn(retryFuture);

    // Initial describe is cancelled.
    statement.describeAsync(backendConnection);
    initialFuture.cancel(true);
    assertFalse(statement.isDescribed());

    // Retrying describe succeeds.
    statement.describeAsync(backendConnection);
    assertTrue(statement.isDescribed());

    ResultSet resultSet = mock(ResultSet.class);
    ResultSetMetadata metadata =
        ResultSetMetadata.newBuilder()
            .setUndeclaredParameters(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("p1")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build())
                    .build())
            .build();
    when(resultSet.getMetadata()).thenReturn(metadata);
    StatementResult statementResult = mock(StatementResult.class);
    when(statementResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    when(statementResult.getResultSet()).thenReturn(resultSet);
    retryFuture.set(statementResult);

    assertTrue(statement.isDescribed());
    DescribeResult describeResult = statement.describe();
    assertNotNull(describeResult);
    assertEquals(1, describeResult.getParameters().length);
    assertEquals(Oid.INT8, describeResult.getParameters()[0]);
  }

  @Test
  public void testRetryDescribeAfterFailure() {
    IntermediatePreparedStatement statement =
        createStatement("select * from foo where id = $1", new int[] {Oid.INT8});
    SettableFuture<StatementResult> initialFuture = SettableFuture.create();
    SettableFuture<StatementResult> retryFuture = SettableFuture.create();
    when(backendConnection.analyze(any(), any(), any()))
        .thenReturn(initialFuture)
        .thenReturn(retryFuture);

    // Initial describe fails.
    statement.describeAsync(backendConnection);
    initialFuture.setException(new RuntimeException("Transient failure"));
    assertFalse(statement.isDescribed());

    // Retrying describe succeeds.
    statement.describeAsync(backendConnection);
    assertTrue(statement.isDescribed());

    StatementResult statementResult = mock(StatementResult.class);
    when(statementResult.getResultType()).thenReturn(ResultType.NO_RESULT);
    retryFuture.set(statementResult);

    assertTrue(statement.isDescribed());
    DescribeResult describeResult = statement.describe();
    assertNotNull(describeResult);
  }
}
