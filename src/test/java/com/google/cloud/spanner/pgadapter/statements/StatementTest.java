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

import static com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement.NO_PARAMS;
import static com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement.NO_PARAMETER_TYPES;
import static com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.EMPTY_LOCAL_STATEMENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext;
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
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
  @Mock private BackendConnection backendConnection;
  @Mock private ProxyServer server;
  @Mock private OptionsMetadata options;
  @Mock private StatementResult statementResult;
  @Mock private ResultSet resultSet;
  @Mock private DataOutputStream outputStream;

  @AfterClass
  public static void cleanup() {
    deleteLogFile();
  }

  private static void deleteLogFile() {
    // TODO: Make error log file configurable and turn off writing to a file during tests.
    try {
      Files.deleteIfExists(new File("output.txt").toPath());
    } catch (IOException ignore) {
    }
  }

  @Test
  public void testBasicSelectStatement() throws Exception {
    String sql = "SELECT * FROM users";
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler, options, "", NO_PARAMETER_TYPES, parse(sql), Statement.of(sql)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("SELECT", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);

    verify(backendConnection).execute(eq(parse(sql)), eq(Statement.of(sql)), any());
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler, options, "", NO_PARAMETER_TYPES, parse(sql), Statement.of(sql)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("UPDATE", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);

    verify(backendConnection).execute(eq(parse(sql)), eq(Statement.of(sql)), any());
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connection.execute(Statement.of(sql))).thenReturn(statementResult);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler, options, "", NO_PARAMETER_TYPES, parse(sql), Statement.of(sql)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());
    BackendConnection backendConnection =
        new BackendConnection(
            connectionHandler.getDatabaseId(),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler, options, "", NO_PARAMETER_TYPES, parse(sql), Statement.of(sql)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());

    assertFalse(intermediateStatement.isExecuted());
    assertEquals("CREATE", intermediateStatement.getCommand());

    intermediateStatement.executeAsync(backendConnection);

    verify(backendConnection).execute(eq(parse(sql)), eq(Statement.of(sql)), any());
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler, options, "", NO_PARAMETER_TYPES, parse(sql), Statement.of(sql)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());
    BackendConnection backendConnection =
        new BackendConnection(
            connectionHandler.getDatabaseId(),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

    intermediateStatement.executeAsync(backendConnection);
    backendConnection.flush();

    assertTrue(intermediateStatement.hasException());
    assertEquals(
        PGExceptionFactory.newPGException("test error"), intermediateStatement.getException());
  }

  @Test
  public void testPreparedStatement() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    SessionState sessionState = mock(SessionState.class);
    when(backendConnection.getSessionState()).thenReturn(sessionState);
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
        new BackendConnection(
            connectionHandler.getDatabaseId(),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            "",
            parameterDataTypes,
            parse(sqlStatement),
            Statement.of(sqlStatement));

    assertEquals(sqlStatement, intermediateStatement.getSql());

    byte[][] parameters = {"userName".getBytes(), "20".getBytes(), "30".getBytes()};
    IntermediatePortalStatement intermediatePortalStatement =
        intermediateStatement.createPortal(
            "", parameters, Arrays.asList((short) 0, (short) 0, (short) 0), new ArrayList<>());
    intermediatePortalStatement.bind(Statement.of(sqlStatement));
    intermediatePortalStatement.executeAsync(backendConnection);
    backendConnection.flush();

    verify(connection).execute(statement);

    assertEquals(sqlStatement, intermediatePortalStatement.getSql());
    assertEquals("SELECT", intermediatePortalStatement.getCommand());
    assertTrue(intermediatePortalStatement.isExecuted());
    assertTrue(intermediateStatement.isBound());
  }

  @Test
  public void testPreparedStatementIllegalTypeThrowsException() {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    SessionState sessionState = mock(SessionState.class);
    when(backendConnection.getSessionState()).thenReturn(sessionState);

    String sqlStatement = "SELECT * FROM users WHERE metadata = $1";
    int[] parameterDataTypes = new int[] {Oid.JSON};

    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            "",
            parameterDataTypes,
            parse(sqlStatement),
            Statement.of(sqlStatement));

    byte[][] parameters = {"{}".getBytes()};

    IntermediatePortalStatement portalStatement =
        intermediateStatement.createPortal("", parameters, new ArrayList<>(), new ArrayList<>());
    assertThrows(
        IllegalArgumentException.class, () -> portalStatement.bind(Statement.of(sqlStatement)));
  }

  @Test
  public void testPreparedStatementDescribeDoesNotThrowException() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    String sqlStatement = "SELECT * FROM users WHERE name = $1 AND age > $2 AND age < $3";

    int[] parameters = new int[3];
    Arrays.fill(parameters, Oid.INT8);
    IntermediatePreparedStatement intermediateStatement =
        new IntermediatePreparedStatement(
            connectionHandler,
            options,
            "",
            parameters,
            parse(sqlStatement),
            Statement.of(sqlStatement));

    intermediateStatement.describe();
  }

  @Test
  public void testPortalStatementDescribePropagatesFailure() {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    String sqlStatement = "SELECT * FROM users WHERE age > $1 AND age < $2 AND name = $3";

    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler,
                options,
                "",
                NO_PARAMETER_TYPES,
                parse(sqlStatement),
                Statement.of(sqlStatement)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());
    BackendConnection backendConnection =
        new BackendConnection(
            connectionHandler.getDatabaseId(),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

    when(connection.execute(Statement.of(sqlStatement)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "test error"));

    intermediateStatement.describeAsync(backendConnection);
    backendConnection.flush();

    assertTrue(intermediateStatement.hasException());
    PGException exception = intermediateStatement.getException();
    assertEquals(SQLState.RaiseException, exception.getSQLState());
    assertEquals("test error", exception.getMessage());
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    SessionState sessionState = new SessionState(options);
    when(backendConnection.getSessionState()).thenReturn(sessionState);
    setupQueryInformationSchemaResults();

    String sql = "COPY keyvalue FROM STDIN;";
    CopyStatement statement =
        (CopyStatement)
            CopyStatement.create(
                connectionHandler, mock(OptionsMetadata.class), "", parse(sql), Statement.of(sql));

    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            ImmutableList::of);
    statement.executeAsync(backendConnection);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(
        () -> {
          byte[] payload = "2 3\n".getBytes();
          MutationWriter mutationWriter = statement.getMutationWriter();
          mutationWriter.addCopyData(payload);
        });
    executor.shutdown();

    backendConnection.flush();

    PGException thrown = assertThrows(PGException.class, statement::getUpdateCount);
    assertEquals(SQLState.DataException, thrown.getSQLState());
    assertEquals(
        "Invalid COPY data: Row length mismatched. Expected 2 columns, but only found 1",
        thrown.getMessage());

    statement.close();
    verify(resultSet, never()).close();
  }

  @Test
  public void testIntermediateStatementExecuteAsyncIsUnsupported() {
    String sql = "select * from foo";

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
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
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    IntermediatePortalStatement intermediateStatement =
        new IntermediatePortalStatement(
            "",
            new IntermediatePreparedStatement(
                connectionHandler, options, "", NO_PARAMETER_TYPES, parse(sql), Statement.of(sql)),
            NO_PARAMS,
            ImmutableList.of(),
            ImmutableList.of());
    BackendConnection backendConnection =
        new BackendConnection(
            connectionHandler.getDatabaseId(),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

    intermediateStatement.executeAsync(backendConnection);

    assertThrows(IllegalStateException.class, intermediateStatement::getStatementResult);
  }

  @Test
  public void testCopyBatchSizeLimit() throws Exception {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    SessionState sessionState = new SessionState(options);
    when(backendConnection.getSessionState()).thenReturn(sessionState);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    setupQueryInformationSchemaResults();
    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            ImmutableList::of);

    byte[] payload = Files.readAllBytes(Paths.get("./src/test/resources/batch-size-test.txt"));

    String sql = "COPY keyvalue FROM STDIN;";
    CopyStatement copyStatement =
        (CopyStatement)
            CopyStatement.create(
                connectionHandler, mock(OptionsMetadata.class), "", parse(sql), Statement.of(sql));

    assertFalse(copyStatement.isExecuted());
    copyStatement.executeAsync(backendConnection);
    assertTrue(copyStatement.isExecuted());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<MutationWriter> future =
        executor.submit(
            () -> {
              MutationWriter mw = copyStatement.getMutationWriter();
              mw.addCopyData(payload);
              mw.close();
              return mw;
            });
    executor.shutdown();

    backendConnection.flush();

    assertEquals("TEXT", copyStatement.getFormatType());
    assertEquals('\t', copyStatement.getDelimiterChar());
    assertFalse(copyStatement.hasException());
    assertEquals(12L, copyStatement.getUpdateCount());
    assertEquals(12L, future.get().getRowCount());

    copyStatement.close();
  }

  @Test
  public void testCopyDataRowLengthMismatchLimit() throws Exception {
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    SessionState sessionState = new SessionState(options);
    when(backendConnection.getSessionState()).thenReturn(sessionState);
    setupQueryInformationSchemaResults();
    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            ImmutableList::of);

    byte[] payload = "1\t'one'\n2".getBytes();

    String sql = "COPY keyvalue FROM STDIN;";
    CopyStatement copyStatement =
        (CopyStatement)
            CopyStatement.create(
                connectionHandler, mock(OptionsMetadata.class), "", parse(sql), Statement.of(sql));

    assertFalse(copyStatement.isExecuted());
    copyStatement.executeAsync(backendConnection);
    assertTrue(copyStatement.isExecuted());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<MutationWriter> future =
        executor.submit(
            () -> {
              MutationWriter mw = copyStatement.getMutationWriter();
              mw.addCopyData(payload);
              mw.close();
              return mw;
            });
    executor.shutdown();
    ;

    backendConnection.flush();

    PGException thrown = assertThrows(PGException.class, copyStatement::getUpdateCount);
    assertEquals(SQLState.DataException, thrown.getSQLState());
    assertEquals(
        "Invalid COPY data: Row length mismatched. Expected 2 columns, but only found 1",
        thrown.getMessage());

    copyStatement.close();

    deleteLogFile();
  }

  private void setupQueryInformationSchemaResults() {
    DatabaseClient databaseClient = mock(DatabaseClient.class);
    ReadContext singleUseReadContext = mock(ReadContext.class);
    when(databaseClient.singleUse()).thenReturn(singleUseReadContext);
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.getDatabaseClient()).thenReturn(databaseClient);
    ResultSet spannerType = mock(ResultSet.class);
    when(spannerType.getString("column_name")).thenReturn("key", "value");
    when(spannerType.getString("data_type")).thenReturn("bigint", "character varying");
    when(spannerType.next()).thenReturn(true, true, false);
    when(singleUseReadContext.executeQuery(
            ArgumentMatchers.argThat(
                statement ->
                    statement != null && statement.getSql().startsWith("SELECT column_name"))))
        .thenReturn(spannerType);

    ResultSet countResult = mock(ResultSet.class);
    when(countResult.getLong(0)).thenReturn(2L);
    when(countResult.next()).thenReturn(true, false);
    when(singleUseReadContext.executeQuery(
            ArgumentMatchers.argThat(
                statement ->
                    statement != null && statement.getSql().startsWith("SELECT COUNT(*)"))))
        .thenReturn(countResult);
  }
}
