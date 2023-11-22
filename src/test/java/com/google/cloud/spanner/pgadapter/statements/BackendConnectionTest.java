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

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.extractDdlUpdateCounts;
import static com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.EMPTY_LOCAL_STATEMENTS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerBatchUpdateException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.connection.TransactionMode;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.local.ListDatabasesStatement;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.utils.CopyDataReceiver;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BackendConnectionTest {
  private static final Tracer NOOP_OTEL = OpenTelemetry.noop().getTracer("test");
  private final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
  private static final NoResult NO_RESULT = new NoResult();
  private static final Runnable DO_NOTHING = () -> {};

  @Test
  public void testExtractDdlUpdateCounts() {
    assertArrayEquals(new long[] {}, extractDdlUpdateCounts(ImmutableList.of(), new long[] {}));
    assertArrayEquals(
        new long[] {1L}, extractDdlUpdateCounts(ImmutableList.of(NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(ImmutableList.of(NO_RESULT, NO_RESULT), new long[] {1L, 1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NO_RESULT, NO_RESULT, NO_RESULT), new long[] {1L, 1L, 1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NO_RESULT, NO_RESULT, NO_RESULT, NO_RESULT),
            new long[] {1L, 1L, 1L, 1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(
                NO_RESULT, NO_RESULT, NO_RESULT, NO_RESULT, NO_RESULT, NO_RESULT, NO_RESULT),
            new long[] {1L, 1L, 1L, 1L, 1L, 1L}));
  }

  @Test
  public void testExecuteStatementsInBatch() {
    Connection spannerConnection = mock(Connection.class);
    SpannerBatchUpdateException expectedException = mock(SpannerBatchUpdateException.class);
    when(expectedException.getMessage()).thenReturn("Invalid statement");
    when(expectedException.getErrorCode()).thenReturn(ErrorCode.INVALID_ARGUMENT);
    when(expectedException.getUpdateCounts()).thenReturn(new long[] {1L, 0L});
    when(spannerConnection.runBatch()).thenThrow(expectedException);
    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);

    backendConnection.execute(
        "CREATE",
        PARSER.parse(Statement.of("CREATE TABLE \"Foo\" (id bigint primary key)")),
        Statement.of("CREATE TABLE \"Foo\" (id bigint primary key)"),
        Function.identity());
    backendConnection.execute(
        "CREATE",
        PARSER.parse(Statement.of("CREATE TABLE bar (id bigint primary key, value text)")),
        Statement.of("CREATE TABLE bar (id bigint primary key, value text)"),
        Function.identity());

    SpannerBatchUpdateException batchUpdateException =
        assertThrows(
            SpannerBatchUpdateException.class, () -> backendConnection.executeStatementsInBatch(0));
    assertSame(expectedException, batchUpdateException);
  }

  @Test
  public void testCopyPropagatesNonSpannerException() {
    String sql = "COPY users FROM STDIN";
    Statement statement = Statement.of(sql);
    ParsedStatement parsedStatement = PARSER.parse(statement);
    Connection spannerConnection = mock(Connection.class);
    CopyDataReceiver receiver = mock(CopyDataReceiver.class);
    MutationWriter writer = mock(MutationWriter.class);
    ExecutorService executor = mock(ExecutorService.class);
    RejectedExecutionException rejectedExecutionException = new RejectedExecutionException();
    doThrow(rejectedExecutionException).when(executor).execute(any(Runnable.class));

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    Future<StatementResult> result =
        backendConnection.executeCopy(parsedStatement, statement, receiver, writer, executor);
    backendConnection.flush();

    assertTrue(result.isDone());
    ExecutionException executionException = assertThrows(ExecutionException.class, result::get);
    assertSame(rejectedExecutionException, executionException.getCause());
  }

  @Test
  public void testHasDmlOrCopyStatementsAfter() {
    CopyDataReceiver receiver = mock(CopyDataReceiver.class);
    MutationWriter writer = mock(MutationWriter.class);
    ExecutorService executor = mock(ExecutorService.class);
    Connection spannerConnection = mock(Connection.class);
    String updateSql = "insert into foo values (1)";
    Statement updateStatement = Statement.of(updateSql);
    ParsedStatement parsedUpdateStatement = PARSER.parse(updateStatement);
    String selectSql = "select * from foo";
    Statement selectStatement = Statement.of(selectSql);
    ParsedStatement parsedSelectStatement = PARSER.parse(selectStatement);
    String copySql = "copy foo from stdin";
    Statement copyStatement = Statement.of(copySql);
    ParsedStatement parsedCopyStatement = PARSER.parse(copyStatement);
    String clientSideSql = "set spanner.statement_tag='foo'";
    Statement clientSideStatement = Statement.of(clientSideSql);
    ParsedStatement parsedClientSideStatement = PARSER.parse(clientSideStatement);
    String unknownSql = "merge foo";
    Statement unknownStatement = Statement.of(unknownSql);
    ParsedStatement parsedUnknownStatement = PARSER.parse(unknownStatement);

    BackendConnection onlyDmlStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    onlyDmlStatements.execute(
        "INSERT", parsedUpdateStatement, updateStatement, Function.identity());
    onlyDmlStatements.execute(
        "INSERT", parsedUpdateStatement, updateStatement, Function.identity());
    assertTrue(onlyDmlStatements.hasUpdateStatementsAfter(0));
    assertTrue(onlyDmlStatements.hasUpdateStatementsAfter(1));

    BackendConnection onlyCopyStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    onlyCopyStatements.executeCopy(parsedCopyStatement, copyStatement, receiver, writer, executor);
    onlyCopyStatements.executeCopy(parsedCopyStatement, copyStatement, receiver, writer, executor);
    assertTrue(onlyCopyStatements.hasUpdateStatementsAfter(0));
    assertTrue(onlyCopyStatements.hasUpdateStatementsAfter(1));

    BackendConnection dmlAndCopyStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    dmlAndCopyStatements.execute(
        "INSERT", parsedUpdateStatement, updateStatement, Function.identity());
    dmlAndCopyStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    assertTrue(dmlAndCopyStatements.hasUpdateStatementsAfter(0));
    assertTrue(dmlAndCopyStatements.hasUpdateStatementsAfter(1));

    BackendConnection onlySelectStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    onlySelectStatements.execute(
        "SELECT", parsedSelectStatement, selectStatement, Function.identity());
    onlySelectStatements.execute(
        "SELECT", parsedSelectStatement, selectStatement, Function.identity());
    assertFalse(onlySelectStatements.hasUpdateStatementsAfter(0));
    assertFalse(onlySelectStatements.hasUpdateStatementsAfter(1));

    BackendConnection onlyClientSideStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    onlyClientSideStatements.execute(
        "SET", parsedClientSideStatement, clientSideStatement, Function.identity());
    onlyClientSideStatements.execute(
        "SET", parsedClientSideStatement, clientSideStatement, Function.identity());
    assertFalse(onlyClientSideStatements.hasUpdateStatementsAfter(0));
    assertFalse(onlyClientSideStatements.hasUpdateStatementsAfter(1));

    BackendConnection onlyUnknownStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    onlyUnknownStatements.execute(
        "", parsedUnknownStatement, unknownStatement, Function.identity());
    onlyUnknownStatements.execute(
        "", parsedUnknownStatement, unknownStatement, Function.identity());
    assertFalse(onlyUnknownStatements.hasUpdateStatementsAfter(0));
    assertFalse(onlyUnknownStatements.hasUpdateStatementsAfter(1));

    BackendConnection dmlAndSelectStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    dmlAndSelectStatements.execute(
        "INSERT", parsedUpdateStatement, updateStatement, Function.identity());
    dmlAndSelectStatements.execute(
        "INSERT", parsedSelectStatement, selectStatement, Function.identity());
    assertTrue(dmlAndSelectStatements.hasUpdateStatementsAfter(0));
    assertFalse(dmlAndSelectStatements.hasUpdateStatementsAfter(1));

    BackendConnection copyAndSelectStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    copyAndSelectStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    copyAndSelectStatements.execute(
        "SELECT", parsedSelectStatement, selectStatement, Function.identity());
    assertTrue(copyAndSelectStatements.hasUpdateStatementsAfter(0));
    assertFalse(copyAndSelectStatements.hasUpdateStatementsAfter(1));

    BackendConnection copyAndUnknownStatements =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            ImmutableList::of);
    copyAndUnknownStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    copyAndUnknownStatements.execute(
        "", parsedUnknownStatement, unknownStatement, Function.identity());
    assertTrue(copyAndUnknownStatements.hasUpdateStatementsAfter(0));
    assertFalse(copyAndUnknownStatements.hasUpdateStatementsAfter(1));
  }

  @Test
  public void testExecuteLocalStatement() throws ExecutionException, InterruptedException {
    Connection connection = mock(Connection.class);
    StatementResult listDatabasesResult = mock(StatementResult.class);
    when(listDatabasesResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    ListDatabasesStatement listDatabasesStatement = mock(ListDatabasesStatement.class);
    when(listDatabasesStatement.getSql())
        .thenReturn(new String[] {ListDatabasesStatement.LIST_DATABASES_SQL});
    when(listDatabasesStatement.execute(any(BackendConnection.class)))
        .thenReturn(listDatabasesResult);
    ImmutableList<LocalStatement> localStatements = ImmutableList.of(listDatabasesStatement);
    ParsedStatement parsedListDatabasesStatement = mock(ParsedStatement.class);
    when(parsedListDatabasesStatement.getSqlWithoutComments())
        .thenReturn(ListDatabasesStatement.LIST_DATABASES_SQL);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> localStatements);
    Future<StatementResult> resultFuture =
        backendConnection.execute(
            "SHOW",
            parsedListDatabasesStatement,
            Statement.of(ListDatabasesStatement.LIST_DATABASES_SQL),
            Function.identity());
    backendConnection.flush();

    verify(listDatabasesStatement).execute(backendConnection);
    assertTrue(resultFuture.isDone());
    assertEquals(listDatabasesResult, resultFuture.get());
  }

  @Test
  public void testExecuteOtherStatementWithLocalStatements()
      throws ExecutionException, InterruptedException {
    Connection connection = mock(Connection.class);
    ListDatabasesStatement listDatabasesStatement = mock(ListDatabasesStatement.class);
    when(listDatabasesStatement.getSql())
        .thenReturn(new String[] {ListDatabasesStatement.LIST_DATABASES_SQL});
    ImmutableList<LocalStatement> localStatements = ImmutableList.of(listDatabasesStatement);

    StatementResult statementResult = mock(StatementResult.class);
    when(statementResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    String sql = "SELECT * FROM foo";
    when(parsedStatement.getSqlWithoutComments()).thenReturn(sql);
    Statement statement = Statement.of(sql);
    when(connection.execute(statement)).thenReturn(statementResult);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> localStatements);
    Future<StatementResult> resultFuture =
        backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.flush();

    verify(listDatabasesStatement, never()).execute(backendConnection);
    assertTrue(resultFuture.isDone());
    assertEquals(statementResult, resultFuture.get());
  }

  @Test
  public void testQueryResult() {
    ResultSet resultSet = mock(ResultSet.class);
    QueryResult queryResult = new QueryResult(resultSet);

    assertEquals(ResultType.RESULT_SET, queryResult.getResultType());
    assertEquals(resultSet, queryResult.getResultSet());
    assertThrows(UnsupportedOperationException.class, queryResult::getClientSideStatementType);
    assertThrows(UnsupportedOperationException.class, queryResult::getUpdateCount);
  }

  @Test
  public void testGeneralException() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select foo from bar");
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    when(parsedStatement.getSqlWithoutComments()).thenReturn(statement.getSql());
    RuntimeException error = new RuntimeException("test error");
    when(connection.execute(statement)).thenThrow(error);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);
    Future<StatementResult> resultFuture =
        backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.flush();

    ExecutionException executionException =
        assertThrows(ExecutionException.class, resultFuture::get);
    assertEquals(executionException.getCause(), PGExceptionFactory.toPGException(error));
  }

  @Test
  public void testCancelledException() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select foo from bar");
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    when(parsedStatement.getSqlWithoutComments()).thenReturn(statement.getSql());
    SpannerException error =
        SpannerExceptionFactory.newSpannerException(ErrorCode.CANCELLED, "query cancelled");
    when(connection.execute(statement)).thenThrow(error);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);
    Future<StatementResult> resultFuture =
        backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.flush();

    ExecutionException executionException =
        assertThrows(ExecutionException.class, resultFuture::get);
    assertEquals(PGException.class, executionException.getCause().getClass());
    PGException pgException = (PGException) executionException.getCause();
    assertEquals(SQLState.QueryCanceled, pgException.getSQLState());
  }

  @Test
  public void testDdlExceptionInBatch() {
    String sql1 = "create table foo (id bigint primary key)";
    String sql2 = "create table bar (id bigint primary key)";

    Connection connection = mock(Connection.class);
    ParsedStatement parsedStatement1 = mock(ParsedStatement.class);
    when(parsedStatement1.getType()).thenReturn(StatementType.DDL);
    when(parsedStatement1.getSqlWithoutComments()).thenReturn(sql1);

    ParsedStatement parsedStatement2 = mock(ParsedStatement.class);
    when(parsedStatement2.getType()).thenReturn(StatementType.DDL);
    when(parsedStatement2.getSqlWithoutComments()).thenReturn(sql2);

    Statement statement1 = Statement.of(sql1);
    Statement statement2 = Statement.of(sql2);
    when(connection.execute(statement1)).thenReturn(NO_RESULT);
    RuntimeException error = new RuntimeException("test error");
    when(connection.execute(statement2)).thenThrow(error);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);
    Future<StatementResult> resultFuture1 =
        backendConnection.execute("CREATE", parsedStatement1, statement1, Function.identity());
    backendConnection.execute("CREATE", parsedStatement2, statement2, Function.identity());
    backendConnection.flush();

    // The error will be set on the first statement in the batch, as the error occurs before
    // anything is actually executed on Cloud Spanner.
    ExecutionException executionException =
        assertThrows(ExecutionException.class, resultFuture1::get);
    assertSame(executionException.getCause(), error);
  }

  @Test
  public void testReplacePgCatalogTables() {
    Connection connection = mock(Connection.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.replacePgCatalogTables()).thenReturn(true);
    Statement statement = Statement.of("select * from pg_catalog.pg_type");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.flush();

    verify(connection)
        .execute(
            Statement.of(
                "with " + AbstractMockServerTest.PG_TYPE_PREFIX + "\nselect * from pg_type"));
  }

  @Test
  public void testDisableReplacePgCatalogTables() {
    Connection connection = mock(Connection.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.replacePgCatalogTables()).thenReturn(false);
    Statement statement = Statement.of("select * from pg_catalog.pg_type");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            options,
            () -> EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.flush();

    verify(connection).execute(statement);
  }

  @Test
  public void testDoNotStartTransactionInBatch() {
    for (boolean ddl : new boolean[] {true, false}) {
      Connection connection = mock(Connection.class);
      if (ddl) {
        when(connection.isDdlBatchActive()).thenReturn(true);
      } else {
        when(connection.isDmlBatchActive()).thenReturn(true);
      }
      Statement statement = Statement.of("insert into foo values (1)");
      ParsedStatement parsedStatement =
          AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

      BackendConnection backendConnection =
          new BackendConnection(
              NOOP_OTEL,
              UUID.randomUUID().toString(),
              DO_NOTHING,
              DatabaseId.of("p", "i", "d"),
              connection,
              () -> WellKnownClient.UNSPECIFIED,
              mock(OptionsMetadata.class),
              () -> EMPTY_LOCAL_STATEMENTS);

      backendConnection.execute("INSERT", parsedStatement, statement, Function.identity());
      backendConnection.flush();

      verify(connection).execute(statement);
      verify(connection, never()).beginTransaction();
    }
  }

  @Test
  public void testDescribeAndExecuteSameStatement_doesNotStartTransaction() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select * from foo where id=$1");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);

    // Describing and executing the same statement in one pipelined operation should not start an
    // implicit transaction.
    backendConnection.analyze("SELECT", parsedStatement, statement);
    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.sync();

    verify(connection).analyzeQuery(statement, QueryAnalyzeMode.PLAN);
    verify(connection).execute(statement);
    verify(connection, never()).beginTransaction();
  }

  @Test
  public void testDescribeAndExecuteDifferentStatements_startsTransaction() {
    Connection connection = mock(Connection.class);
    Statement statement1 = Statement.of("select * from foo where id=$1");
    ParsedStatement parsedStatement1 =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement1);
    Statement statement2 = Statement.of("select * from bar where id=$1");
    ParsedStatement parsedStatement2 =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement1);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);

    // Describing and executing different queries in one pipelined operation should start an
    // implicit read-only transaction.
    backendConnection.analyze("SELECT", parsedStatement1, statement1);
    backendConnection.execute("SELECT", parsedStatement2, statement2, Function.identity());
    backendConnection.sync();

    verify(connection).analyzeQuery(statement1, QueryAnalyzeMode.PLAN);
    verify(connection).execute(statement2);
    verify(connection).setTransactionMode(TransactionMode.READ_ONLY_TRANSACTION);
    verify(connection).beginTransaction();
  }

  @Test
  public void testExecuteSameStatementTwice_startsTransaction() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select * from foo where id=$1");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);

    // Executing the same query twice in one pipelined operation should start an
    // implicit read-only transaction.
    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.sync();

    verify(connection, times(2)).execute(statement);
    verify(connection).setTransactionMode(TransactionMode.READ_ONLY_TRANSACTION);
    verify(connection).beginTransaction();
  }

  @Test
  public void testExecuteAndCopy_startsTransaction() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select * from foo where id=$1");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.executeCopyOut(parsedStatement, statement);
    backendConnection.sync();

    verify(connection).execute(statement);
    verify(connection).setTransactionMode(TransactionMode.READ_ONLY_TRANSACTION);
    verify(connection).beginTransaction();
  }

  @Test
  public void testTruncateAndExecute_startsTransaction() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select * from foo where id=$1");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    String truncateSql = "truncate foo";
    TruncateStatement truncateStatement =
        new TruncateStatement(
            connectionHandler,
            mock(OptionsMetadata.class),
            "",
            AbstractStatementParser.getInstance(Dialect.POSTGRESQL)
                .parse(Statement.of(truncateSql)),
            Statement.of(truncateSql));

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute(truncateStatement);
    backendConnection.execute("SELECT", parsedStatement, statement, Function.identity());
    backendConnection.sync();

    verify(connection).execute(statement);
    verify(connection, never()).setTransactionMode(TransactionMode.READ_ONLY_TRANSACTION);
    verify(connection).beginTransaction();
  }

  @Test
  public void testExecuteAndVacuum_startsTransaction() {
    Connection connection = mock(Connection.class);
    Statement statement = Statement.of("select * from foo where id=$1");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    String vacuumSql = "vacuum foo";
    VacuumStatement vacuumStatement =
        new VacuumStatement(
            connectionHandler,
            mock(OptionsMetadata.class),
            "",
            AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(Statement.of(vacuumSql)),
            Statement.of(vacuumSql));

    BackendConnection backendConnection =
        new BackendConnection(
            NOOP_OTEL,
            UUID.randomUUID().toString(),
            DO_NOTHING,
            DatabaseId.of("p", "i", "d"),
            connection,
            () -> WellKnownClient.UNSPECIFIED,
            mock(OptionsMetadata.class),
            () -> EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute("VACUUM", parsedStatement, statement, Function.identity());
    backendConnection.execute(vacuumStatement);
    backendConnection.sync();

    verify(connection).execute(statement);
    verify(connection).setTransactionMode(TransactionMode.READ_ONLY_TRANSACTION);
    verify(connection).beginTransaction();
  }
}
