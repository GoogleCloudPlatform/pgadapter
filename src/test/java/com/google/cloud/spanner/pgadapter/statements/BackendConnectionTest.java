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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.DdlExecutor.NotExecuted;
import com.google.cloud.spanner.pgadapter.statements.local.ListDatabasesStatement;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.cloud.spanner.pgadapter.utils.CopyDataReceiver;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BackendConnectionTest {
  private final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
  private static final NotExecuted NOT_EXECUTED = new NotExecuted();
  private static final NoResult NO_RESULT = new NoResult();

  @Test
  public void testExtractDdlUpdateCounts() {
    assertArrayEquals(new long[] {}, extractDdlUpdateCounts(ImmutableList.of(), new long[] {}));
    assertArrayEquals(
        new long[] {1L}, extractDdlUpdateCounts(ImmutableList.of(NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(ImmutableList.of(NO_RESULT, NOT_EXECUTED), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(ImmutableList.of(NOT_EXECUTED, NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NO_RESULT, NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NO_RESULT, NOT_EXECUTED), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NOT_EXECUTED, NO_RESULT, NOT_EXECUTED),
            new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NOT_EXECUTED, NO_RESULT, NOT_EXECUTED, NO_RESULT),
            new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NO_RESULT, NO_RESULT, NOT_EXECUTED), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(
                NOT_EXECUTED,
                NO_RESULT,
                NO_RESULT,
                NOT_EXECUTED,
                NOT_EXECUTED,
                NO_RESULT,
                NO_RESULT),
            new long[] {1L, 1L, 1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NOT_EXECUTED, NOT_EXECUTED), new long[] {}));
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
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
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
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlyDmlStatements.execute(parsedUpdateStatement, updateStatement);
    onlyDmlStatements.execute(parsedUpdateStatement, updateStatement);
    assertTrue(onlyDmlStatements.hasDmlOrCopyStatementsAfter(0));
    assertTrue(onlyDmlStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyCopyStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlyCopyStatements.executeCopy(parsedCopyStatement, copyStatement, receiver, writer, executor);
    onlyCopyStatements.executeCopy(parsedCopyStatement, copyStatement, receiver, writer, executor);
    assertTrue(onlyCopyStatements.hasDmlOrCopyStatementsAfter(0));
    assertTrue(onlyCopyStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection dmlAndCopyStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    dmlAndCopyStatements.execute(parsedUpdateStatement, updateStatement);
    dmlAndCopyStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    assertTrue(dmlAndCopyStatements.hasDmlOrCopyStatementsAfter(0));
    assertTrue(dmlAndCopyStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlySelectStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlySelectStatements.execute(parsedSelectStatement, selectStatement);
    onlySelectStatements.execute(parsedSelectStatement, selectStatement);
    assertFalse(onlySelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlySelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyClientSideStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlyClientSideStatements.execute(parsedClientSideStatement, clientSideStatement);
    onlyClientSideStatements.execute(parsedClientSideStatement, clientSideStatement);
    assertFalse(onlyClientSideStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlyClientSideStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyUnknownStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlyUnknownStatements.execute(parsedUnknownStatement, unknownStatement);
    onlyUnknownStatements.execute(parsedUnknownStatement, unknownStatement);
    assertFalse(onlyUnknownStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlyUnknownStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection dmlAndSelectStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    dmlAndSelectStatements.execute(parsedUpdateStatement, updateStatement);
    dmlAndSelectStatements.execute(parsedSelectStatement, selectStatement);
    assertTrue(dmlAndSelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(dmlAndSelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection copyAndSelectStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    copyAndSelectStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    copyAndSelectStatements.execute(parsedSelectStatement, selectStatement);
    assertTrue(copyAndSelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(copyAndSelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection copyAndUnknownStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    copyAndUnknownStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    copyAndUnknownStatements.execute(parsedUnknownStatement, unknownStatement);
    assertTrue(copyAndUnknownStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(copyAndUnknownStatements.hasDmlOrCopyStatementsAfter(1));
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
            DatabaseId.of("p", "i", "d"), connection, mock(OptionsMetadata.class), localStatements);
    Future<StatementResult> resultFuture =
        backendConnection.execute(
            parsedListDatabasesStatement, Statement.of(ListDatabasesStatement.LIST_DATABASES_SQL));
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
            DatabaseId.of("p", "i", "d"), connection, mock(OptionsMetadata.class), localStatements);
    Future<StatementResult> resultFuture = backendConnection.execute(parsedStatement, statement);
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
}
