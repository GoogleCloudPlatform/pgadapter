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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
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
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
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
import java.util.function.Function;
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
  public void testExecuteStatementsInBatch() {
    Connection spannerConnection = mock(Connection.class);
    SpannerBatchUpdateException expectedException = mock(SpannerBatchUpdateException.class);
    when(expectedException.getMessage()).thenReturn("Invalid statement");
    when(expectedException.getErrorCode()).thenReturn(ErrorCode.INVALID_ARGUMENT);
    when(expectedException.getUpdateCounts()).thenReturn(new long[] {1L, 0L});
    when(spannerConnection.runBatch()).thenThrow(expectedException);
    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());

    backendConnection.execute(
        PARSER.parse(Statement.of("CREATE TABLE \"Foo\" (id bigint primary key)")),
        Statement.of("CREATE TABLE \"Foo\" (id bigint primary key)"),
        Function.identity());
    backendConnection.execute(
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
    onlyDmlStatements.execute(parsedUpdateStatement, updateStatement, Function.identity());
    onlyDmlStatements.execute(parsedUpdateStatement, updateStatement, Function.identity());
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
    dmlAndCopyStatements.execute(parsedUpdateStatement, updateStatement, Function.identity());
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
    onlySelectStatements.execute(parsedSelectStatement, selectStatement, Function.identity());
    onlySelectStatements.execute(parsedSelectStatement, selectStatement, Function.identity());
    assertFalse(onlySelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlySelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyClientSideStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlyClientSideStatements.execute(
        parsedClientSideStatement, clientSideStatement, Function.identity());
    onlyClientSideStatements.execute(
        parsedClientSideStatement, clientSideStatement, Function.identity());
    assertFalse(onlyClientSideStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlyClientSideStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyUnknownStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    onlyUnknownStatements.execute(parsedUnknownStatement, unknownStatement, Function.identity());
    onlyUnknownStatements.execute(parsedUnknownStatement, unknownStatement, Function.identity());
    assertFalse(onlyUnknownStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlyUnknownStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection dmlAndSelectStatements =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            spannerConnection,
            mock(OptionsMetadata.class),
            ImmutableList.of());
    dmlAndSelectStatements.execute(parsedUpdateStatement, updateStatement, Function.identity());
    dmlAndSelectStatements.execute(parsedSelectStatement, selectStatement, Function.identity());
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
    copyAndSelectStatements.execute(parsedSelectStatement, selectStatement, Function.identity());
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
    copyAndUnknownStatements.execute(parsedUnknownStatement, unknownStatement, Function.identity());
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
            DatabaseId.of("p", "i", "d"), connection, mock(OptionsMetadata.class), localStatements);
    Future<StatementResult> resultFuture =
        backendConnection.execute(parsedStatement, statement, Function.identity());
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
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    Statement statement = Statement.of("select foo from bar");
    RuntimeException error = new RuntimeException("test error");
    when(connection.execute(statement)).thenThrow(error);

    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            mock(OptionsMetadata.class),
            EMPTY_LOCAL_STATEMENTS);
    Future<StatementResult> resultFuture =
        backendConnection.execute(parsedStatement, statement, Function.identity());
    backendConnection.flush();

    ExecutionException executionException =
        assertThrows(ExecutionException.class, resultFuture::get);
    assertEquals(executionException.getCause(), PGExceptionFactory.toPGException(error));
  }

  @Test
  public void testCancelledException() {
    Connection connection = mock(Connection.class);
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    Statement statement = Statement.of("select foo from bar");
    SpannerException error =
        SpannerExceptionFactory.newSpannerException(ErrorCode.CANCELLED, "query cancelled");
    when(connection.execute(statement)).thenThrow(error);

    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            mock(OptionsMetadata.class),
            EMPTY_LOCAL_STATEMENTS);
    Future<StatementResult> resultFuture =
        backendConnection.execute(parsedStatement, statement, Function.identity());
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
    when(connection.isDdlBatchActive()).thenReturn(true);

    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            mock(OptionsMetadata.class),
            EMPTY_LOCAL_STATEMENTS);
    Future<StatementResult> resultFuture1 =
        backendConnection.execute(parsedStatement1, statement1, Function.identity());
    backendConnection.execute(parsedStatement2, statement2, Function.identity());
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
            DatabaseId.of("p", "i", "d"), connection, options, EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute(parsedStatement, statement, Function.identity());
    backendConnection.flush();

    verify(connection)
        .execute(
            Statement.of(
                "with pg_namespace as (\n"
                    + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
                    + "        schema_name as nspname, null as nspowner, null as nspacl\n"
                    + "  from information_schema.schemata\n"
                    + "),\n"
                    + "pg_type as (\n"
                    + "  select 16 as oid, 'bool' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 17 as oid, 'bytea' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 20 as oid, 'int8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 21 as oid, 'int2' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 23 as oid, 'int4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 25 as oid, 'text' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 700 as oid, 'float4' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 701 as oid, 'float8' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1043 as oid, 'varchar' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1082 as oid, 'date' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1114 as oid, 'timestamp' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1184 as oid, 'timestamptz' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 1700 as oid, 'numeric' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
                    + "  select 3802 as oid, 'jsonb' as typname, (select oid from pg_namespace where nspname='pg_catalog') as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl\n"
                    + ")\n"
                    + "select * from pg_type"));
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
            DatabaseId.of("p", "i", "d"), connection, options, EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute(parsedStatement, statement, Function.identity());
    backendConnection.flush();

    verify(connection).execute(statement);
  }

  @Test
  public void testDoNotStartTransactionInBatch() {
    Connection connection = mock(Connection.class);
    when(connection.isDmlBatchActive()).thenReturn(true);
    Statement statement = Statement.of("insert into foo values (1)");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    BackendConnection backendConnection =
        new BackendConnection(
            DatabaseId.of("p", "i", "d"),
            connection,
            mock(OptionsMetadata.class),
            EMPTY_LOCAL_STATEMENTS);

    backendConnection.execute(parsedStatement, statement, Function.identity());
    backendConnection.flush();

    verify(connection).execute(statement);
    verify(connection, never()).beginTransaction();
  }
}
