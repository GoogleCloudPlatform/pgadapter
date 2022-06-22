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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.utils.CopyDataReceiver;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
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
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
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
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    onlyDmlStatements.execute(parsedUpdateStatement, updateStatement);
    onlyDmlStatements.execute(parsedUpdateStatement, updateStatement);
    assertTrue(onlyDmlStatements.hasDmlOrCopyStatementsAfter(0));
    assertTrue(onlyDmlStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyCopyStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    onlyCopyStatements.executeCopy(parsedCopyStatement, copyStatement, receiver, writer, executor);
    onlyCopyStatements.executeCopy(parsedCopyStatement, copyStatement, receiver, writer, executor);
    assertTrue(onlyCopyStatements.hasDmlOrCopyStatementsAfter(0));
    assertTrue(onlyCopyStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection dmlAndCopyStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    dmlAndCopyStatements.execute(parsedUpdateStatement, updateStatement);
    dmlAndCopyStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    assertTrue(dmlAndCopyStatements.hasDmlOrCopyStatementsAfter(0));
    assertTrue(dmlAndCopyStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlySelectStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    onlySelectStatements.execute(parsedSelectStatement, selectStatement);
    onlySelectStatements.execute(parsedSelectStatement, selectStatement);
    assertFalse(onlySelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlySelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyClientSideStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    onlyClientSideStatements.execute(parsedClientSideStatement, clientSideStatement);
    onlyClientSideStatements.execute(parsedClientSideStatement, clientSideStatement);
    assertFalse(onlyClientSideStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlyClientSideStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection onlyUnknownStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    onlyUnknownStatements.execute(parsedUnknownStatement, unknownStatement);
    onlyUnknownStatements.execute(parsedUnknownStatement, unknownStatement);
    assertFalse(onlyUnknownStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(onlyUnknownStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection dmlAndSelectStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    dmlAndSelectStatements.execute(parsedUpdateStatement, updateStatement);
    dmlAndSelectStatements.execute(parsedSelectStatement, selectStatement);
    assertTrue(dmlAndSelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(dmlAndSelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection copyAndSelectStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    copyAndSelectStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    copyAndSelectStatements.execute(parsedSelectStatement, selectStatement);
    assertTrue(copyAndSelectStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(copyAndSelectStatements.hasDmlOrCopyStatementsAfter(1));

    BackendConnection copyAndUnknownStatements =
        new BackendConnection(spannerConnection, DdlTransactionMode.Batch);
    copyAndUnknownStatements.executeCopy(
        parsedCopyStatement, copyStatement, receiver, writer, executor);
    copyAndUnknownStatements.execute(parsedUnknownStatement, unknownStatement);
    assertTrue(copyAndUnknownStatements.hasDmlOrCopyStatementsAfter(0));
    assertFalse(copyAndUnknownStatements.hasDmlOrCopyStatementsAfter(1));
  }
}
