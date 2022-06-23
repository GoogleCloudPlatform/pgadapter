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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.QueryResult;
import com.google.cloud.spanner.pgadapter.statements.local.ListDatabasesStatement;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BackendConnectionTest {

  @Test
  public void testExecuteLocalStatement() throws ExecutionException, InterruptedException {
    Connection connection = mock(Connection.class);
    StatementResult listDatabasesResult = mock(StatementResult.class);
    when(listDatabasesResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    ListDatabasesStatement listDatabasesStatement = mock(ListDatabasesStatement.class);
    when(listDatabasesStatement.getSql()).thenReturn(ListDatabasesStatement.LIST_DATABASES_SQL);
    when(listDatabasesStatement.execute(connection)).thenReturn(listDatabasesResult);
    ImmutableList<LocalStatement> localStatements = ImmutableList.of(listDatabasesStatement);
    ParsedStatement parsedListDatabasesStatement = mock(ParsedStatement.class);
    when(parsedListDatabasesStatement.getSqlWithoutComments())
        .thenReturn(ListDatabasesStatement.LIST_DATABASES_SQL);

    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, localStatements);
    Future<StatementResult> resultFuture =
        backendConnection.execute(
            parsedListDatabasesStatement, Statement.of(ListDatabasesStatement.LIST_DATABASES_SQL));
    backendConnection.flush();

    verify(listDatabasesStatement).execute(connection);
    assertTrue(resultFuture.isDone());
    assertEquals(listDatabasesResult, resultFuture.get());
  }

  @Test
  public void testExecuteOtherStatementWithLocalStatements()
      throws ExecutionException, InterruptedException {
    Connection connection = mock(Connection.class);
    ListDatabasesStatement listDatabasesStatement = mock(ListDatabasesStatement.class);
    when(listDatabasesStatement.getSql()).thenReturn(ListDatabasesStatement.LIST_DATABASES_SQL);
    ImmutableList<LocalStatement> localStatements = ImmutableList.of(listDatabasesStatement);

    StatementResult statementResult = mock(StatementResult.class);
    when(statementResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    ParsedStatement parsedStatement = mock(ParsedStatement.class);
    String sql = "SELECT * FROM foo";
    when(parsedStatement.getSqlWithoutComments()).thenReturn(sql);
    Statement statement = Statement.of(sql);
    when(connection.execute(statement)).thenReturn(statementResult);

    BackendConnection backendConnection =
        new BackendConnection(connection, DdlTransactionMode.Batch, localStatements);
    Future<StatementResult> resultFuture = backendConnection.execute(parsedStatement, statement);
    backendConnection.flush();

    verify(listDatabasesStatement, never()).execute(connection);
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
