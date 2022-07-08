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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(PythonTest.class)
@RunWith(JUnit4.class)
public class PythonBatchTests extends PythonTestSetup {

  @Test
  public void testInsertUsingExecuteMany() throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE VALUES(%s, %s)";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("2");
    parameters.add("hello");
    parameters.add("world");
    parameters.add("hello1");
    parameters.add("world1");

    String sql1 = "INSERT INTO SOME_TABLE VALUES('hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 10));
    String sql2 = "INSERT INTO SOME_TABLE VALUES('hello1', 'world1')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 20));

    String actualOutput = executeInBatch(pgServer.getLocalPort(), sql, "execute_many", parameters);
    String expectedOutput = "30\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql1, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1).getSql());
  }

  @Test
  public void testInsertUsingExecuteBatch() throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE VALUES(%s, %s)";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("2");
    parameters.add("hello");
    parameters.add("world");
    parameters.add("hello1");
    parameters.add("world1");

    String sql1 = "INSERT INTO SOME_TABLE VALUES('hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 23));
    String sql2 = "INSERT INTO SOME_TABLE VALUES('hello1', 'world1')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 27));

    String actualOutput = executeInBatch(pgServer.getLocalPort(), sql, "execute_batch", parameters);
    String expectedOutput = "27\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    assertEquals(
        2, mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0).getStatementsCount());
    assertEquals(
        sql1,
        mockSpanner
            .getRequestsOfType(ExecuteBatchDmlRequest.class)
            .get(0)
            .getStatements(0)
            .getSql());
    assertEquals(
        sql2,
        mockSpanner
            .getRequestsOfType(ExecuteBatchDmlRequest.class)
            .get(0)
            .getStatements(1)
            .getSql());
  }

  @Test
  public void testInsertUsingExecuteValues() throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE VALUES(%s)";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("2");
    parameters.add("hello");
    parameters.add("world");
    parameters.add("hello1");
    parameters.add("world1");

    String sql1 = "INSERT INTO SOME_TABLE VALUES(('hello','world'),('hello1','world1'))";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 57));

    String actualOutput =
        executeInBatch(pgServer.getLocalPort(), sql, "execute_values", parameters);
    String expectedOutput = "57\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql1, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void testInsertUsingExecuteManyWithNamedParameters()
      throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE VALUES(%(VALUE1)s, %(VALUE2)s)";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("2");
    parameters.add("VALUE1");
    parameters.add("HELLO");
    parameters.add("VALUE2");
    parameters.add("WORLD");
    parameters.add("VALUE1");
    parameters.add("HELLO1");
    parameters.add("VALUE2");
    parameters.add("WORLD1");
    String sql1 = "INSERT INTO SOME_TABLE VALUES('HELLO', 'WORLD')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 29));
    String sql2 = "INSERT INTO SOME_TABLE VALUES('HELLO1', 'WORLD1')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 37));

    String actualOutput =
        executeInBatch(pgServer.getLocalPort(), sql, "named_execute_many", parameters);
    String expectedOutput = "66\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql1, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
    assertEquals(sql2, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(1).getSql());
  }

  @Test
  public void testInsertUsingExecuteBatchWithNamedParameters()
      throws IOException, InterruptedException {
    String sql = "INSERT INTO SOME_TABLE VALUES(%(VALUE1)s, %(VALUE2)s)";
    ArrayList<String> parameters = new ArrayList<>();

    parameters.add("2");
    parameters.add("VALUE1");
    parameters.add("hello");
    parameters.add("VALUE2");
    parameters.add("world");
    parameters.add("VALUE1");
    parameters.add("hello1");
    parameters.add("VALUE2");
    parameters.add("world1");
    String sql1 = "INSERT INTO SOME_TABLE VALUES('hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql1), 23));
    String sql2 = "INSERT INTO SOME_TABLE VALUES('hello1', 'world1')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql2), 27));

    String actualOutput =
        executeInBatch(pgServer.getLocalPort(), sql, "named_execute_batch", parameters);
    String expectedOutput = "27\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteBatchDmlRequest.class));
    assertEquals(
        2, mockSpanner.getRequestsOfType(ExecuteBatchDmlRequest.class).get(0).getStatementsCount());
    assertEquals(
        sql1,
        mockSpanner
            .getRequestsOfType(ExecuteBatchDmlRequest.class)
            .get(0)
            .getStatements(0)
            .getSql());
    assertEquals(
        sql2,
        mockSpanner
            .getRequestsOfType(ExecuteBatchDmlRequest.class)
            .get(0)
            .getStatements(1)
            .getSql());
  }
}
