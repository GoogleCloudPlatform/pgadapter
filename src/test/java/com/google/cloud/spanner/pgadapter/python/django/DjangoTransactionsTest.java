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

package com.google.cloud.spanner.pgadapter.python.django;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.RollbackRequest;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class DjangoTransactionsTest extends DjangoTestSetup {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"} /*, new Object[] {"/tmp"}*/);
  }

  @Test
  public void transactionCommitTest() throws Exception {

    String updateSQL1 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<String>();
    options.add("commit");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "Transaction Committed\n";

    assertEquals(expectedOutput, actualOutput);
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void transactionRollbackTest() throws Exception {

    String updateSQL1 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<String>();
    options.add("rollback");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "Transaction Rollbacked\n";

    assertEquals(expectedOutput, actualOutput);
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void transactionAtomicTest() throws Exception {

    String updateSQL1 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<String>();
    options.add("atomic");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "Atomic Successful\n";

    assertEquals(expectedOutput, actualOutput);
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void transactionNestedAtomicTest() throws Exception {

    String updateSQL1 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<String>();
    options.add("nested_atomic");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "Atomic Successful\n";

    assertEquals(expectedOutput, actualOutput);
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void transactionErrorTest() throws Exception {

    String updateSQL1 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(
        StatementResult.exception(
            Statement.of(insertSQL1),
            Status.ALREADY_EXISTS.withDescription("Row [1] already exists").asRuntimeException()));

    String updateSQL2 =
        "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 =
        "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<>();
    options.add("error_during_transaction");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "current transaction is aborted, commands ignored until end of transaction block\n" + "\n";

    assertEquals(expectedOutput, actualOutput);
    // since the error has occurred in between the transaction,
    // it will be rolled back
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }
}
