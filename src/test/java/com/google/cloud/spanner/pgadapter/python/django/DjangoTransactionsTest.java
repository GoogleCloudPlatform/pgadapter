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
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
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
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  private ResultSet createResultSet(List<String> rows) {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .setName("singerid")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("firstname")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("lastname")
                            .build())
                    .build())
            .build());
    for (int i = 0; i < rows.size(); i += 3) {
      String singerid = rows.get(i), firstname = rows.get(i + 1), lastname = rows.get(i + 2);
      resultSetBuilder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(singerid).build())
              .addValues(Value.newBuilder().setStringValue(firstname).build())
              .addValues(Value.newBuilder().setStringValue(lastname).build())
              .build());
    }
    return resultSetBuilder.build();
  }


  @Test
  public void transactionCommitTest() throws IOException, InterruptedException {

    String updateSQL1 = "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 = "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 = "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 = "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
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
  public void transactionRollbackTest() throws IOException, InterruptedException {

    String updateSQL1 = "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 = "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 = "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 = "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<String>();
    options.add("rollback");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "Transaction Rollbacked\n";

    assertEquals(expectedOutput, actualOutput);
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));

  }

  @Test
  public void transactionAtomicTest() throws IOException, InterruptedException {

    String updateSQL1 = "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'world' WHERE \"singers\".\"singerid\" = 1";
    String insertSQL1 = "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (1, 'hello', 'world')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL1), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL1), 1));

    String updateSQL2 = "UPDATE \"singers\" SET \"firstname\" = 'hello', \"lastname\" = 'python' WHERE \"singers\".\"singerid\" = 2";
    String insertSQL2 = "INSERT INTO \"singers\" (\"singerid\", \"firstname\", \"lastname\") VALUES (2, 'hello', 'python')";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(updateSQL2), 0));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insertSQL2), 1));

    List<String> options = new ArrayList<String>();
    options.add("atomic");

    String actualOutput = executeTransactionTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "Atomic Successful\n";

    assertEquals(expectedOutput, actualOutput);
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));

  }



}
