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

package com.google.cloud.spanner.pgadapter.python.pg8000;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FlushMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.TerminateMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class Pg8000BasicsTest extends AbstractMockServerTest {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  static String execute(String script, String host, int port)
      throws IOException, InterruptedException {
    String[] runCommand = new String[] {"python3", script, host, Integer.toString(port)};
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python/pg8000"));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());
    Scanner errorScanner = new Scanner(process.getErrorStream());

    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    StringBuilder error = new StringBuilder();
    while (errorScanner.hasNextLine()) {
      error.append(errorScanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(error.toString(), 0, result);

    return output.toString();
  }

  @Test
  public void testBasicSelect() throws IOException, InterruptedException {
    String sql = "SELECT 1";

    String actualOutput = execute("select1.py", host, pgServer.getLocalPort());
    String expectedOutput = "SELECT 1: [1]\n";
    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());

    List<WireMessage> messages = getWireMessages();
    assertEquals(3, messages.size());
    assertEquals(StartupMessage.class, messages.get(0).getClass());
    assertEquals(QueryMessage.class, messages.get(1).getClass());
    assertEquals(TerminateMessage.class, messages.get(2).getClass());
  }

  @Test
  public void testSelectAllTypes() throws IOException, InterruptedException {
    String sql = "SELECT * FROM all_types";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String actualOutput = execute("select_all_types.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "row: [1, True, b'test', 3.14, 100, Decimal('6.626'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=tzutc()), datetime.date(2022, 3, 29), "
            + "'test', {'key': 'value'}]\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testSelectParameterized() throws IOException, InterruptedException {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String actualOutput = execute("select_parameterized.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "first execution: [1, True, b'test', 3.14, 100, Decimal('6.626'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=tzutc()), datetime.date(2022, 3, 29), "
            + "'test', {'key': 'value'}]\n"
            + "second execution: [1, True, b'test', 3.14, 100, Decimal('6.626'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=tzutc()), datetime.date(2022, 3, 29), "
            + "'test', {'key': 'value'}]\n";
    assertEquals(expectedOutput, actualOutput);

    List<WireMessage> messages = getWireMessages();
    // Yes, you read that right. 24 messages to execute the same query twice.
    assertEquals(24, messages.size());
    assertEquals(StartupMessage.class, messages.get(0).getClass());

    assertEquals(ParseMessage.class, messages.get(1).getClass());
    assertEquals(FlushMessage.class, messages.get(2).getClass());
    assertEquals(SyncMessage.class, messages.get(3).getClass());
    assertEquals(DescribeMessage.class, messages.get(4).getClass());
    assertEquals(FlushMessage.class, messages.get(5).getClass());
    assertEquals(SyncMessage.class, messages.get(6).getClass());
    assertEquals(BindMessage.class, messages.get(7).getClass());
    assertEquals(FlushMessage.class, messages.get(8).getClass());
    assertEquals(ExecuteMessage.class, messages.get(9).getClass());
    assertEquals(FlushMessage.class, messages.get(10).getClass());
    assertEquals(SyncMessage.class, messages.get(11).getClass());

    assertEquals(ParseMessage.class, messages.get(1).getClass());
    assertEquals(FlushMessage.class, messages.get(2).getClass());
    assertEquals(SyncMessage.class, messages.get(3).getClass());
    assertEquals(DescribeMessage.class, messages.get(4).getClass());
    assertEquals(FlushMessage.class, messages.get(5).getClass());
    assertEquals(SyncMessage.class, messages.get(6).getClass());
    assertEquals(BindMessage.class, messages.get(7).getClass());
    assertEquals(FlushMessage.class, messages.get(8).getClass());
    assertEquals(ExecuteMessage.class, messages.get(9).getClass());
    assertEquals(FlushMessage.class, messages.get(10).getClass());
    assertEquals(SyncMessage.class, messages.get(11).getClass());

    assertEquals(TerminateMessage.class, messages.get(23).getClass());
  }
}
