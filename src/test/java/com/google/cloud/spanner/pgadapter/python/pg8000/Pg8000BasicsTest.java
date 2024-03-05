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

import static com.google.cloud.spanner.pgadapter.python.PythonTestUtil.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
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
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(PythonTest.class)
public class Pg8000BasicsTest extends AbstractMockServerTest {
  static final String DIRECTORY_NAME = "./src/test/python/pg8000";

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  @BeforeClass
  public static void createVirtualEnv() throws Exception {
    PythonTestUtil.createVirtualEnv(DIRECTORY_NAME);
  }

  static String execute(String script, String host, int port) throws Exception {
    File directory = new File(DIRECTORY_NAME);
    // Make sure to run the python executable in the specific virtual environment.
    return run(
        new String[] {
          directory.getAbsolutePath() + "/venv/bin/python3", script, host, Integer.toString(port)
        },
        DIRECTORY_NAME);
  }

  @Test
  public void testBasicSelect() throws Exception {
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
    assertEquals(4, messages.size());
    assertEquals(StartupMessage.class, messages.get(0).getClass());
    assertEquals(QueryMessage.class, messages.get(1).getClass());
    assertEquals(QueryMessage.class, messages.get(2).getClass());
    assertEquals(TerminateMessage.class, messages.get(3).getClass());
  }

  @Test
  public void testSelectAllTypes() throws Exception {
    String sql = "SELECT * FROM all_types";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String actualOutput = execute("select_all_types.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "row: [1, True, b'test', 3.14, 3.14, 100, Decimal('6.626'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2022, 3, 29), 'test', {'key': 'value'}, "
            + "[1, None, 2], [True, None, False], [b'bytes1', None, b'bytes2'], [3.14, None, -99.99], [3.14, None, -99.99], "
            + "[-100, None, -200], [Decimal('6.626'), None, Decimal('-3.14')], "
            + "[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=datetime.timezone.utc), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)], "
            + "[datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)], "
            + "['string1', None, 'string2'], [{'key': 'value1'}, None, {'key': 'value2'}]]\n";
    assertEquals(expectedOutput, actualOutput.replace("tzlocal()", "tzutc()"));
  }

  @Test
  public void testSelectParameterized() throws Exception {
    String sql = "SELECT * FROM all_types WHERE col_bigint=$1";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), ALL_TYPES_RESULTSET));

    String actualOutput = execute("select_parameterized.py", host, pgServer.getLocalPort());
    String expectedOutput =
        "first execution: [1, True, b'test', 3.14, 3.14, 100, Decimal('6.626'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2022, 3, 29), 'test', {'key': 'value'}, "
            + "[1, None, 2], [True, None, False], [b'bytes1', None, b'bytes2'], [3.14, None, -99.99], [3.14, None, -99.99], "
            + "[-100, None, -200], [Decimal('6.626'), None, Decimal('-3.14')], "
            + "[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=datetime.timezone.utc), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)], "
            + "[datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)], "
            + "['string1', None, 'string2'], [{'key': 'value1'}, None, {'key': 'value2'}]]\n"
            + "second execution: [1, True, b'test', 3.14, 3.14, 100, Decimal('6.626'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, 123456, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2022, 3, 29), 'test', {'key': 'value'}, "
            + "[1, None, 2], [True, None, False], [b'bytes1', None, b'bytes2'], [3.14, None, -99.99], [3.14, None, -99.99], "
            + "[-100, None, -200], [Decimal('6.626'), None, Decimal('-3.14')], "
            + "[datetime.datetime(2022, 2, 16, 16, 18, 2, 123456, tzinfo=datetime.timezone.utc), None, datetime.datetime(2000, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)], "
            + "[datetime.date(2023, 2, 20), None, datetime.date(2000, 1, 1)], "
            + "['string1', None, 'string2'], [{'key': 'value1'}, None, {'key': 'value2'}]]\n";
    assertEquals(expectedOutput, actualOutput);

    List<WireMessage> messages = getWireMessages();
    assertEquals(25, messages.size());
    // Yes, you read that right. 25 messages to execute the same query twice.
    // 3 of these messages are not related to executing the queries:
    // 1. Startup
    // 2. Execute `set time zone 'utc'`
    // 3. Terminate
    int index = 0;
    assertEquals(StartupMessage.class, messages.get(index++).getClass());
    assertEquals(QueryMessage.class, messages.get(index++).getClass());

    assertEquals(ParseMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(SyncMessage.class, messages.get(index++).getClass());
    assertEquals(DescribeMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(SyncMessage.class, messages.get(index++).getClass());
    assertEquals(BindMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(ExecuteMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(SyncMessage.class, messages.get(index++).getClass());

    assertEquals(ParseMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(SyncMessage.class, messages.get(index++).getClass());
    assertEquals(DescribeMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(SyncMessage.class, messages.get(index++).getClass());
    assertEquals(BindMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(ExecuteMessage.class, messages.get(index++).getClass());
    assertEquals(FlushMessage.class, messages.get(index++).getClass());
    assertEquals(SyncMessage.class, messages.get(index++).getClass());

    assertEquals(TerminateMessage.class, messages.get(index++).getClass());
  }
}
