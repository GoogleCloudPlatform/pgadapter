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
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
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
public class DjangoPgAggregatesTest extends DjangoTestSetup {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  @Test
  public void testStrAgg() throws IOException, InterruptedException {

    String sql =
        "SELECT \"singers\".\"firstname\", STRING_AGG(\"singers\".\"firstname\", '|' ) AS \"str\" FROM \"singers\" GROUP BY \"singers\".\"firstname\" LIMIT 21";

    List<String> result = new ArrayList<>();

    result.add("string");
    result.add("firstname");

    result.add("string");
    result.add("str");

    result.add("hello");
    result.add("hello|world");

    result.add("world");
    result.add("world|hello");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 2)));

    List<String> options = new ArrayList<>();
    options.add("string_agg");

    String actualOutput = executePgAggregateTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'firstname': 'hello', 'str': 'hello|world'}, {'firstname': 'world', 'str': 'world|hello'}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2).getSql());
  }

  @Test
  public void testArrAgg() throws IOException, InterruptedException {

    String sql = "SELECT ARRAY_AGG(\"singers\".\"firstname\" ) AS \"arr\" FROM \"singers\"";

    List<String> result = new ArrayList<>();

    result.add("array");
    result.add("arr");

    result.add("[hello,beautiful,world,]");
    ResultSet rs = createResultSet(result, 2);

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 1)));

    List<String> options = new ArrayList<>();
    options.add("arr_agg");

    String actualOutput = executePgAggregateTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "{'arr': ['hello', 'beautiful', 'world']}\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(3, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(2).getSql());
  }
}
