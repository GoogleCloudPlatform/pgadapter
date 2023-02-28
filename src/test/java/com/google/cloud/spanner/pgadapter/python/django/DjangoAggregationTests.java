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
public class DjangoAggregationTests extends DjangoTestSetup {

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static List<Object[]> data() {
    return ImmutableList.of(new Object[] {"localhost"}, new Object[] {"/tmp"});
  }

  @Test
  public void simpleCountTest() throws Exception {

    String sql = "SELECT COUNT(*) AS \"__count\" FROM \"singers\"";

    List<String> result = new ArrayList<>();
    result.add("int");
    result.add("__count");

    result.add("10");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 1)));

    List<String> options = new ArrayList<>();
    options.add("simple_count");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "10\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void aggregateAvg() throws Exception {

    String sql = "SELECT AVG(\"singers\".\"singerid\") AS \"singerid__avg\" FROM \"singers\"";

    List<String> result = new ArrayList<>();
    result.add("int");
    result.add("singerid__avg");

    result.add("10");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 1)));

    List<String> options = new ArrayList<>();
    options.add("aggregate_avg");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "{'singerid__avg': 10.0}\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void aggregateMin() throws Exception {

    String sql = "SELECT MIN(\"singers\".\"singerid\") AS \"singerid__min\" FROM \"singers\"";

    List<String> result = new ArrayList<>();
    result.add("int");
    result.add("singerid__min");

    result.add("10");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 1)));

    List<String> options = new ArrayList<>();
    options.add("aggregate_min");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "{'singerid__min': 10}\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void aggregateMax() throws Exception {

    String sql = "SELECT MAX(\"singers\".\"singerid\") AS \"singerid__max\" FROM \"singers\"";

    List<String> result = new ArrayList<>();
    result.add("int");
    result.add("singerid__max");

    result.add("10");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 1)));

    List<String> options = new ArrayList<>();
    options.add("aggregate_max");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "{'singerid__max': 10}\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void aggregateMultiple() throws Exception {

    String sql =
        "SELECT MAX(\"singers\".\"singerid\") AS \"singerid__max\", MIN(\"singers\".\"singerid\") AS \"singerid__min\", AVG(\"singers\".\"singerid\") AS \"singerid__avg\" FROM \"singers\"";

    List<String> result = new ArrayList<>();
    result.add("int");
    result.add("singerid__max");
    result.add("int");
    result.add("singerid__min");
    result.add("int");
    result.add("singerid__avg");

    result.add("12");
    result.add("1");
    result.add("8");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 3)));

    List<String> options = new ArrayList<>();
    options.add("aggregate_multiple");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput = "{'singerid__max': 12, 'singerid__min': 1, 'singerid__avg': 8.0}\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void annotateCount() throws Exception {

    String sql =
        "SELECT \"singers\".\"firstname\", COUNT(\"singers\".\"firstname\") AS \"firstname__count\" FROM \"singers\" GROUP BY \"singers\".\"firstname\" LIMIT 21";

    List<String> result = new ArrayList<>();
    result.add("string");
    result.add("firstname");

    result.add("int");
    result.add("firstname__count");

    result.add("john");
    result.add("2");

    result.add("jane");
    result.add("3");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 2)));

    List<String> options = new ArrayList<>();
    options.add("annotate_count");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'firstname': 'john', 'firstname__count': 2}, {'firstname': 'jane', 'firstname__count': 3}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void annotateMin() throws Exception {

    String sql =
        "SELECT \"singers\".\"firstname\", MIN(\"singers\".\"singerid\") AS \"singerid__min\" FROM \"singers\" GROUP BY \"singers\".\"firstname\" LIMIT 21";
    List<String> result = new ArrayList<>();
    result.add("string");
    result.add("firstname");

    result.add("int");
    result.add("firstname__min");

    result.add("john");
    result.add("2");

    result.add("jane");
    result.add("3");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 2)));

    List<String> options = new ArrayList<>();
    options.add("annotate_min");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'firstname': 'john', 'singerid__min': 2}, {'firstname': 'jane', 'singerid__min': 3}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void annotateMax() throws Exception {

    String sql =
        "SELECT \"singers\".\"firstname\", MAX(\"singers\".\"singerid\") AS \"singerid__max\" FROM \"singers\" GROUP BY \"singers\".\"firstname\" LIMIT 21";

    List<String> result = new ArrayList<>();
    result.add("string");
    result.add("firstname");

    result.add("int");
    result.add("firstname__max");

    result.add("john");
    result.add("2");

    result.add("jane");
    result.add("3");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 2)));

    List<String> options = new ArrayList<>();
    options.add("annotate_max");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'firstname': 'john', 'singerid__max': 2}, {'firstname': 'jane', 'singerid__max': 3}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void annotateAvg() throws Exception {

    String sql =
        "SELECT \"singers\".\"firstname\", AVG(\"singers\".\"singerid\") AS \"singerid__avg\" FROM \"singers\" GROUP BY \"singers\".\"firstname\" LIMIT 21";

    List<String> result = new ArrayList<>();
    result.add("string");
    result.add("firstname");

    result.add("int");
    result.add("firstname__avg");

    result.add("john");
    result.add("2");

    result.add("jane");
    result.add("3");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 2)));

    List<String> options = new ArrayList<>();
    options.add("annotate_avg");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'firstname': 'john', 'singerid__avg': 2.0}, {'firstname': 'jane', 'singerid__avg': 3.0}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }

  @Test
  public void annotateMultiple() throws Exception {

    String sql =
        "SELECT \"singers\".\"singerid\", AVG(\"singers\".\"singerid\") AS \"singerid__avg\", COUNT(\"singers\".\"singerid\") AS \"singerid__count\", MAX(\"singers\".\"singerid\") AS \"singerid__max\", MIN(\"singers\".\"singerid\") AS \"singerid__min\" FROM \"singers\" GROUP BY \"singers\".\"singerid\" LIMIT 21";

    List<String> result = new ArrayList<>();
    result.add("int");
    result.add("singerid");
    result.add("int");
    result.add("singerid__avg");
    result.add("int");
    result.add("singerid__count");
    result.add("int");
    result.add("singerid__max");
    result.add("int");
    result.add("singerid__min");

    result.add("1");
    result.add("10");
    result.add("18");
    result.add("15");
    result.add("5");

    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql), createResultSet(result, 5)));

    List<String> options = new ArrayList<>();
    options.add("annotate_multiple");

    String actualOutput = executeAggregationTests(pgServer.getLocalPort(), host, options);
    String expectedOutput =
        "<QuerySet [{'singerid': 1, 'singerid__avg': 10.0, 'singerid__count': 18, 'singerid__max': 15, 'singerid__min': 5}]>\n";

    assertEquals(expectedOutput, actualOutput);

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    assertEquals(sql, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0).getSql());
  }
}
