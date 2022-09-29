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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(NodeJSTest.class)
@RunWith(JUnit4.class)
public class NodePostgresMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("node-postgres");
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    String output = runTest("testSelect1");

    assertEquals("\n\nSELECT 1 returned: 1\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInsert() throws Exception {
    // The node-postgres driver by default does not use prepared statements. However, it also sends
    // all query parameters as untyped strings. As Cloud Spanner does not support this for all data
    // types, PGAdapter will in that case always do an extra round-trip to describe the parameters
    // of the statement, and then use the types for the parameters that are returned.
    // The result of the describe statement call is cached for that connection, so executing the
    // same statement once more will not cause another describe-statement round-trip.
    String sql = "INSERT INTO users(name) VALUES($1)";
    String describeParamsSql = "select $1 from (select name=$1 from users) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeParamsSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));

    String output = runTest("testInsert");

    assertEquals("\n\nInserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(
                request ->
                    request.getSql().equals(sql) || request.getSql().equals(describeParamsSql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(describeParamsSql, describeRequest.getSql());
    assertFalse(describeRequest.hasTransaction());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertExecutedTwice() throws Exception {
    String sql = "INSERT INTO users(name) VALUES($1)";
    String describeParamsSql = "select $1 from (select name=$1 from users) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeParamsSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("bar").build(), 2L));

    String output = runTest("testInsertTwice");

    assertEquals("\n\nInserted 1 row(s)\nInserted 2 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(
                request ->
                    request.getSql().equals(sql) || request.getSql().equals(describeParamsSql))
            .collect(Collectors.toList());
    assertEquals(3, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(describeParamsSql, describeRequest.getSql());
    assertFalse(describeRequest.hasTransaction());

    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());

    executeRequest = executeSqlRequests.get(2);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasId());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAutoCommit() throws IOException, InterruptedException {
    String sql = "INSERT INTO users(name) VALUES($1)";
    String describeParamsSql = "select $1 from (select name=$1 from users) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeParamsSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(createMetadata(ImmutableList.of(TypeCode.STRING)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.update(Statement.newBuilder(sql).bind("p1").to("foo").build(), 1L));

    String output = runTest("testInsertAutoCommit");

    assertEquals("\n\nInserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(
                request ->
                    request.getSql().equals(sql) || request.getSql().equals(describeParamsSql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(describeParamsSql, describeRequest.getSql());
    assertFalse(describeRequest.hasTransaction());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(1, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testInsertAllTypes() throws IOException, InterruptedException {
    String sql =
        "INSERT INTO AllTypes "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    String describeParamsSql =
        "select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 from "
            + "(select col_bigint=$1, col_bool=$2, col_bytea=$3, col_float8=$4, col_int=$5, col_numeric=$6, col_timestamptz=$7, col_date=$8, col_varchar=$9, col_jsonb=$10 from AllTypes) p";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(describeParamsSql),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    createMetadata(
                        ImmutableList.of(
                            TypeCode.INT64,
                            TypeCode.BOOL,
                            TypeCode.BYTES,
                            TypeCode.FLOAT64,
                            TypeCode.INT64,
                            TypeCode.NUMERIC,
                            TypeCode.TIMESTAMP,
                            TypeCode.DATE,
                            TypeCode.STRING,
                            TypeCode.JSON)))
                .build()));
    StatementResult updateResult =
        StatementResult.update(
            Statement.newBuilder(sql)
                .bind("p1")
                .to(1L)
                .bind("p2")
                .to(true)
                .bind("p3")
                .to(ByteArray.copyFrom("some random string"))
                .bind("p4")
                .to(3.14d)
                .bind("p5")
                .to(100)
                .bind("p6")
                .to(Value.pgNumeric("234.54235"))
                .bind("p7")
                .to(Timestamp.parseTimestamp("2022-07-22T20:15:42.011+02:00"))
                .bind("p8")
                .to(Date.parseDate("2022-07-22"))
                .bind("p9")
                .to("some-random-string")
                .bind("p10")
                .to("{\"my_key\":\"my-value\"}")
                .build(),
            1L);
    mockSpanner.putStatementResult(updateResult);

    String output = runTest("testInsertAllTypes");

    assertEquals("\n\nInserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(
                request ->
                    request.getSql().equals(sql) || request.getSql().equals(describeParamsSql))
            .collect(Collectors.toList());
    assertEquals(2, executeSqlRequests.size());
    ExecuteSqlRequest describeRequest = executeSqlRequests.get(0);
    assertEquals(describeParamsSql, describeRequest.getSql());
    assertFalse(describeRequest.hasTransaction());
    ExecuteSqlRequest executeRequest = executeSqlRequests.get(1);
    assertEquals(sql, executeRequest.getSql());
    assertEquals(10, executeRequest.getParamTypesCount());
    assertTrue(executeRequest.getTransaction().hasBegin());
    assertTrue(executeRequest.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  static String runTest(String testName) throws IOException, InterruptedException {
    return NodeJSTest.runTest("node-postgres", testName, pgServer.getLocalPort());
  }
}
